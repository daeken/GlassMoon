mod broker;
mod protocol;

use broker::{Broker, PeerId};
use futures_util::{SinkExt, StreamExt};
use protocol::{Incoming, MAX_FRAME};
use std::error::Error;
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Read};
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, ChildStdin, Command};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tokio_util::codec::{Framed, LinesCodec};

enum Event {
    Message(PeerId, Incoming),
    Disconnected(PeerId),
}

const USAGE: &str = "usage: glassmoon SOCKET [DRAIN_MS] [--bootstrap COMMAND ARG ...]";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1).peekable();
    if args
        .peek()
        .is_some_and(|value| value == "--help" || value == "-h")
    {
        println!("{USAGE}");
        return Ok(());
    }
    let path = PathBuf::from(args.next().ok_or(USAGE)?);
    let mut drain_ms = 30_000;
    if args.peek().is_some_and(|value| value != "--bootstrap") {
        drain_ms = args
            .next()
            .and_then(|value| value.to_str()?.parse().ok())
            .filter(|value| (1..=3_600_000).contains(value))
            .ok_or("DRAIN_MS must be between 1 and 3600000")?;
    }
    let bootstrap = match args.next() {
        None => None,
        Some(flag) if flag == "--bootstrap" => {
            Some((args.next().ok_or(USAGE)?, args.collect::<Vec<_>>()))
        }
        _ => return Err(USAGE.into()),
    };

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let _lock = bootstrap.as_ref().map(|_| lock_socket(&path)).transpose()?;
    let listener = UnixListener::bind(&path)?;
    let _socket = SocketFile::new(path)?;
    // Keep generation IDs distinct across broker lifetimes, within JSON's exact integer range.
    let mut seed = [0; 4];
    fs::File::open("/dev/urandom")?.read_exact(&mut seed)?;
    let first_generation = (u64::from(u32::from_ne_bytes(seed)) << 20) + 1;
    let mut bootstrap = bootstrap
        .map(|(program, args)| {
            Command::new(program)
                .args(args)
                .stdin(Stdio::piped())
                .process_group(0)
                .kill_on_drop(true)
                .spawn()
        })
        .transpose()?;
    let bootstrap_group = bootstrap.as_ref().and_then(Child::id);
    // Child::wait closes stdin itself unless we keep the writer separately.
    let mut bootstrap_pipe = bootstrap.as_mut().and_then(|child| child.stdin.take());
    let mut shutdown_deadline = None;
    let (events, mut incoming) = mpsc::channel(256);
    let mut broker = Broker::new(Duration::from_millis(drain_ms), first_generation);
    loop {
        let deadline = broker.next_deadline();
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, _) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => {
                        eprintln!("GlassMoon could not accept a connection ({error})");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let (output, outgoing) = mpsc::channel(128);
                let (close, closed) = oneshot::channel();
                let peer = broker.connect(output, close);
                tokio::spawn(serve(peer, socket, events.clone(), outgoing, closed));
            }
            Some(event) = incoming.recv() => match event {
                Event::Message(peer, message) => {
                    broker.expire_drains();
                    broker.receive(peer, message);
                }
                Event::Disconnected(peer) => broker.disconnect(peer),
            },
            () = wait_for_deadline(deadline) => broker.expire_drains(),
            status = wait_for_bootstrap(&mut bootstrap) => {
                if let Some(group) = bootstrap_group {
                    kill_group(group);
                }
                let status = status?;
                if shutdown_deadline.is_some() {
                    if !status.success() {
                        eprintln!("GlassMoon bootstrap exited during shutdown ({status})");
                    }
                    break;
                }
                return Err(format!("GlassMoon bootstrap exited ({status})").into());
            },
            () = wait_for_deadline(shutdown_deadline) => {
                if let Some(group) = bootstrap_group {
                    kill_group(group);
                }
                if let Some(child) = &mut bootstrap {
                    let _ = child.wait().await;
                }
                return Err("GlassMoon bootstrap exceeded its shutdown deadline".into());
            },
            _ = interrupt.recv(), if shutdown_deadline.is_none() => {
                if !begin_shutdown(&mut bootstrap_pipe, &mut shutdown_deadline) {
                    break;
                }
            },
            _ = terminate.recv(), if shutdown_deadline.is_none() => {
                if !begin_shutdown(&mut bootstrap_pipe, &mut shutdown_deadline) {
                    break;
                }
            },
        }
        broker.disconnect_slow_peers();
    }
    Ok(())
}

fn begin_shutdown(pipe: &mut Option<ChildStdin>, deadline: &mut Option<Instant>) -> bool {
    // Closing the lifetime pipe asks bootstrap to stop while IPC is still up.
    if pipe.take().is_none() {
        return false;
    }
    *deadline = Some(Instant::now() + Duration::from_secs(30));
    true
}

async fn wait_for_bootstrap(bootstrap: &mut Option<Child>) -> io::Result<std::process::ExitStatus> {
    match bootstrap {
        Some(child) => child.wait().await,
        None => std::future::pending().await,
    }
}

fn kill_group(group: u32) {
    // Bootstrap leads a dedicated process group, so this also terminates any surviving workers.
    unsafe { libc::kill(-(group as libc::pid_t), libc::SIGKILL) };
}

fn lock_socket(path: &Path) -> io::Result<File> {
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".kernel-lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(lock_path)?;
    lock.try_lock()?;
    let previous = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(lock),
        Err(error) => return Err(error),
    };
    if !previous.file_type().is_socket() || previous.uid() != lock.metadata()?.uid() {
        return Err(io::Error::other(
            "The broker socket path must be an owned Unix socket",
        ));
    }
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "A broker is already listening",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(lock),
        Err(error) => return Err(error),
    }
    let current = fs::symlink_metadata(path)?;
    if (current.dev(), current.ino()) != (previous.dev(), previous.ino()) {
        return Err(io::Error::other(
            "The broker socket changed during recovery",
        ));
    }
    fs::remove_file(path)?;
    Ok(lock)
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn serve(
    peer: PeerId,
    socket: UnixStream,
    events: mpsc::Sender<Event>,
    mut outgoing: mpsc::Receiver<String>,
    closed: oneshot::Receiver<()>,
) {
    {
        let (mut writer, mut reader) =
            Framed::new(socket, LinesCodec::new_with_max_length(MAX_FRAME)).split();
        let read = async {
            while let Some(Ok(frame)) = reader.next().await {
                let Ok(message) = serde_json::from_str(&frame) else {
                    break;
                };
                if events.send(Event::Message(peer, message)).await.is_err() {
                    break;
                }
            }
        };
        let write = async {
            while let Some(frame) = outgoing.recv().await {
                if writer.send(frame).await.is_err() {
                    break;
                }
            }
        };
        tokio::select! {
            () = read => {}
            () = write => {}
            _ = closed => {}
        }
    }
    let _ = events.send(Event::Disconnected(peer)).await;
}

struct SocketFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketFile {
    fn new(path: PathBuf) -> io::Result<Self> {
        let metadata = fs::symlink_metadata(&path)?;
        let socket = Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        fs::set_permissions(&socket.path, Permissions::from_mode(0o600))?;
        Ok(socket)
    }
}

impl Drop for SocketFile {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|metadata| (metadata.dev(), metadata.ino()) == (self.device, self.inode))
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}
