mod broker;
mod protocol;

use broker::{Broker, PeerId};
use futures_util::{SinkExt, StreamExt};
use protocol::{Incoming, MAX_FRAME};
use std::error::Error;
use std::fs::{self, Permissions};
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Framed, LinesCodec};

enum Event {
    Message(PeerId, Incoming),
    Disconnected(PeerId),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or("usage: glassmoon SOCKET [DRAIN_MS]")?;
    let drain_ms = args
        .next()
        .map(|value| {
            value
                .to_str()
                .and_then(|value| value.parse().ok())
                .ok_or("invalid drain time")
        })
        .transpose()?
        .unwrap_or(30_000);
    if args.next().is_some() || !(1..=3_600_000).contains(&drain_ms) {
        return Err("usage: glassmoon SOCKET [DRAIN_MS (1–3600000)]".into());
    }

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let path = PathBuf::from(path);
    let listener = UnixListener::bind(&path)?;
    let _socket = SocketFile::new(path)?;
    let (events, mut incoming) = mpsc::channel(256);
    // Keep generation IDs distinct across broker lifetimes, within JSON's exact integer range.
    let mut seed = [0; 4];
    fs::File::open("/dev/urandom")?.read_exact(&mut seed)?;
    let first_generation = (u64::from(u32::from_ne_bytes(seed)) << 20) + 1;
    let mut broker = Broker::new(Duration::from_millis(drain_ms), first_generation);
    loop {
        let deadline = broker.next_deadline();
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
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
            _ = interrupt.recv() => break,
            _ = terminate.recv() => break,
        }
        broker.disconnect_slow_peers();
    }
    Ok(())
}

async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
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
