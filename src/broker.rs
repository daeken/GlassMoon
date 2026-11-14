use crate::protocol::{
    Command, Completion, Incoming, Interface, MAX_FRAME, Outcome, Request, RpcError, VERSION,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

pub type PeerId = u64;
const MAX_PENDING_PER_SERVICE: usize = 128;
const MAX_PENDING: usize = 1024;

struct Peer {
    output: mpsc::Sender<String>,
    _close: oneshot::Sender<()>,
    greeted: bool,
    registered: bool,
    watching: bool,
    pending: HashSet<String>,
}

struct Generation {
    interface: Interface,
    state: State,
    in_flight: usize,
}

#[derive(Clone, Copy)]
enum State {
    Staged,
    Active,
    Draining { deadline: Instant },
}

struct Call {
    client: PeerId,
    request_id: String,
    generation: PeerId,
    stream: bool,
}

struct Invocation {
    service: String,
    generation: Option<PeerId>,
    interface_version: u32,
    method: String,
    params: Value,
}

pub struct Broker {
    peers: BTreeMap<PeerId, Peer>,
    generations: BTreeMap<PeerId, Generation>,
    active: HashMap<String, PeerId>,
    calls: HashMap<u64, Call>,
    slow_peers: BTreeSet<PeerId>,
    drain_time: Duration,
    next_peer: PeerId,
    next_call: u64,
}

impl Broker {
    pub fn new(drain_time: Duration, first_generation: PeerId) -> Self {
        Self {
            peers: BTreeMap::new(),
            generations: BTreeMap::new(),
            active: HashMap::new(),
            calls: HashMap::new(),
            slow_peers: BTreeSet::new(),
            drain_time,
            next_peer: first_generation,
            next_call: 1,
        }
    }

    pub fn connect(&mut self, output: mpsc::Sender<String>, close: oneshot::Sender<()>) -> PeerId {
        let peer = self.next_peer;
        self.next_peer += 1;
        self.peers.insert(
            peer,
            Peer {
                output,
                _close: close,
                greeted: false,
                registered: false,
                watching: false,
                pending: HashSet::new(),
            },
        );
        peer
    }

    pub fn receive(&mut self, peer: PeerId, incoming: Incoming) {
        if !self.peers.contains_key(&peer) {
            return;
        }
        match incoming {
            Incoming::Completion(Completion::Chunk { call_id, data }) => {
                self.chunk(peer, call_id, data);
            }
            Incoming::Completion(Completion::Complete { call_id, outcome }) => {
                self.complete(peer, call_id, outcome);
            }
            Incoming::Request(request) => self.request(peer, request),
        }
    }

    fn request(&mut self, peer: PeerId, request: Request) {
        let client = self.peers.get_mut(&peer).unwrap();
        if request.id.is_empty() || request.id.len() > 128 || client.pending.contains(&request.id) {
            self.disconnect(peer);
            return;
        }
        let id = request.id;
        if let Command::Hello { protocol } = request.command {
            if protocol == VERSION {
                client.greeted = true;
                self.ok(peer, &id, json!({ "protocol": VERSION }));
            } else {
                self.error(
                    peer,
                    &id,
                    "unsupported_protocol",
                    "GlassMoon expects protocol 1",
                );
            }
            return;
        }
        if !client.greeted {
            self.error(
                peer,
                &id,
                "hello_required",
                "Send hello before other requests",
            );
            return;
        }
        match request.command {
            Command::Hello { .. } => unreachable!(),
            Command::List => self.ok(peer, &id, self.snapshot()),
            Command::Describe { generation } => match self.generations.get(&generation) {
                Some(service) => self.ok(peer, &id, json!(&service.interface)),
                None => self.error(peer, &id, "unknown_generation", "Generation is absent"),
            },
            Command::Watch => {
                client.watching = true;
                self.ok(peer, &id, self.snapshot());
            }
            Command::Register { interface } => self.register(peer, &id, interface),
            Command::Promote { expected_active } => self.promote(peer, &id, peer, expected_active),
            Command::Activate {
                generation,
                expected_active,
            } => self.promote(peer, &id, generation, expected_active),
            Command::Call {
                service,
                generation,
                interface_version,
                method,
                params,
                stream,
            } => {
                self.call(
                    peer,
                    id,
                    Invocation {
                        service,
                        generation,
                        interface_version,
                        method,
                        params,
                    },
                    stream,
                );
            }
        }
    }

    fn register(&mut self, peer: PeerId, id: &str, interface: Interface) {
        let client = self.peers.get_mut(&peer).unwrap();
        if client.registered {
            self.error(
                peer,
                id,
                "already_registered",
                "Use a new connection for each generation",
            );
            return;
        }
        if interface.name.is_empty()
            || interface.name.len() > 96
            || !interface
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
            || interface.interface_version == 0
            || interface.methods.is_empty()
        {
            self.error(
                peer,
                id,
                "invalid_interface",
                "Supply a service name, positive interface version, and methods",
            );
            return;
        }
        client.registered = true;
        self.generations.insert(
            peer,
            Generation {
                interface,
                state: State::Staged,
                in_flight: 0,
            },
        );
        self.ok(peer, id, json!({ "generation": peer }));
        self.publish("registered", self.summary(peer));
    }

    fn promote(&mut self, caller: PeerId, id: &str, peer: PeerId, expected_active: Option<PeerId>) {
        let Some(candidate) = self.generations.get(&peer) else {
            self.error(
                caller,
                id,
                "unknown_generation",
                "Register a generation first",
            );
            return;
        };
        if !matches!(candidate.state, State::Staged) {
            self.error(
                caller,
                id,
                "not_staged",
                "Promotion requires a staged generation",
            );
            return;
        }
        let name = candidate.interface.name.clone();
        if self.active.get(&name).copied() != expected_active {
            self.error(
                caller,
                id,
                "active_changed",
                "Refresh the active generation and try again",
            );
            return;
        }

        self.active.insert(name, peer);
        self.generations.get_mut(&peer).unwrap().state = State::Active;
        if let Some(old) = expected_active {
            self.generations.get_mut(&old).unwrap().state = State::Draining {
                deadline: Instant::now() + self.drain_time,
            };
        }

        self.ok(
            caller,
            id,
            json!({ "generation": peer, "previous": expected_active }),
        );
        self.publish("promoted", self.summary(peer));
        if let Some(old) = expected_active {
            self.publish("draining", self.summary(old));
            self.send_message(
                old,
                json!({ "type": "draining", "deadline_ms": self.drain_time.as_millis() }),
            );
            if self.generations[&old].in_flight == 0 {
                self.retire(old, "drained");
            }
        }
    }

    fn call(&mut self, client: PeerId, id: String, invocation: Invocation, stream: bool) {
        let Invocation {
            service: name,
            generation,
            interface_version: version,
            method,
            params,
        } = invocation;
        let Some(generation) = generation.or_else(|| self.active.get(&name).copied()) else {
            self.error(
                client,
                &id,
                "unavailable",
                "Service has no active generation",
            );
            return;
        };
        let Some(service) = self.generations.get(&generation).filter(|service| {
            service.interface.name == name && !matches!(service.state, State::Draining { .. })
        }) else {
            self.error(
                client,
                &id,
                "generation_unavailable",
                "Requested generation cannot receive new calls",
            );
            return;
        };
        if service.interface.interface_version != version {
            self.error(
                client,
                &id,
                "interface_mismatch",
                "Inspect the active interface before calling",
            );
            return;
        }
        if !service.interface.methods.contains_key(&method) {
            self.error(
                client,
                &id,
                "unknown_method",
                "Method is absent from the active interface",
            );
            return;
        }
        if self.calls.len() >= MAX_PENDING || service.in_flight >= MAX_PENDING_PER_SERVICE {
            self.error(client, &id, "busy", "Too many calls are in flight");
            return;
        }

        let call_id = self.next_call;
        let mut frame =
            json!({ "type": "invoke", "call_id": call_id, "method": method, "params": params });
        if stream {
            frame["stream"] = json!(true);
        }
        let frame = frame.to_string();
        if frame.len() > MAX_FRAME {
            self.error(
                client,
                &id,
                "frame_too_large",
                "Invocation exceeds the frame limit",
            );
            return;
        }
        if !self.send(generation, frame) {
            self.error(
                client,
                &id,
                "unavailable",
                "Service could not accept the call",
            );
            return;
        }
        self.next_call += 1;
        self.generations.get_mut(&generation).unwrap().in_flight += 1;
        self.peers
            .get_mut(&client)
            .unwrap()
            .pending
            .insert(id.clone());
        self.calls.insert(
            call_id,
            Call {
                client,
                request_id: id,
                generation,
                stream,
            },
        );
    }

    fn chunk(&mut self, peer: PeerId, call_id: u64, data: Value) {
        let Some(call) = self.calls.get(&call_id) else {
            return;
        };
        if call.generation != peer || !call.stream {
            return;
        }
        let frame = json!({
            "type": "chunk", "id": call.request_id, "generation": peer, "data": data,
        })
        .to_string();
        if frame.len() > MAX_FRAME {
            self.complete(
                peer,
                call_id,
                Outcome::Error {
                    error: RpcError::new("frame_too_large", "Stream chunk exceeds the frame limit"),
                },
            );
            return;
        }
        self.send(call.client, frame);
    }

    fn complete(&mut self, peer: PeerId, call_id: u64, outcome: Outcome) {
        if self
            .calls
            .get(&call_id)
            .is_none_or(|call| call.generation != peer)
        {
            return;
        }
        let call = self.calls.remove(&call_id).unwrap();
        self.deliver(call, outcome);
        let service = self.generations.get_mut(&peer).unwrap();
        service.in_flight -= 1;
        if matches!(service.state, State::Draining { .. }) && service.in_flight == 0 {
            self.retire(peer, "drained");
        }
    }

    fn deliver(&mut self, call: Call, outcome: Outcome) {
        if let Some(client) = self.peers.get_mut(&call.client) {
            client.pending.remove(&call.request_id);
            self.response(
                call.client,
                &call.request_id,
                outcome,
                Some(call.generation),
            );
        }
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.generations
            .values()
            .filter_map(|service| match service.state {
                State::Draining { deadline } => Some(deadline),
                _ => None,
            })
            .min()
    }

    pub fn expire_drains(&mut self) {
        let now = Instant::now();
        let expired: Vec<_> = self
            .generations
            .iter()
            .filter_map(|(&id, service)| match service.state {
                State::Draining { deadline } if deadline <= now => Some(id),
                _ => None,
            })
            .collect();
        for id in expired {
            self.retire(id, "deadline");
        }
    }

    fn retire(&mut self, generation: PeerId, reason: &str) {
        if let Some(service) = self.generations.remove(&generation) {
            let name = service.interface.name;
            if self.active.get(&name) == Some(&generation) {
                self.active.remove(&name);
            }
            if service.in_flight > 0 {
                let calls: Vec<_> = self
                    .calls
                    .extract_if(|_, call| call.generation == generation)
                    .map(|(_, call)| call)
                    .collect();
                for call in calls {
                    let error = if reason == "deadline" {
                        RpcError::new("drain_timeout", "Service drain deadline expired")
                    } else {
                        RpcError::new(
                            "service_disconnected",
                            "Service disconnected before replying",
                        )
                    };
                    self.deliver(call, Outcome::Error { error });
                }
            }
            self.publish(
                "retired",
                json!({ "name": name, "generation": generation, "reason": reason }),
            );
            if reason != "disconnected" {
                self.send_message(generation, json!({ "type": "retire", "reason": reason }));
            }
        }
    }

    pub fn disconnect(&mut self, peer: PeerId) {
        self.peers.remove(&peer);
        self.retire(peer, "disconnected");
    }

    pub fn disconnect_slow_peers(&mut self) {
        while let Some(peer) = self.slow_peers.pop_first() {
            self.disconnect(peer);
        }
    }

    fn snapshot(&self) -> Value {
        json!({ "services": self.generations.keys().map(|&id| self.summary(id)).collect::<Vec<_>>() })
    }

    fn summary(&self, id: PeerId) -> Value {
        let service = &self.generations[&id];
        let state = match service.state {
            State::Staged => "staged",
            State::Active => "active",
            State::Draining { .. } => "draining",
        };
        let mut summary = json!({
            "name": service.interface.name,
            "generation": id,
            "interface_version": service.interface.interface_version,
            "state": state,
            "in_flight": service.in_flight,
        });
        if let Some(kind) = service.interface.kind {
            summary["kind"] = json!(kind);
        }
        summary
    }

    fn publish(&mut self, event: &str, service: Value) {
        let frame = json!({ "type": "event", "event": event, "service": service }).to_string();
        for (&id, peer) in &self.peers {
            if peer.watching && peer.output.try_send(frame.clone()).is_err() {
                self.slow_peers.insert(id);
            }
        }
    }

    fn ok(&mut self, peer: PeerId, id: &str, result: Value) {
        self.response(peer, id, Outcome::Ok { result }, None);
    }

    fn error(&mut self, peer: PeerId, id: &str, code: &str, message: &str) {
        self.response(
            peer,
            id,
            Outcome::Error {
                error: RpcError::new(code, message),
            },
            None,
        );
    }

    fn response(&mut self, peer: PeerId, id: &str, outcome: Outcome, generation: Option<PeerId>) {
        let mut frame = json!(outcome);
        frame["type"] = json!("response");
        frame["id"] = json!(id);
        if let Some(generation) = generation {
            frame["generation"] = json!(generation);
        }
        let frame = frame.to_string();
        if frame.len() > MAX_FRAME {
            self.error(
                peer,
                id,
                "frame_too_large",
                "Response exceeds the frame limit",
            );
        } else {
            self.send(peer, frame);
        }
    }

    fn send_message(&mut self, peer: PeerId, message: Value) {
        self.send(peer, message.to_string());
    }

    fn send(&mut self, peer: PeerId, frame: String) -> bool {
        let Some(connection) = self.peers.get(&peer) else {
            return false;
        };
        if connection.output.try_send(frame).is_err() {
            self.slow_peers.insert(peer);
            return false;
        }
        true
    }
}
