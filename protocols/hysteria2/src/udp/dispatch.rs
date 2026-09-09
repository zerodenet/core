//! One datagram reader per authenticated connection, with bounded session queues.
//! Matches the session dispatch contract in Hysteria app/v2.12.2 core/client/udp.go.
use super::{parse_udp_datagram, Hysteria2UdpPacket};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use zero_core::Error;

const MAX_SESSIONS: usize = 4096;
#[derive(Default)]
struct State {
    next_id: u64,
    closed: bool,
    sessions: BTreeMap<u32, mpsc::Sender<Hysteria2UdpPacket>>,
}
pub(crate) struct Dispatcher {
    state: Arc<Mutex<State>>,
    reader: tokio::task::JoinHandle<()>,
}
pub(super) struct Registration {
    state: Arc<Mutex<State>>,
    pub(super) id: u32,
    pub(super) receiver: mpsc::Receiver<Hysteria2UdpPacket>,
}
impl Dispatcher {
    pub(crate) fn new(connection: quinn::Connection) -> Self {
        let state = Arc::new(Mutex::new(State {
            next_id: 1,
            ..Default::default()
        }));
        let shared = state.clone();
        let reader = tokio::spawn(async move {
            while let Ok(data) = connection.read_datagram().await {
                let Ok(packet) = parse_udp_datagram(&data) else {
                    continue;
                };
                let state = shared.lock().unwrap();
                state.deliver(packet);
            }
            let mut state = shared.lock().unwrap();
            state.closed = true;
            state.sessions.clear();
        });
        Self { state, reader }
    }
    pub(super) fn register(&self) -> Result<Registration, Error> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(Error::Io("hysteria2 UDP connection closed"));
        }
        if state.sessions.len() >= MAX_SESSIONS || state.next_id > u32::MAX as u64 {
            return Err(Error::Io("hysteria2 UDP session capacity exhausted"));
        }
        // Do not reuse IDs on a live connection: delayed packets must not reach a new flow.
        let id = state.next_id as u32;
        state.next_id += 1;
        let (sender, receiver) = mpsc::channel(64);
        state.sessions.insert(id, sender);
        Ok(Registration {
            state: self.state.clone(),
            id,
            receiver,
        })
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.state.lock().unwrap().sessions.remove(&self.id);
    }
}
impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.reader.abort();
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        state.sessions.clear();
    }
}

impl State {
    fn deliver(&self, packet: Hysteria2UdpPacket) {
        if let Some(sender) = self.sessions.get(&packet.session_id()) {
            // A slow consumer may drop its own UDP packets, never stall siblings.
            let _ = sender.try_send(packet);
        }
    }
}
#[cfg(test)]
#[path = "tests/dispatch.rs"]
mod tests;
