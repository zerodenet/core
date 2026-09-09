use super::*;
use crate::receive_window::{ReceiveWindowController, ReceiveWindowFactory};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
struct Observed {
    streams: BTreeMap<StreamId, u64>,
    read: u64,
    closed: usize,
    updates: usize,
    received: usize,
}
struct Factory(Arc<Mutex<Observed>>);
#[derive(Debug)]
struct Policy(Arc<Mutex<Observed>>);
impl ReceiveWindowFactory for Factory {
    fn build(&self, _: u64, _: u64) -> Box<dyn ReceiveWindowController> {
        Box::new(Policy(self.0.clone()))
    }
}
impl ReceiveWindowController for Policy {
    fn received(&mut self, id: StreamId, _: Instant) {
        let mut state = self.0.lock().unwrap();
        state.received += 1;
        state.streams.entry(id).or_default();
    }
    fn read_stream(&mut self, id: StreamId, bytes: u64) -> bool {
        *self.0.lock().unwrap().streams.get_mut(&id).unwrap() += bytes;
        true
    }
    fn read_connection(&mut self, bytes: u64) -> bool {
        self.0.lock().unwrap().read += bytes;
        true
    }
    fn stream_update(&mut self, id: StreamId, _: Instant, rtt: Duration) -> Option<u64> {
        assert!(!rtt.is_zero());
        let mut state = self.0.lock().unwrap();
        state.updates += 1;
        state.streams.get(&id).map(|read| read + 65_536)
    }
    fn connection_update(&mut self, _: Instant, _: Duration) -> Option<u64> {
        Some(self.0.lock().unwrap().read + 131_072)
    }
    fn closed(&mut self, id: StreamId) {
        let mut state = self.0.lock().unwrap();
        state.streams.remove(&id);
        state.closed += 1;
    }
    fn connection_window(&self) -> u64 {
        131_072
    }
    fn set_connection_window(&mut self, _: u64, _: u64) {}
}

#[test]
fn reset_returns_unread_credit_once_and_empty_reset_does_not_start_sampling() {
    let mut pair = Pair::default();
    let observed = Arc::new(Mutex::new(Observed::default()));
    let mut transport = TransportConfig::default();
    transport.receive_window_controller(Arc::new(Factory(observed.clone())));
    let mut config = client_config();
    config.transport_config(Arc::new(transport));
    let (client, server) = pair.connect_with(config);
    for bytes in [0, 4096] {
        let id = pair.server_streams(server).open(Dir::Uni).unwrap();
        if bytes != 0 {
            assert_eq!(
                pair.server_send(server, id).write(&vec![7; bytes]).unwrap(),
                bytes
            );
        }
        // Reset before transmitting data: the final offset alone returns the credit.
        pair.server_send(server, id).reset(VarInt(42)).unwrap();
        pair.drive();
        let mut recv = pair.client_recv(client, id);
        let mut chunks = recv.read(true).unwrap();
        assert_matches!(chunks.next(4096), Err(ReadError::Reset(VarInt(42))));
        let _ = chunks.finalize();
        pair.drive();
        let state = observed.lock().unwrap();
        assert_eq!(state.read, bytes as u64);
        assert_eq!(state.received, usize::from(bytes != 0));
        assert!(state.streams.is_empty());
    }
    assert_eq!(observed.lock().unwrap().closed, 2);
}

#[test]
fn receive_policy_grants_wire_credit_and_reclaims_closed_stream_state() {
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(10);
    let observed = Arc::new(Mutex::new(Observed::default()));
    let mut transport = TransportConfig::default();
    transport
        .stream_receive_window(16_384u32.into())
        .receive_window(32_768u32.into())
        .receive_window_controller(Arc::new(Factory(observed.clone())));
    let mut config = client_config();
    config.transport_config(Arc::new(transport));
    let (client, server) = pair.connect_with(config);
    let id = pair.server_streams(server).open(Dir::Uni).unwrap();
    let mut written = pair.server_send(server, id).write(&[7; 48_000]).unwrap();
    pair.drive();
    let mut total = 0;
    for _ in 0..10 {
        let mut recv = pair.client_recv(client, id);
        let mut chunks = recv.read(true).unwrap();
        let mut finished = false;
        loop {
            match chunks.next(4096) {
                Ok(Some(chunk)) => {
                    assert!(chunk.bytes.iter().all(|b| *b == 7));
                    total += chunk.bytes.len();
                }
                Ok(None) => {
                    finished = true;
                    break;
                }
                Err(ReadError::Blocked) => break,
                Err(e) => panic!("{e:?}"),
            }
        }
        let _ = chunks.finalize();
        if finished {
            break;
        }
        pair.drive();
        if written < 48_000 {
            if let Ok(n) = pair
                .server_send(server, id)
                .write(&vec![7; 48_000 - written])
            {
                written += n;
                if written == 48_000 {
                    pair.server_send(server, id).finish().unwrap();
                }
            }
            pair.drive();
        }
    }
    assert_eq!(total, 48_000);
    let state = observed.lock().unwrap();
    assert_eq!(state.read, 48_000);
    assert!(state.updates > 0);
    assert_eq!(state.closed, 1);
    assert!(state.streams.is_empty());
}
