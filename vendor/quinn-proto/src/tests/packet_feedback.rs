use super::*;
use crate::congestion::{Controller, ControllerFactory, NewRenoConfig, PacketEvent, PacketKey};
use crate::packet::SpaceId;
use std::{any::Any, collections::BTreeSet};

struct CaptureFactory(Arc<Mutex<Vec<PacketEvent>>>);
struct Capture {
    inner: Box<dyn Controller>,
    events: Arc<Mutex<Vec<PacketEvent>>>,
}
impl ControllerFactory for CaptureFactory {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        Box::new(Capture {
            inner: Arc::new(NewRenoConfig::default()).build(now, mtu),
            events: self.0.clone(),
        })
    }
}
impl Controller for Capture {
    fn on_packet_event(&mut self, event: PacketEvent) {
        self.events.lock().unwrap().push(event);
    }
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        limited: bool,
        rtt: &crate::RttEstimator,
    ) {
        self.inner.on_ack(now, sent, bytes, limited, rtt);
    }
    fn on_congestion_event(&mut self, now: Instant, sent: Instant, persistent: bool, lost: u64) {
        self.inner.on_congestion_event(now, sent, persistent, lost);
    }
    fn on_mtu_update(&mut self, mtu: u16) {
        self.inner.on_mtu_update(mtu);
    }
    fn window(&self) -> u64 {
        self.inner.window()
    }
    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            events: self.events.clone(),
        })
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[test]
fn exact_packet_feedback_covers_handshake_coalescing_ack_and_loss_batches() {
    let mut pair = Pair::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut transport = TransportConfig::default();
    transport.congestion_controller_factory(Arc::new(CaptureFactory(events.clone())));
    let mut config = client_config();
    config.transport_config(Arc::new(transport));
    let (client, _) = pair.connect_with(config);
    let stream = pair.client_streams(client).open(Dir::Uni).unwrap();
    pair.client_send(client, stream)
        .write(&[42; 64 * 1024])
        .unwrap();
    pair.drive_client();
    pair.server.inbound.clear();
    pair.drive();
    let events = events.lock().unwrap();
    let mut sent = BTreeSet::<PacketKey>::new();
    let mut pending = false;
    let (mut acks, mut losses, mut ends) = (0, 0, 0);
    for &event in &*events {
        match event {
            PacketEvent::Sent {
                key,
                bytes,
                ack_eliciting,
                ..
            } => {
                assert!(
                    sent.insert(key),
                    "duplicate per-packet notification: {key:?}"
                );
                if ack_eliciting {
                    assert!(bytes > 0 && bytes <= 1452);
                }
            }
            PacketEvent::Acked { key } => {
                assert!(sent.contains(&key));
                acks += 1;
                pending = true;
            }
            PacketEvent::Lost { key, .. } => {
                assert!(sent.contains(&key));
                losses += 1;
                pending = true;
            }
            PacketEvent::FeedbackEnd { .. } => {
                if pending {
                    ends += 1;
                }
                pending = false;
            }
            _ => {}
        }
    }
    assert!(acks > 2 && losses > 0 && ends > 0);
    assert!(!pending, "feedback must end after ACK and loss callbacks");
    assert!(sent.iter().any(|key| key.0 == SpaceId::Initial as u8));
    assert!(sent.iter().any(|key| key.0 == SpaceId::Data as u8));
}
