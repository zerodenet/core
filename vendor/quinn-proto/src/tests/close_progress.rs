use super::*;
use crate::congestion::{Controller, ControllerFactory};
use std::{
    any::Any,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Clone)]
struct Window(Arc<AtomicU64>);
impl ControllerFactory for Window {
    fn build(self: Arc<Self>, _: Instant, _: u16) -> Box<dyn Controller> {
        Box::new(self.as_ref().clone())
    }
}
impl Controller for Window {
    fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}
    fn on_mtu_update(&mut self, _: u16) {}
    fn window(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
    fn initial_window(&self) -> u64 {
        12_000
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[test]
fn application_close_is_emitted_with_pending_data_and_exhausted_congestion_window() {
    let mut pair = Pair::default();
    let window = Arc::new(AtomicU64::new(24_000));
    let mut transport = TransportConfig::default();
    transport.congestion_controller_factory(Arc::new(Window(window.clone())));
    let mut config = client_config();
    config.transport_config(Arc::new(transport));
    let (client, server) = pair.connect_with(config);
    let stream = pair
        .client_conn_mut(client)
        .streams()
        .open(Dir::Bi)
        .unwrap();
    pair.client_conn_mut(client)
        .send_stream(stream)
        .write(b"pending")
        .unwrap();
    // A close-only packet must not inherit the queued STREAM's congestion
    // status. No peer acknowledgment is required before reporting this close.
    window.store(1, Ordering::Relaxed);
    let now = pair.time;
    pair.client_conn_mut(client)
        .close(now, VarInt(42), Bytes::from_static(b"done"));
    pair.drive();
    assert_matches!(pair.server_conn_mut(server).poll(),
        Some(Event::ConnectionLost { reason: ConnectionError::ApplicationClosed(
            ApplicationClose { error_code: VarInt(42), ref reason }
        )}) if reason.as_ref() == b"done");
}
