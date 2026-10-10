//! Exercise the real device scheduler without crypto or a host network socket.
use super::super::SharedRawIpDevice;
use crate::runtime::raw_ip::{RawIpAction, RawIpTunnel, RawIpWireCarrier};
use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, Mutex};
use zero_engine::EngineError;

struct Carrier(Mutex<mpsc::Receiver<u8>>);
#[async_trait::async_trait]
impl RawIpWireCarrier for Carrier {
    async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)> {
        buf[0] = self
            .0
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
        Ok((1, None))
    }
    async fn send(&self, _: &[u8], _: SocketAddr) -> io::Result<()> {
        Ok(())
    }
}
struct Tunnel {
    enabled: Arc<AtomicBool>,
    ticks: Arc<AtomicUsize>,
    precise: Option<Duration>,
    next: Option<tokio::time::Instant>,
}
impl RawIpTunnel for Tunnel {
    fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        Ok(Vec::new())
    }
    fn send_ip_packet(&mut self, _: &[u8]) -> Result<Vec<RawIpAction>, EngineError> {
        self.enabled.store(true, Ordering::Relaxed);
        self.next = self
            .precise
            .map(|delay| tokio::time::Instant::now() + delay);
        Ok(Vec::new())
    }
    fn receive_datagram(
        &mut self,
        _: Option<SocketAddr>,
        datagram: &[u8],
    ) -> Result<Vec<RawIpAction>, EngineError> {
        self.enabled.store(datagram[0] != 0, Ordering::Relaxed);
        self.next = self
            .precise
            .map(|delay| tokio::time::Instant::now() + delay);
        Ok(Vec::new())
    }
    fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        self.ticks.fetch_add(1, Ordering::Relaxed);
        self.next = self
            .precise
            .map(|delay| tokio::time::Instant::now() + delay);
        Ok(Vec::new())
    }
    fn timer_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
    fn allows_source(&self, _: IpAddr) -> bool {
        true
    }
    fn timer_schedule(&self) -> crate::runtime::raw_ip::timer::TimerSchedule {
        use crate::runtime::raw_ip::timer::TimerSchedule;
        if !self.timer_enabled() {
            TimerSchedule::Parked
        } else if let Some(next) = self.next {
            TimerSchedule::After(next.saturating_duration_since(tokio::time::Instant::now()))
        } else {
            TimerSchedule::Polling
        }
    }
}
async fn settle() {
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}
#[tokio::test(start_paused = true)]
async fn parked_protocol_timer_revives_on_receive_and_send_without_catch_up_bursts() {
    let enabled = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicUsize::new(0));
    let (wire, received) = mpsc::channel(2);
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let device = SharedRawIpDevice::start_with_carrier(
        vec![local],
        1420,
        "192.0.2.1:51820".parse().unwrap(),
        Arc::new(Carrier(Mutex::new(received))),
        Box::new(Tunnel {
            enabled: enabled.clone(),
            ticks: ticks.clone(),
            precise: None,
            next: None,
        }),
    )
    .unwrap();
    device.wait_ready().await.unwrap();
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), 0);
    wire.send(1).await.unwrap();
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), 0);
    tokio::time::advance(Duration::from_millis(249)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), 0);
    tokio::time::advance(Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), 1);
    wire.send(0).await.unwrap();
    settle().await;
    let count = ticks.load(Ordering::Relaxed);
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), count);
    let socket = device.bind_udp(local).unwrap();
    socket
        .send_to(
            b"revive",
            zero_stack::packet::Endpoint {
                ip: "10.0.0.2".parse().unwrap(),
                port: 53,
            },
        )
        .await
        .unwrap();
    settle().await;
    assert!(enabled.load(Ordering::Relaxed));
    assert_eq!(ticks.load(Ordering::Relaxed), count);
    tokio::time::advance(Duration::from_millis(250)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), count + 1);
    device.close_now();
}

#[tokio::test(start_paused = true)]
async fn standalone_device_uses_owner_deadline_without_rounding_to_polling_interval() {
    let enabled = Arc::new(AtomicBool::new(false));
    let ticks = Arc::new(AtomicUsize::new(0));
    let (wire, received) = mpsc::channel(2);
    let device = SharedRawIpDevice::start_with_carrier(
        vec!["10.0.0.1".parse().unwrap()],
        1420,
        "192.0.2.1:51820".parse().unwrap(),
        Arc::new(Carrier(Mutex::new(received))),
        Box::new(Tunnel {
            enabled,
            ticks: ticks.clone(),
            precise: Some(Duration::from_millis(37)),
            next: None,
        }),
    )
    .unwrap();
    device.wait_ready().await.unwrap();
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), 0);
    wire.send(1).await.unwrap();
    settle().await;
    tokio::time::advance(Duration::from_millis(36)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), 0);
    tokio::time::advance(Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), 1);
    wire.send(0).await.unwrap();
    settle().await;
    tokio::time::advance(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!(ticks.load(Ordering::Relaxed), 1);
    device.close_now();
    settle().await;
    assert!(device.is_closed());
}
