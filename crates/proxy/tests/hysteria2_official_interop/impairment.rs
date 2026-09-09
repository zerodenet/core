//! One-client UDP link with deterministic data loss and 10 ms one-way delay.
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    net::UdpSocket,
    task::JoinHandle,
    time::{sleep_until, Duration, Instant},
};

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Counters {
    pub packets: u64,
    pub bytes: u64,
    pub dropped: u64,
    pub peak_queue_us: u64,
}

pub(super) struct Link {
    pub address: SocketAddr,
    counters: Arc<Mutex<Counters>>,
    task: JoinHandle<()>,
}

impl Link {
    pub async fn start(server_port: u16, drop_every: u64) -> Self {
        Self::with_bottleneck(server_port, drop_every, None).await
    }

    pub async fn with_bottleneck(server_port: u16, drop_every: u64, rate: Option<u64>) -> Self {
        let front = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = front.local_addr().unwrap();
        let back = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        back.connect(("127.0.0.1", server_port)).await.unwrap();
        let counters = Arc::new(Mutex::new(Counters::default()));
        let stats = counters.clone();
        let task = tokio::spawn(async move {
            let mut client = None;
            let mut up = vec![0; 65_536];
            let mut down = vec![0; 65_536];
            let mut pending =
                BinaryHeap::<Reverse<(Instant, u64, Option<SocketAddr>, Vec<u8>)>>::new();
            let mut serial = 0;
            let mut up_ready = Instant::now();
            loop {
                let wake = pending
                    .peek()
                    .map(|p| p.0 .0)
                    .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
                tokio::select! {
                    packet = front.recv_from(&mut up) => {
                        let (len, address) = packet.unwrap();
                        if let Some(previous) = client { assert_eq!(previous, address); }
                        client = Some(address);
                        let drop = {
                            let mut stats = stats.lock().unwrap();
                            stats.packets += 1;
                            stats.bytes += len as u64;
                            // Exclude QUIC long headers (handshake), ACKs and probes.
                            let drop = drop_every != 0 && len > 1000 && up[0] & 0x80 == 0
                                && stats.packets.is_multiple_of(drop_every);
                            stats.dropped += u64::from(drop);
                            drop
                        };
                        if !drop {
                            let now = Instant::now();
                            let departure = rate.map_or(now, |rate| up_ready.max(now) + Duration::from_secs_f64(len as f64 / rate as f64));
                            let queued = departure - now;
                            if queued > Duration::from_millis(200) {
                                stats.lock().unwrap().dropped += 1;
                            } else {
                                up_ready = departure;
                                let mut stats = stats.lock().unwrap();
                                stats.peak_queue_us = stats.peak_queue_us.max(queued.as_micros() as u64);
                                serial += 1;
                                pending.push(Reverse((departure + Duration::from_millis(10), serial, None, up[..len].to_vec())));
                            }
                        }
                    }
                    packet = back.recv(&mut down) => {
                        let len = packet.unwrap();
                        serial += 1;
                        pending.push(Reverse((Instant::now() + Duration::from_millis(10), serial, client, down[..len].to_vec())));
                    }
                    _ = sleep_until(wake), if !pending.is_empty() => {
                        let Reverse((_, _, target, packet)) = pending.pop().unwrap();
                        if let Some(target) = target {
                            front.send_to(&packet, target).await.unwrap();
                        } else {
                            back.send(&packet).await.unwrap();
                        }
                    }
                }
            }
        });
        Self {
            address,
            counters,
            task,
        }
    }

    pub fn reset(&self) {
        *self.counters.lock().unwrap() = Counters::default();
    }
    pub fn counters(&self) -> Counters {
        *self.counters.lock().unwrap()
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.task.abort();
    }
}
