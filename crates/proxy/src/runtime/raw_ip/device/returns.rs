//! Bounded reverse delivery for packets forwarded through a shared device.

use std::{
    collections::HashMap,
    io,
    net::IpAddr,
    sync::Mutex,
    time::{Duration, Instant},
};

use tokio::sync::mpsc;
use zero_stack::packet;

const MAX_RETURN_ROUTES: usize = 1_024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Default)]
pub(super) struct PacketReturns {
    routes: Mutex<HashMap<IpAddr, ReturnRoute>>,
}

struct ReturnRoute {
    ingress_id: u64,
    replies: mpsc::Sender<Vec<u8>>,
    touched: Instant,
}

impl PacketReturns {
    pub(super) fn register(
        &self,
        source: IpAddr,
        ingress_id: u64,
        replies: mpsc::Sender<Vec<u8>>,
    ) -> io::Result<()> {
        let now = Instant::now();
        let mut routes = self
            .routes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(route) = routes.get_mut(&source) {
            if route.ingress_id != ingress_id
                && now.duration_since(route.touched) < IDLE_TIMEOUT
                && !route.replies.is_closed()
            {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "overlapping packet source on shared outbound",
                ));
            }
            route.touched = now;
            route.replies = replies;
            return Ok(());
        }
        if routes.len() >= MAX_RETURN_ROUTES {
            routes.retain(|_, route| {
                now.duration_since(route.touched) < IDLE_TIMEOUT && !route.replies.is_closed()
            });
        }
        if routes.len() >= MAX_RETURN_ROUTES {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "packet return route limit",
            ));
        }
        routes.insert(
            source,
            ReturnRoute {
                ingress_id,
                replies,
                touched: now,
            },
        );
        Ok(())
    }

    pub(super) fn deliver(&self, packet: &[u8]) -> bool {
        let Some(destination) = packet::ip_destination(packet) else {
            return false;
        };
        let replies = self
            .routes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&destination)
            .filter(|route| route.touched.elapsed() < IDLE_TIMEOUT)
            .map(|route| route.replies.clone());
        let Some(replies) = replies else {
            return false;
        };
        let mut forwarded = packet.to_vec();
        if packet::advance_ip_hop(&mut forwarded) {
            let _ = replies.try_send(forwarded);
        }
        true
    }

    pub(super) fn clear(&self) {
        self.routes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }
}

#[cfg(test)]
#[path = "returns/tests.rs"]
mod tests;
