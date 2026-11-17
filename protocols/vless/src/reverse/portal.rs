use super::{Control, PortalHeartbeat, CONTROL_DOMAIN, MONITOR_INTERVAL, PORTAL_IDLE_TIMEOUT};
use crate::{mux::MuxResponseBacklogPolicy, mux_pool::MuxPoolConn};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::io::{AsyncRead, AsyncWrite};
mod attach;
pub mod component;
mod ownership;
use zero_core::{Address, Error, Session, UdpFlowPacket};

/// The configured virtual outbound owns the admitted reverse connections.
/// Prepared leaves retain this pool, never a proxy registry or configuration.
#[derive(Clone, Default)]
pub struct Portal(pub(super) Arc<State>);
#[derive(Default)]
pub(super) struct State {
    pub(super) workers: Mutex<Vec<Arc<Worker>>>,
    pub(super) retired: AtomicBool,
    pub(super) managed: AtomicBool,
    pub(super) ready: AtomicBool,
    pub(super) generation: Mutex<Option<zero_core::component::ComponentInstanceId>>,
    pub(super) provider:
        std::sync::OnceLock<Arc<zero_platform_tokio::controlled_carrier::AcceptedCarrierProvider>>,
}
pub(super) struct Worker {
    connection: Arc<MuxPoolConn>,
    draining: AtomicBool,
    custody: Arc<ownership::Custody>,
    resource: std::sync::OnceLock<Arc<zero_platform_tokio::AcceptedResource>>,
    origin: Option<zero_platform_tokio::controlled_carrier::CarrierOrigin>,
}
impl core::fmt::Debug for Portal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RvsPortal")
            .field("workers", &self.0.workers.lock().unwrap().len())
            .finish()
    }
}
impl Portal {
    pub fn retire(&self) {
        self.0.retired.store(true, Ordering::Release);
        let workers = self.0.workers.lock().unwrap().clone();
        for worker in workers {
            worker.connection.close();
        }
    }

    fn lease(
        &self,
        worker: &Worker,
    ) -> Result<ownership::Lease, zero_platform_tokio::contract::ChannelError> {
        let actual = match self.0.provider.get() {
            Some(provider) => {
                let driver = worker
                    .custody
                    .driver
                    .lock()
                    .unwrap()
                    .clone()
                    .ok_or_else(|| {
                        component::error(
                            zero_platform_tokio::contract::Stage::Prepare,
                            zero_platform_tokio::contract::Reason::StalePlan,
                        )
                    })?;
                Some(provider.borrow_driver(&driver)?)
            }
            None => None,
        };
        Ok(worker.custody.lease(actual))
    }
    fn reserve_worker(&self) -> Result<(Arc<Worker>, u16), Error> {
        if self.0.retired.load(Ordering::Acquire) {
            return Err(Error::Io("Rvs portal retired"));
        }
        let mut workers = self.0.workers.lock().unwrap();
        workers.retain(|worker| !worker.custody.confirmed());
        // Prefer the least loaded non-draining worker, then allow an existing
        // draining worker while the bridge establishes its replacement.
        for allow_draining in [false, true] {
            let mut candidates: Vec<_> = workers
                .iter()
                .filter(|worker| allow_draining || !worker.draining.load(Ordering::Acquire))
                .collect();
            candidates.sort_by_key(|worker| worker.connection.active_connections());
            for worker in candidates {
                if self.0.provider.get().is_some_and(|provider| {
                    worker
                        .origin
                        .as_ref()
                        .is_none_or(|origin| !provider.contains(origin))
                }) {
                    continue;
                }
                if worker
                    .connection
                    .inbound_caller
                    .as_ref()
                    .is_some_and(|caller| caller.check_open().is_err())
                {
                    continue;
                }
                if let Some(sid) = worker.connection.try_reserve_stream_id() {
                    return Ok(((*worker).clone(), sid));
                }
            }
        }
        Err(Error::Io("Rvs portal has no available bridge"))
    }

    pub fn open_tcp(
        &self,
        session: &Session,
    ) -> Result<impl AsyncRead + AsyncWrite + Unpin + Send + 'static, Error> {
        let (worker, sid) = self.reserve_worker()?;
        let connection = &worker.connection;
        connection.open_tcp_stream_with_origin(
            sid,
            session.port,
            &session.target,
            Some(&crate::mux::origin::Origin::from_session(session)),
        )
    }
    pub fn open_udp_connection(
        &self,
        session: &Session,
    ) -> Result<crate::udp::VlessUdpFlowConnection, Error> {
        let (worker, sid) = self.reserve_worker()?;
        let connection = &worker.connection;
        let (_, sender, receiver) = connection.open_udp_stream_with_origin(
            sid,
            None,
            Some(crate::mux::origin::Origin::from_session(session)),
        )?;
        match connection.inbound_caller.as_ref() {
            Some(caller) => crate::udp::start_scoped_mux_udp_flow(sender, receiver, caller),
            None => Ok(crate::udp::start_mux_udp_flow(sender, receiver)),
        }
    }
}

pub struct PortalRegistration {
    worker: Arc<Worker>,
    sender: Option<tokio::sync::mpsc::UnboundedSender<UdpFlowPacket>>,
    receiver: Option<tokio::sync::mpsc::Receiver<crate::mux_pool::MuxDownlink<UdpFlowPacket>>>,
    native_control: Option<crate::mux_pool::native::DatagramIo>,
}
impl Drop for PortalRegistration {
    fn drop(&mut self) {
        self.worker.connection.close();
        // The persistent portal and original accepted graph retain custody
        // until actual views and this driver's original join are confirmed.
    }
}
impl PortalRegistration {
    pub async fn run(mut self) -> Result<(), Error> {
        let mut heartbeat = PortalHeartbeat::default();
        let mut ticker = tokio::time::interval(MONITOR_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let cleanup_period = std::time::Duration::from_secs(16);
        let mut cleanup =
            tokio::time::interval_at(tokio::time::Instant::now() + cleanup_period, cleanup_period);
        cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut previous = (
            self.worker.connection.active_connections(),
            self.worker.connection.total_connections(),
        );
        let idle = tokio::time::sleep(PORTAL_IDLE_TIMEOUT);
        tokio::pin!(idle);
        loop {
            tokio::select! {
                _ = self.worker.connection.wait_closed() => return Ok(()),
                _ = cleanup.tick() => {
                    let current = (self.worker.connection.active_connections(), self.worker.connection.total_connections());
                    if current.0 == 0 && previous.0 == 0 && current.1 == previous.1 { return Ok(()); }
                    previous = current;
                }
                _ = &mut idle => return Err(Error::Io("Rvs portal control idle timeout")),
                _ = ticker.tick(), if !heartbeat.draining() => {
                    if let Some(control) = heartbeat.tick(self.worker.connection.total_connections()) {
                        self.send_control(control).await?;
                        idle.as_mut().reset(tokio::time::Instant::now() + PORTAL_IDLE_TIMEOUT);
                    }
                    if heartbeat.draining() {
                        self.worker.draining.store(true, Ordering::Release);
                        self.sender.take();
                        self.receiver.take();
                        self.native_control.take();
                    }
                }
            }
        }
    }
    async fn send_control(&mut self, message: Control) -> Result<(), Error> {
        if let Some(control) = &mut self.native_control {
            let payload = message.into_packet();
            std::future::poll_fn(|cx| control.poll_control(cx, &payload))
                .await
                .map_err(|_| Error::Io("Rvs original control send failed"))?;
            return Ok(());
        }
        self.sender
            .as_ref()
            .ok_or(Error::Io("Rvs control is closed"))?
            .send(UdpFlowPacket::new(
                Address::Domain(CONTROL_DOMAIN.into()),
                0,
                message.into_packet(),
            ))
            .map_err(|_| Error::Io("Rvs heartbeat send failed"))
    }
}
