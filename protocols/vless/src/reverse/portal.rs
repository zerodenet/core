use super::{Control, PortalHeartbeat, CONTROL_DOMAIN, MONITOR_INTERVAL, PORTAL_IDLE_TIMEOUT};
use crate::{mux::MuxResponseBacklogPolicy, mux_pool::MuxPoolConn};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
use tokio::io::{AsyncRead, AsyncWrite};
use zero_core::{Address, Error, Session, UdpFlowPacket};

/// The configured virtual outbound owns the admitted reverse connections.
/// Prepared leaves retain this pool, never a proxy registry or configuration.
#[derive(Clone, Default)]
pub struct Portal(Arc<State>);
#[derive(Default)]
struct State {
    workers: Mutex<Vec<Arc<Worker>>>,
    retired: AtomicBool,
}
struct Worker {
    connection: Arc<MuxPoolConn>,
    draining: AtomicBool,
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
        for worker in self.0.workers.lock().unwrap().drain(..) {
            worker.connection.close();
        }
    }

    /// Called only after runtime has granted admission to the authenticated
    /// control session. The returned guard owns removal and transport teardown.
    pub(crate) fn attach<S>(
        &self,
        stream: S,
        backlog: MuxResponseBacklogPolicy,
    ) -> Result<PortalRegistration, Error>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut workers = self.0.workers.lock().unwrap();
        if self.0.retired.load(Ordering::Acquire) {
            return Err(Error::Io("Rvs portal retired"));
        }
        workers.retain(|worker| !worker.connection.closed());
        let worker = Arc::new(Worker {
            connection: Arc::new(MuxPoolConn::new(
                stream,
                &[0; 16],
                u16::MAX as u32,
                None,
                backlog,
                false,
            )),
            draining: AtomicBool::new(false),
        });
        let sid = worker
            .connection
            .try_reserve_stream_id()
            .ok_or(Error::Io("Rvs control admission failed"))?;
        let (_, sender, receiver) = worker.connection.open_udp_stream_with_id(sid, None)?;
        workers.push(worker.clone());
        Ok(PortalRegistration {
            owner: Arc::downgrade(&self.0),
            worker,
            sender: Some(sender),
            receiver: Some(receiver),
        })
    }

    fn reserve(&self) -> Result<(Arc<MuxPoolConn>, u16), Error> {
        let mut workers = self.0.workers.lock().unwrap();
        workers.retain(|worker| !worker.connection.closed());
        // Prefer the least loaded non-draining worker, then allow an existing
        // draining worker while the bridge establishes its replacement.
        for allow_draining in [false, true] {
            let mut candidates: Vec<_> = workers
                .iter()
                .filter(|worker| allow_draining || !worker.draining.load(Ordering::Acquire))
                .collect();
            candidates.sort_by_key(|worker| worker.connection.active_connections());
            for worker in candidates {
                if let Some(sid) = worker.connection.try_reserve_stream_id() {
                    return Ok((worker.connection.clone(), sid));
                }
            }
        }
        Err(Error::Io("Rvs portal has no available bridge"))
    }

    pub fn open_tcp(
        &self,
        session: &Session,
    ) -> Result<impl AsyncRead + AsyncWrite + Unpin + Send + 'static, Error> {
        let (connection, sid) = self.reserve()?;
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
        let (connection, sid) = self.reserve()?;
        let (_, sender, receiver) = connection.open_udp_stream_with_origin(
            sid,
            None,
            Some(crate::mux::origin::Origin::from_session(session)),
        )?;
        Ok(crate::udp::start_mux_udp_flow(sender, receiver))
    }
}

pub struct PortalRegistration {
    owner: Weak<State>,
    worker: Arc<Worker>,
    sender: Option<tokio::sync::mpsc::UnboundedSender<UdpFlowPacket>>,
    receiver: Option<tokio::sync::mpsc::Receiver<crate::mux_pool::MuxDownlink<UdpFlowPacket>>>,
}
impl Drop for PortalRegistration {
    fn drop(&mut self) {
        self.worker.connection.close();
        if let Some(owner) = self.owner.upgrade() {
            owner
                .workers
                .lock()
                .unwrap()
                .retain(|worker| !Arc::ptr_eq(worker, &self.worker));
        }
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
                        self.send_control(control)?;
                        idle.as_mut().reset(tokio::time::Instant::now() + PORTAL_IDLE_TIMEOUT);
                    }
                    if heartbeat.draining() {
                        self.worker.draining.store(true, Ordering::Release);
                        self.sender.take();
                        self.receiver.take();
                    }
                }
            }
        }
    }
    fn send_control(&self, message: Control) -> Result<(), Error> {
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
