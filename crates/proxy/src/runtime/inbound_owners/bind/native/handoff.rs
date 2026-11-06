//! Neutral external-carrier handoff over the original listener and same Scope.
use super::*;
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Mutex, OnceLock},
};
use zero_platform_tokio::contract::{
    Impact, Reason, Stage,
    lifecycle::{CloseReport, ResourceLifecycle},
    scope::TaskRegistrar,
};
mod probe;
#[derive(Clone, Copy)]
pub(crate) struct Endpoints {
    pub external: SocketAddr,
    pub stream: SocketAddr,
    pub datagram: SocketAddr,
}
pub(crate) struct Grant {
    pub(super) outer: Arc<grant::NativeTcpGrant>,
    inner: Mutex<Option<Arc<grant::NativeTcpGrant>>>,
    parked: Mutex<Option<ControlledTcpListener>>,
    pub endpoints: OnceLock<Endpoints>,
    stream: bool,
    datagram: bool,
    declaration: ResourceDeclaration,
    identity: ComponentInstanceId,
    tasks: TaskRegistrar,
    originals: OnceLock<Arc<zero_runtime::HostOriginalBudgets>>,
    host: Weak<InboundOwner>,
    cancel: CancellationToken,
    report: Mutex<CloseReport>,
    probe: tokio::sync::Mutex<probe::Probe>,
}
fn denied(message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
        .with_source(std::io::Error::other(message))
}
impl Grant {
    #[cfg(test)]
    pub(crate) fn pause_probe(&self) -> Arc<probe::Gate> {
        self.probe.try_lock().unwrap().pause()
    }
    pub(super) fn new(
        identity: ComponentInstanceId,
        declarations: &[ResourceDeclaration],
        address: Option<SocketAddr>,
        attempts: NonZeroUsize,
        cancel: CancellationToken,
        tasks: TaskRegistrar,
        host: Weak<InboundOwner>,
    ) -> Result<Self, ChannelError> {
        let [
            tcp @ ResourceDeclaration::TcpListener { .. },
            ResourceDeclaration::DatagramListener { .. },
            ResourceDeclaration::ListenerHandoff {
                stream,
                datagram,
                max_probe_attempts,
            },
            ResourceDeclaration::Process { max_starts, .. },
        ] = declarations
        else {
            return Err(denied(
                "handoff requires one paired listener and one process grant",
            ));
        };
        if (!stream && !datagram) || max_starts.get() != 1 {
            return Err(denied("invalid handoff native budget"));
        }
        Ok(Self {
            outer: Arc::new(grant::NativeTcpGrant::new(
                identity,
                tcp,
                address,
                attempts,
                cancel.clone(),
                &tasks,
            )?),
            inner: Mutex::new(None),
            parked: Mutex::new(None),
            endpoints: OnceLock::new(),
            stream: *stream,
            datagram: *datagram,
            declaration: tcp.clone(),
            identity,
            tasks,
            originals: OnceLock::new(),
            host,
            cancel,
            report: Mutex::new(Default::default()),
            probe: tokio::sync::Mutex::new(probe::Probe::new(*max_probe_attempts)),
        })
    }
    pub(super) fn bind_original_budgets(
        &self,
        roots: &Arc<zero_runtime::HostOriginalBudgets>,
    ) -> Result<(), ChannelError> {
        if let Some(actual) = self.originals.get() {
            if !Arc::ptr_eq(actual, roots) {
                return Err(denied("handoff cannot cross actual Host roots"));
            }
        } else {
            self.originals
                .set(roots.clone())
                .map_err(|_| denied("handoff root already installed"))?;
        }
        self.outer.bind_original_budgets(roots, &self.tasks)
    }
    pub(super) fn bind(self: &Arc<Self>) -> Result<NativeListenerView, EngineError> {
        let outer = self.outer.bind()?;
        let external = outer.local_addr()?;
        let loopback = if external.is_ipv4() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            IpAddr::V6(Ipv6Addr::LOCALHOST)
        };
        let listener = if self.stream {
            *self.parked.lock().unwrap() = Some(outer);
            let mut declaration = self.declaration.clone();
            if let ResourceDeclaration::TcpListener {
                destination,
                max_bind_attempts,
                ..
            } = &mut declaration
            {
                *destination = destination_for(SocketAddr::new(loopback, 0));
                *max_bind_attempts = NonZeroUsize::new(1).unwrap();
            }
            let grant = Arc::new(
                grant::NativeTcpGrant::new(
                    self.identity,
                    &declaration,
                    Some(SocketAddr::new(loopback, 0)),
                    NonZeroUsize::new(1).unwrap(),
                    self.cancel.clone(),
                    &self.tasks,
                )
                .map_err(std::io::Error::other)?,
            );
            if let Some(roots) = self.originals.get() {
                grant
                    .bind_original_budgets(roots, &self.tasks)
                    .map_err(std::io::Error::other)?;
            }
            // Persist the actual owner and receiver before native creation.
            *self.inner.lock().unwrap() = Some(grant.clone());
            if let Some(receiver) = grant.accepted_task_receiver() {
                self.host
                    .upgrade()
                    .ok_or_else(|| std::io::Error::other("handoff host retired"))?
                    .install_accepted_receiver(receiver)?;
            }
            grant.bind()?
        } else {
            outer
        };
        let stream = listener.local_addr()?;
        let datagram = if self.datagram {
            if self.stream {
                stream
            } else {
                SocketAddr::new(loopback, 0)
            }
        } else {
            external
        };
        self.endpoints
            .set(Endpoints {
                external,
                stream,
                datagram,
            })
            .map_err(|_| std::io::Error::other("handoff already bound"))?;
        Ok(NativeListenerView::Handoff {
            listener,
            grant: self.clone(),
        })
    }
    pub(in crate::runtime::inbound_owners) async fn release_public(
        &self,
        deadline: Instant,
    ) -> Result<(), ChannelError> {
        if self.stream {
            drop(self.parked.lock().unwrap().take());
            let report = self.outer.close_original(deadline).await?;
            if !report.is_complete() {
                return Err(denied("public reservation close remains incomplete"));
            }
            self.report.lock().unwrap().append(report);
        }
        Ok(())
    }
    pub(in crate::runtime::inbound_owners) async fn probe(
        &self,
        deadline: Instant,
    ) -> Result<bool, ChannelError> {
        let external = self
            .endpoints
            .get()
            .ok_or_else(|| denied("handoff not bound"))?
            .external;
        self.probe
            .lock()
            .await
            .run(external, deadline, &self.cancel)
            .await
    }
}
fn destination_for(address: SocketAddr) -> zero_core::request::Destination {
    zero_core::request::Destination::new(
        match address.ip() {
            IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
            IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
        },
        address.port(),
    )
}
pub(super) struct Owner(pub Arc<Grant>);
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: zero_platform_tokio::contract::CloseReason) {
        self.0.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.cancel.cancel();
        drop(self.0.parked.lock().unwrap().take());
        let mut failure = None;
        if let Err(error) = self.0.probe.lock().await.close(deadline).await {
            failure = Some(error);
        }
        let inner = self.0.inner.lock().unwrap().clone();
        for grant in inner.iter().chain(std::iter::once(&self.0.outer)) {
            match grant.close_original(deadline).await {
                Ok(report) => self.0.report.lock().unwrap().append(report),
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        let probes = self.0.probe.lock().await.take_report();
        self.0.report.lock().unwrap().append(probes);
        Ok(std::mem::take(&mut *self.0.report.lock().unwrap()))
    }
}
