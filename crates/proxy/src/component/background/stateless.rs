//! Actual taskless protocol framing over one same-slot native or borrowed carrier.
//! The protocol keeps its existing role, TLS/security state and lifecycle.
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::time::Instant;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentSpec, ResourceAccess,
            ResourceDeclaration,
        },
        context::OpenContext,
        lifecycle::{
            erase_lifecycle, BuildError, CloseReport, DynChannelControl, DynResourceLifecycle,
            ResourceLifecycle,
        },
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        scope::TaskRegistrar,
        stream::{
            erase_stream, BoxStream, ConnectRequest, Connected, DynStreamOutbound, StreamOutbound,
        },
        BoxFuture, CancellationToken, ChannelError, CloseReason, Reason,
    },
    controlled_carrier::{
        loan::{LoanOwner, StreamLoan},
        CarrierGrant, CarrierGrants, CarrierOwner,
    },
};

pub(crate) struct Options<T> {
    inner: Arc<T>,
    endpoint: Destination,
    dial: zero_traits::DialPolicy,
    limit: NonZeroUsize,
}
impl<T> Options<T> {
    pub(crate) fn new(
        inner: T,
        endpoint: Destination,
        dial: zero_traits::DialPolicy,
        limit: NonZeroUsize,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            endpoint,
            dial,
            limit,
        }
    }
}
pub(crate) struct Factory<F>(pub(crate) F);
impl<F: ComponentFactory> ComponentFactory for Factory<F> {
    type Options = Options<F::Options>;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        let mut spec = self.0.describe(&options.inner);
        spec.resources
            .push(ResourceDeclaration::ControlledCarrierPool {
                endpoint: options.endpoint.clone(),
                dial: options.dial.clone(),
                datagram: false,
                max_sockets: options.limit,
            });
        let mut identity = spec.static_identity.to_vec();
        identity.extend_from_slice(b"same-slot-taskless-carrier-v1");
        identity.extend_from_slice(&options.limit.get().to_le_bytes());
        identity.extend_from_slice(format!("{:?}{:?}", options.endpoint, options.dial).as_bytes());
        spec.static_identity = identity.into();
        spec
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let original = self.0.describe(&options.inner);
        // A transport driver must keep its actual protocol owner; it cannot be
        // made taskless by placing its role inside this custody adapter.
        if original.task_limit.is_some() {
            return Err(super::super::instance::invalid(
                Reason::InvalidConfig,
                "carrier framing requires the actual taskless role",
            ));
        }
        let carrier = CarrierOwner::new(context.instance(), options.limit, &context.cancel);
        let resources = context.resources.clone();
        let tasks = context.tasks.clone();
        let endpoint = options.endpoint.clone();
        let dial = options.dial.clone();
        let transfers = Arc::new(Mutex::new(Vec::new()));
        let mut instance = self.0.build(options.inner.clone(), context)?;
        let inner = instance.roles.stream.take().ok_or_else(|| {
            super::super::instance::invalid(
                Reason::InvalidConfig,
                "carrier framing requires the actual Stream role",
            )
        })?;
        instance.roles.stream = Some(erase_stream(Outbound {
            inner,
            endpoint,
            dial,
            grants: carrier.grants(),
            transfers: transfers.clone(),
        }));
        instance.lifecycle = erase_lifecycle(Owner {
            inner: instance.lifecycle,
            carrier,
            resources,
            tasks,
            ordinal: original.resources.len(),
            transfers,
        });
        Ok(instance)
    }
}

struct Transfer {
    grant: Mutex<Option<CarrierGrant>>,
    owner: tokio::sync::Mutex<Option<LoanOwner>>,
    stop: CancellationToken,
    published: std::sync::atomic::AtomicBool,
    opening: std::sync::atomic::AtomicBool,
}
type Transfers = Arc<Mutex<Vec<Arc<Transfer>>>>;
impl Transfer {
    async fn close(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.stop.cancel();
        let mut guard = tokio::time::timeout_at(end, self.owner.lock())
            .await
            .map_err(|source| {
                super::super::instance::invalid(
                    Reason::Timeout,
                    "taskless original logical control is busy",
                )
                .with_source(source)
            })?;
        let Some(owner) = guard.as_mut() else {
            return Ok(CloseReport::default());
        };
        let report = owner.close(end).await?;
        if report.is_complete() {
            *guard = None;
        }
        Ok(report)
    }
    fn finished(&self) -> bool {
        !self.opening.load(std::sync::atomic::Ordering::Acquire)
            && self.owner.try_lock().is_ok_and(|owner| owner.is_none())
    }
}
struct Opening(Arc<Transfer>);
impl Drop for Opening {
    fn drop(&mut self) {
        self.0
            .opening
            .store(false, std::sync::atomic::Ordering::Release);
    }
}
struct Owner {
    inner: Box<dyn DynResourceLifecycle>,
    carrier: CarrierOwner,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    ordinal: usize,
    transfers: Transfers,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.resources.register_controlled_carrier(
            self.ordinal,
            self.carrier.registration(),
            &self.tasks,
        )?;
        self.carrier.start(end).await?;
        self.inner.start(end).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
        self.carrier.revoke();
        for transfer in self.transfers.lock().unwrap().iter() {
            transfer.stop.cancel();
        }
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let transfers = self.transfers.lock().unwrap().clone();
        let mut report = self.inner.shutdown(end).await?;
        for transfer in transfers {
            report.append(transfer.close(end).await?);
        }
        self.transfers
            .lock()
            .unwrap()
            .retain(|transfer| !transfer.finished());
        report.append(self.carrier.shutdown(end).await?);
        Ok(report)
    }
}
struct Outbound {
    inner: Arc<dyn DynStreamOutbound>,
    endpoint: Destination,
    dial: zero_traits::DialPolicy,
    grants: CarrierGrants,
    transfers: Transfers,
}
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        self.inner.connect(request, context).await
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        mut context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        if context.session_identity()?.is_some() || context.upstream.binding().dial != self.dial {
            return Err(super::super::instance::invalid(
                Reason::StalePlan,
                "taskless carrier requires its actual Service binding",
            ));
        }
        let end = context.deadline.ok_or_else(|| {
            super::super::instance::invalid(
                Reason::InvalidConfig,
                "taskless carrier requires a finite custody deadline",
            )
        })?;
        let pending = self.transfers.lock().unwrap().clone();
        for transfer in pending {
            if !transfer
                .published
                .load(std::sync::atomic::Ordering::Acquire)
                && !transfer.opening.load(std::sync::atomic::Ordering::Acquire)
            {
                let report = transfer.close(end).await?;
                if !report.is_complete() {
                    return Err(super::super::instance::invalid(
                        Reason::Capacity,
                        "previous actual taskless carrier cleanup is unfinished",
                    ));
                }
            }
        }
        self.transfers
            .lock()
            .unwrap()
            .retain(|transfer| !transfer.finished());
        self.grants.reap_returned(end).await?;
        let grant = self.grants.reserve()?;
        // The actual waiting caller must hold cleanup authority before this
        // protocol can suspend or acquire native I/O. A persistent component
        // ledger alone cannot clean a cancelled, never-delivered attempt.
        context.upstream.retain_pending_carrier(grant.adoption())?;
        let transfer = Arc::new(Transfer {
            grant: Mutex::new(Some(grant)),
            owner: tokio::sync::Mutex::new(None),
            stop: context.cancel.child_token(),
            published: false.into(),
            opening: true.into(),
        });
        // The persistent owning slot retains this original attempt before the
        // protocol's first await, including handshake error or future cancellation.
        self.transfers.lock().unwrap().push(transfer.clone());
        let _opening = Opening(transfer.clone());
        context.upstream = UpstreamTransport::new(
            context.upstream.binding().clone(),
            Arc::new(Opener {
                upstream: context.upstream.clone(),
                endpoint: self.endpoint.clone(),
                transfer: transfer.clone(),
            }),
        );
        let connected = self.inner.connect(request, context).await?;
        let original = transfer
            .owner
            .lock()
            .await
            .as_ref()
            .ok_or_else(|| {
                super::super::instance::invalid(
                    Reason::InvalidConfig,
                    "protocol did not consume its unique actual carrier",
                )
            })?
            .original()
            .clone();
        transfer
            .published
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(Connected {
            io: StreamLoan::new(connected.io, Box::new(Control(transfer)), original),
            peer_confirmation: connected.peer_confirmation,
        })
    }
}
struct Opener {
    upstream: UpstreamTransport,
    endpoint: Destination,
    transfer: Arc<Transfer>,
}
impl UpstreamOpener for Opener {
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding != self.upstream.binding() {
            return Err(super::super::instance::invalid(
                Reason::StalePlan,
                "nested pending original binding changed",
            ));
        }
        self.upstream.retain_pending_carrier(adoption)
    }
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        mut call: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            let mut original = self.transfer.owner.lock().await;
            if &binding != self.upstream.binding()
                || endpoint != self.endpoint
                || call.session.is_some()
                || original.is_some()
            {
                return Err(super::super::instance::invalid(
                    Reason::StalePlan,
                    "protocol requested a foreign or duplicate carrier",
                ));
            }
            call.cancel = self.transfer.stop.clone();
            let mut grant = self.transfer.grant.lock().unwrap().take().ok_or_else(|| {
                super::super::instance::invalid(
                    Reason::StalePlan,
                    "protocol already consumed its actual carrier grant",
                )
            })?;
            let view = grant.claim_transferred_view()?;
            call.deadline.ok_or_else(|| {
                super::super::instance::invalid(
                    Reason::InvalidConfig,
                    "taskless native acquisition requires a deadline",
                )
            })?;
            let io = if binding.prefix.is_empty() {
                self.upstream
                    .acquire_controlled_stream(endpoint, call, grant)
                    .await?
            } else {
                let loan = self
                    .upstream
                    .borrow_stream(endpoint.clone(), call.clone())
                    .await?;
                grant.adopt_stream_loan(&binding, endpoint, &call, loan)?
            };
            let loan = io.into_stream_loan(view)?;
            let StreamLoan { io, owner } = loan;
            // Keep the original control in the persistent actual owner before
            // returning the view to protocol TLS/framing/handshake code.
            *original = Some(owner);
            Ok(Box::pin(io) as BoxStream)
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async {
            Err(super::super::instance::invalid(
                Reason::Unsupported,
                "a Stream carrier has no unrelated Datagram authority",
            ))
        })
    }
    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        self.upstream.carrier_dns_generation()
    }
}
struct Control(Arc<Transfer>);
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.stop.cancel();
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(self.0.close(end))
    }
}
