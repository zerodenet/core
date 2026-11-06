//! The ordinary node's Datagram role uses the existing solo cache or real prefix.
use super::{
    logical,
    pool::{error, Managed, PoolRoot, UdpPoolOptions},
};
use std::{
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Weak},
};
use tokio::time::Instant;
use zero_core::{
    component::{ComponentId, ComponentInstanceId},
    request::DatagramTargets,
};
use zero_platform_tokio::{
    contract::{
        admission::{SessionAdmission, SessionOwner},
        component::{BuildContext, ResourceAccess, ResourceDeclaration},
        context::OpenContext,
        datagram::{
            BoxDatagramSession, DatagramLimits, DatagramOutbound, DatagramRequest, DatagramRx,
            DatagramSession, DatagramTx,
        },
        lifecycle::{ChannelControl, CloseReport, DynChannelControl, ResourceLifecycle},
        network::CarrierContext,
        scope::TaskRegistrar,
        CancellationToken, ChannelError, CloseReason, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{loan::DatagramLoan, CarrierGrants, CarrierOrigin, CarrierOwner},
};
#[derive(Clone)]
pub(crate) struct RoleOptions {
    root: Weak<PoolRoot>,
    pub(crate) pool: ComponentId,
    pub(crate) process: Option<(ComponentId, std::time::Duration)>,
    endpoint: zero_core::request::Destination,
    binding: zero_platform_tokio::contract::network::UpstreamBinding,
    resume: crate::udp::ShadowsocksUdpFlowResume,
}
impl UdpPoolOptions {
    pub(crate) fn role(&self, pool: ComponentId) -> RoleOptions {
        RoleOptions {
            root: self.root.clone(),
            pool,
            process: self.process,
            endpoint: self.endpoint.clone(),
            binding: self.binding.clone(),
            resume: self.resume.protocol.clone(),
        }
    }
}
impl RoleOptions {
    pub(crate) fn declaration(&self) -> ResourceDeclaration {
        ResourceDeclaration::ControlledCarrierPool {
            endpoint: self.endpoint.clone(),
            dial: self.binding.dial.clone(),
            datagram: true,
            max_sockets: NonZeroUsize::new(4096).unwrap(),
        }
    }
    fn actual(&self, instance: ComponentInstanceId) -> Result<Arc<Managed>, ChannelError> {
        self.root
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?
            .managed(instance)
    }
}
pub(crate) struct RoleOwner {
    carrier: CarrierOwner,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    session: SessionOwner,
    ordinal: usize,
}
pub(crate) struct Outbound {
    options: RoleOptions,
    pool: ComponentInstanceId,
    admission: Arc<SessionAdmission>,
    grants: CarrierGrants,
    process: Option<(ComponentInstanceId, std::time::Duration)>,
}
impl RoleOwner {
    pub(crate) fn build(
        options: RoleOptions,
        context: &BuildContext,
        ordinal: usize,
    ) -> Result<(Self, Outbound), ChannelError> {
        let pool = context
            .dependency(options.pool)
            .ok_or_else(|| error(context.instance(), Stage::Prepare, Reason::StalePlan))?;
        let carrier = CarrierOwner::new(
            context.instance(),
            NonZeroUsize::new(4096).unwrap(),
            &context.cancel,
        );
        let (session, admission) = SessionOwner::new(context.component, &context.cancel);
        let outbound = Outbound {
            options: options.clone(),
            pool,
            admission,
            grants: carrier.grants(),
            process: options
                .process
                .map(|(id, startup)| {
                    context
                        .dependency(id)
                        .map(|instance| (instance, startup))
                        .ok_or_else(|| error(context.instance(), Stage::Prepare, Reason::StalePlan))
                })
                .transpose()?,
        };
        Ok((
            Self {
                carrier,
                resources: context.resources.clone(),
                tasks: context.tasks.clone(),
                session,
                ordinal,
            },
            outbound,
        ))
    }
}
impl ResourceLifecycle for RoleOwner {
    async fn start(&mut self, end: Instant) -> Result<(), ChannelError> {
        self.resources.register_controlled_carrier(
            self.ordinal,
            self.carrier.registration(),
            &self.tasks,
        )?;
        self.carrier.start(end).await?;
        self.session.start(end).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.session.revoke(reason);
        self.carrier.revoke();
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let mut report = self.session.shutdown(end).await?;
        report.append(self.carrier.shutdown(end).await?);
        Ok(report)
    }
}
pub struct Control {
    inner: ControlKind,
    original: CarrierOrigin,
    admission: Arc<SessionAdmission>,
    caller: CancellationToken,
}
enum ControlKind {
    Cached(logical::Control),
    Borrowed(Box<dyn DynChannelControl>),
}
impl ChannelControl for Control {
    fn revoked(&self) -> zero_platform_tokio::contract::BoxFuture<'static, ()> {
        let inner = match &self.inner {
            ControlKind::Cached(control) => control.revoked(),
            ControlKind::Borrowed(control) => control.revoked(),
        };
        let (admission, caller) = (self.admission.clone(), self.caller.clone());
        Box::pin(async move {
            tokio::select! {_=inner=>{},_=admission.cancelled()=>{},_=caller.cancelled()=>{}}
        })
    }
    fn cancel(&self, reason: CloseReason) {
        match &self.inner {
            ControlKind::Cached(control) => control.cancel(reason),
            ControlKind::Borrowed(control) => control.cancel(reason),
        }
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        match &mut self.inner {
            ControlKind::Cached(control) => control.try_close(),
            ControlKind::Borrowed(control) => control.try_close(),
        }
    }
    async fn wait_closed(&mut self, end: Instant) -> Result<CloseReport, ChannelError> {
        match &mut self.inner {
            ControlKind::Cached(control) => control.wait_closed(end).await,
            ControlKind::Borrowed(control) => control.wait_closed(end).await,
        }
    }
}
mod open;

struct Pending {
    original: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    armed: bool,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if self.armed {
            self.original.request_retirement();
        }
    }
}
mod codec;

impl Drop for Control {
    fn drop(&mut self) {
        self.cancel(CloseReason::Shutdown);
    }
}
