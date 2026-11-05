//! Host generations fence each new operation, including cached role handles.
use super::instance::invalid;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    stream::{BoxStream, ConnectRequest, Connected, DynStreamOutbound},
    BoxFuture, ChannelError, Reason,
};
pub(in crate::component) mod borrowing;
mod packet;
pub(super) use packet::FencedPacket;
mod service;
pub use service::ServiceAdmission;

#[derive(Default)]
struct View {
    stopping: bool,
    loans: borrowing::LoanEdges,
    loan_revision: u64,
    loan_frozen: bool,
    loan_staged: bool,
    current: BTreeMap<ComponentInstanceId, Arc<()>>,
    preparing: BTreeMap<ComponentInstanceId, Arc<()>>,
}
#[derive(Default)]
pub(super) struct HostAdmission(RwLock<View>, tokio::sync::Notify);
impl HostAdmission {
    pub(in crate::component) fn signal(&self) -> &tokio::sync::Notify {
        &self.1
    }
    pub(in crate::component) fn check_installed(
        &self,
        instance: ComponentInstanceId,
        owner: &Arc<()>,
    ) -> Result<(), ChannelError> {
        let view = self.0.read().expect("host admission poisoned");
        if view
            .current
            .get(&instance)
            .into_iter()
            .chain(view.preparing.get(&instance))
            .any(|token| Arc::ptr_eq(token, owner))
        {
            return Ok(());
        }
        let mut error = invalid(
            Reason::StalePlan,
            "component runtime owner is not installed by the host",
        );
        error.component = Some(instance.component);
        Err(error)
    }
    pub(in crate::component) fn revoke(&self, instance: ComponentInstanceId, owner: &Arc<()>) {
        let mut view = self.0.write().expect("host admission poisoned");
        let current = view
            .current
            .get(&instance)
            .is_some_and(|token| Arc::ptr_eq(token, owner));
        let preparing = view
            .preparing
            .get(&instance)
            .is_some_and(|token| Arc::ptr_eq(token, owner));
        if current {
            view.current.remove(&instance);
        }
        if preparing {
            view.preparing.remove(&instance);
        }
        drop(view);
        if current || preparing {
            self.1.notify_waiters();
        }
    }
    pub(super) fn stage(&self, instances: impl Iterator<Item = (ComponentInstanceId, Arc<()>)>) {
        let mut view = self.0.write().expect("host admission poisoned");
        view.preparing = instances.collect();
        view.loan_staged = true;
        view.loan_frozen = false;
        drop(view);
        self.1.notify_waiters();
    }
    pub(super) fn publish(&self, instances: impl Iterator<Item = (ComponentInstanceId, Arc<()>)>) {
        let mut view = self.0.write().expect("host admission poisoned");
        view.current = instances.collect();
        view.preparing.clear();
        view.loan_staged = false;
        let retained: std::collections::BTreeSet<_> = view.current.keys().copied().collect();
        let count = view.loans.len();
        view.loans
            .retain(|(a, b)| retained.contains(a) && retained.contains(b));
        if view.loans.len() != count {
            view.loan_revision = view.loan_revision.saturating_add(1);
        }
        view.loan_frozen = false;
        drop(view);
        self.1.notify_waiters();
    }
    pub(super) fn stop_business(&self) {
        self.0.write().expect("host admission poisoned").stopping = true;
        self.1.notify_waiters();
    }
    pub(super) fn close(&self) {
        let mut view = self.0.write().expect("host admission poisoned");
        view.current.clear();
        view.preparing.clear();
        view.loan_frozen = true;
        drop(view);
        self.1.notify_waiters();
    }
    pub(super) fn check(
        &self,
        instance: ComponentInstanceId,
        owner: &Arc<()>,
        context: &OpenContext,
    ) -> Result<(), ChannelError> {
        self.check_identity(instance, owner, &context.identity)
    }
    pub(in crate::component) fn check_identity(
        &self,
        instance: ComponentInstanceId,
        owner: &Arc<()>,
        identity: &RequestIdentity,
    ) -> Result<(), ChannelError> {
        let view = self.0.read().expect("host admission poisoned");
        let service = matches!(identity, RequestIdentity::Service(_));
        let permitted = ((!view.stopping || service)
            && view
                .current
                .get(&instance)
                .is_some_and(|token| Arc::ptr_eq(token, owner)))
            || (service
                && view
                    .preparing
                    .get(&instance)
                    .is_some_and(|token| Arc::ptr_eq(token, owner)));
        if permitted {
            return Ok(());
        }
        let mut error = invalid(
            Reason::StalePlan,
            "component generation is not admitted by the host",
        );
        error.component = Some(instance.component);
        error.session = identity.session();
        Err(error)
    }
}

pub(super) struct FencedStream {
    pub(super) inner: std::sync::Weak<dyn DynStreamOutbound>,
    pub(super) host: Arc<HostAdmission>,
    pub(super) instance: ComponentInstanceId,
    pub(super) owner: Arc<()>,
    pub(super) users: Arc<[Arc<super::instance::users::Users>]>,
}
impl DynStreamOutbound for FencedStream {
    fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<
        '_,
        Result<Connected<zero_platform_tokio::controlled_carrier::loan::StreamLoan>, ChannelError>,
    > {
        Box::pin(async move {
            let session = context.session_identity()?;
            self.host.check(self.instance, &self.owner, &context)?;
            let inner = self
                .inner
                .upgrade()
                .ok_or_else(|| retired_role(self.instance, session))?;
            let Some(lease) = super::instance::users::Lease::enter(
                self.instance.component,
                &self.users,
                session,
            )?
            else {
                return inner.connect_carrier(request, context).await;
            };
            let mut connected = tokio::select! { biased;
                _ = lease.revoked() => return Err(lease.failure(zero_platform_tokio::contract::Stage::Dial)),
                result = inner.connect_carrier(request, context) => result?,
            };
            connected.io.io.wrap(|io| lease.stream(io));
            Ok(connected)
        })
    }
    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<BoxStream>, ChannelError>> {
        Box::pin(async move {
            let session = context.session_identity()?;
            self.host.check(self.instance, &self.owner, &context)?;
            let inner = self
                .inner
                .upgrade()
                .ok_or_else(|| retired_role(self.instance, session))?;
            let Some(lease) = super::instance::users::Lease::enter(
                self.instance.component,
                &self.users,
                session,
            )?
            else {
                return inner.connect(request, context).await;
            };
            let connected = tokio::select! { biased;
                _ = lease.revoked() => return Err(lease.failure(zero_platform_tokio::contract::Stage::Dial)),
                result = inner.connect(request, context) => result?,
            };
            Ok(Connected {
                io: lease.stream(connected.io),
                peer_confirmation: connected.peer_confirmation,
            })
        })
    }
}

// Only new opens consult the host view. Transferred sessions keep their real
// protocol/session owner and are not spuriously cancelled by a publication.
pub(super) struct FencedDatagram {
    pub(super) inner:
        std::sync::Weak<dyn zero_platform_tokio::contract::datagram::DynDatagramOutbound>,
    pub(super) host: Arc<HostAdmission>,
    pub(super) instance: ComponentInstanceId,
    pub(super) owner: Arc<()>,
    pub(super) users: Arc<[Arc<super::instance::users::Users>]>,
}
impl zero_platform_tokio::contract::datagram::DynDatagramOutbound for FencedDatagram {
    fn open_carrier(
        &self,
        request: zero_platform_tokio::contract::datagram::DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError>,
    > {
        Box::pin(async move {
            let session = context.session_identity()?;
            self.host.check(self.instance, &self.owner, &context)?;
            let inner = self
                .inner
                .upgrade()
                .ok_or_else(|| retired_role(self.instance, session))?;
            let Some(lease) = super::instance::users::Lease::enter(
                self.instance.component,
                &self.users,
                session,
            )?
            else {
                return inner.open_carrier(request, context).await;
            };
            let mut loan = tokio::select! {biased;_=lease.revoked()=>return Err(lease.failure(zero_platform_tokio::contract::Stage::Dial)),result=inner.open_carrier(request,context)=>result?};
            lease.datagram_loan(&mut loan);
            Ok(loan)
        })
    }
    fn open(
        &self,
        request: zero_platform_tokio::contract::datagram::DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async move {
            let session = context.session_identity()?;
            self.host.check(self.instance, &self.owner, &context)?;
            let inner = self
                .inner
                .upgrade()
                .ok_or_else(|| retired_role(self.instance, session))?;
            let Some(lease) = super::instance::users::Lease::enter(
                self.instance.component,
                &self.users,
                session,
            )?
            else {
                return inner.open(request, context).await;
            };
            let session = tokio::select! { biased;
                _ = lease.revoked() => return Err(lease.failure(zero_platform_tokio::contract::Stage::Dial)),
                result = inner.open(request, context) => result?,
            };
            Ok(lease.datagram(session))
        })
    }
}

fn retired_role(
    instance: ComponentInstanceId,
    session: Option<zero_core::component::SessionId>,
) -> ChannelError {
    let mut error = invalid(Reason::Revoked, "component role owner retired");
    error.component = Some(instance.component);
    error.session = session;
    error
}
