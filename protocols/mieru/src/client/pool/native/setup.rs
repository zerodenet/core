//! Actual initializer work uses the existing owning Scope and original slot.
//! Its real exit is consumed before the same slot starts its transport driver.
use super::*;
use crate::client::pool::managed::CarrierLease;
use std::{error::Error, fmt, future::Future, sync::Arc};
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamBinding},
        scope::TaskJoinWitness,
        ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::{CarrierAdoption, CarrierOwnerRegistration},
};

pub(super) mod candidate;

pub(super) enum Prepared {
    Tcp(crate::client::tcp::NativeTcpPreparation),
    Packet(crate::client::packet::NativePacketPreparation),
}
impl Prepared {
    pub(super) async fn start(
        self,
        adoption: CarrierAdoption,
        lease: CarrierLease,
    ) -> io::Result<Arc<ClientConnection>> {
        match self {
            Self::Tcp(prepared) => {
                ClientConnection::start_tcp_controlled(prepared, adoption, lease)
            }
            Self::Packet(prepared) => {
                ClientConnection::start_packet_controlled(prepared, adoption, lease).await
            }
        }
    }
}
#[derive(Clone)]
pub(super) struct Caller {
    pub(super) witness: TaskJoinWitness,
    pub(super) owner: CarrierOwnerRegistration,
    pub(super) binding: UpstreamBinding,
    pub(super) call: CarrierContext,
}
impl Caller {
    pub(super) fn check(
        &self,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        let bound = self.owner.bound().ok_or_else(|| failure(Reason::Revoked))?;
        if self.witness.is_joined()
            || self.call.cancel.is_cancelled()
            || call.cancel.is_cancelled()
            || call.session.is_some()
            || self.call.session.is_some()
            || call.deadline != self.call.deadline
            || !self.witness.scope_identity().resource_open()
            || !bound
                .scope_identity()
                .same_scope(&self.witness.scope_identity())
            || bound.host_cancelled()
            || !matches!(identity,zero_platform_tokio::contract::context::RequestIdentity::Service(facts)
                if facts.origin==zero_core::request::ServiceOrigin::Runtime
                && facts.purpose==zero_core::request::ServicePurpose::BackgroundControl)
        {
            return Err(failure(Reason::StalePlan));
        }
        Ok(self.witness.clone())
    }
}
fn failure(reason: Reason) -> ChannelError {
    let mut failure = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    failure.delivery = DeliveryState::NotSubmitted;
    failure
}
#[derive(Debug)]
struct Source(Arc<io::Error>);
impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for Source {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn report(source: Arc<io::Error>, component: zero_core::component::ComponentId) -> ChannelError {
    let mut error = source
        .get_ref()
        .and_then(|source| source.downcast_ref::<ChannelError>())
        .map_or_else(
            || {
                let mut error = ChannelError::new(Stage::Handshake, Reason::Io, Impact::Component);
                error.delivery = DeliveryState::Unknown;
                error
            },
            |actual| {
                let mut copy = ChannelError::new(actual.stage, actual.reason, actual.impact);
                copy.component = actual.component;
                copy.session = actual.session;
                copy.delivery = actual.delivery;
                copy.retry = actual.retry;
                copy
            },
        );
    error.impact = Impact::Component;
    error.component.get_or_insert(component);
    error.with_source(Source(source))
}
struct CancelOnDrop(Option<zero_platform_tokio::contract::CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(stop) = &self.0 {
            stop.cancel();
        }
    }
}

pub(super) async fn run<T, F, Fut>(
    lease: &CarrierLease,
    adoption: &CarrierAdoption,
    stop: zero_platform_tokio::contract::CancellationToken,
    operation: F,
) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce(TaskJoinWitness) -> Fut + Send + 'static,
    Fut: Future<Output = io::Result<T>> + Send + 'static,
{
    let (begin, started) = tokio::sync::oneshot::channel();
    let (complete, completed) = tokio::sync::oneshot::channel();
    let operation_stop = stop.clone();
    let component = adoption.instance().component;
    let record_stop = lease.cancellation();
    let (_, witness) = adoption
        .spawn_auxiliary(&lease.owner.tasks, async move {
            let Ok(witness) = started.await else {
                return Ok(());
            };
            let outcome = tokio::select! {biased;
                _=operation_stop.cancelled()=>{
                    let _=complete.send(Err(Arc::new(io::Error::from(io::ErrorKind::Interrupted))));
                    return Ok(());
                },
                _=record_stop.cancelled()=>{
                    let _=complete.send(Err(Arc::new(io::Error::from(io::ErrorKind::Interrupted))));
                    return Ok(());
                },
                result=operation(witness)=>result,
            };
            match outcome {
                Ok(prepared) => {
                    let _ = complete.send(Ok(prepared));
                    Ok(())
                }
                Err(source) => {
                    let source = Arc::new(source);
                    let _ = complete.send(Err(source.clone()));
                    // The actual Scope retains this original error even if the
                    // waiting operation disappears before receiving its result.
                    Err(report(source, component))
                }
            }
        })
        .map_err(io::Error::other)?;
    let wait_stop = stop.clone();
    let mut cancel_on_drop = CancelOnDrop(Some(stop));
    begin
        .send(witness.clone())
        .map_err(|_| io::ErrorKind::Interrupted)?;
    let outcome = completed.await.map_err(|_| io::ErrorKind::Interrupted)?;
    // This is the actual host Scope's consumed auxiliary witness, not task body
    // completion. Accept its original failure before reusing the one task slot.
    let joined = adoption
        .accept_auxiliary()
        .await
        .map_err(io::Error::other)?;
    if !joined
        .as_ref()
        .is_some_and(|joined| witness.matches_joined(joined))
    {
        return Err(io::Error::other(failure(Reason::Internal)));
    }
    let released = tokio::select! {biased;
        released=witness.wait_registration_released()=>Some(released),
        _=wait_stop.cancelled()=>None,
        _=lease.owner.scope_cancel.cancelled()=>None,
    };
    if let Some(released) = released {
        if !witness.matches_joined(&released) || !released.registration_released() {
            return Err(io::Error::other(failure(Reason::Internal)));
        }
    } else {
        return match outcome {
            Err(source) => Err(io::Error::new(source.kind(), report(source, component))),
            Ok(_) => Err(io::ErrorKind::Interrupted.into()),
        };
    }
    // Success transfers this exact preparation to the same slot's driver.
    // Do not cancel the canonical native token during that transfer.
    if outcome.is_ok() {
        drop(cancel_on_drop.0.take());
    }
    outcome.map_err(|source| io::Error::new(source.kind(), report(source, component)))
}
