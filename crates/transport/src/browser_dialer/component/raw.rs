//! Weak access to the same installed Browser original for existing upper pools.
use super::*;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
};
use zero_core::{
    component::ComponentInstanceId,
    request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity, ServiceFactsOwner},
        network::{CarrierContext, PendingCarrierObserver, UpstreamBinding},
    },
    controlled_carrier::loan::StreamLoan,
};
use zero_traits::packet_metadata::PacketMetadataWeak;
mod factory;
pub use factory::{RawFactory, RawOptions};
#[derive(Default)]
pub(crate) struct Registry(Mutex<HashMap<Vec<u8>, Arc<Slot>>>);
struct Registered {
    owner: PacketMetadataWeak<super::factory::Shared>,
    closed: PacketMetadataArc<AtomicBool>,
}
struct Slot(Mutex<Option<Registered>>);
#[derive(Clone)]
pub struct RawCarrier {
    slot: Arc<Slot>,
    endpoint: Destination,
}
impl Registry {
    pub(crate) fn access(&self, identity: &[u8], endpoint: Destination) -> RawCarrier {
        let mut slots = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        slots.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || slot
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .is_some_and(|old| !old.closed.load(Ordering::Acquire))
        });
        let slot = slots
            .entry(identity.to_vec())
            .or_insert_with(|| Arc::new(Slot(Mutex::new(None))))
            .clone();
        RawCarrier { slot, endpoint }
    }
}
impl RawCarrier {
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    fn install(
        &self,
        owner: &PacketMetadataArc<super::factory::Shared>,
    ) -> Result<(), ChannelError> {
        let mut slot = self
            .slot
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(old) = &*slot {
            if old
                .owner
                .upgrade()
                .is_some_and(|actual| PacketMetadataArc::ptr_eq(&actual, owner))
            {
                return Ok(());
            }
            if !old.closed.load(Ordering::Acquire) {
                return Err(error(Stage::Prepare, Reason::StalePlan));
            }
        }
        *slot = Some(Registered {
            owner: PacketMetadataArc::downgrade(owner),
            closed: owner.closed.clone(),
        });
        Ok(())
    }
    pub async fn open_carrier(
        &self,
        expected: ComponentInstanceId,
        binding: UpstreamBinding,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> Result<StreamLoan, ChannelError> {
        let owner = self
            .slot
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|entry| entry.owner.upgrade())
            .filter(|owner| {
                owner.instance == expected
                    && owner.ready.load(Ordering::Acquire)
                    && !owner.cancel.is_cancelled()
                    && !owner.closed.load(Ordering::Acquire)
            })
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        if call.session.is_some()
            || call.cancel.is_cancelled()
            || binding.dial != owner.options.dial
            || !binding.prefix.is_empty()
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let credit = owner
            .memory
            .reserve(ServiceFactsOwner::paid_layout().map_err(capacity)?)
            .map_err(capacity)?;
        let identity = ServiceFactsOwner::from_prepaid(
            ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::BackgroundControl,
            },
            credit,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        let upstream = match waiting {
            Some(waiting) => {
                owner
                    .upstream
                    .select_background_carrier_observed(Some(binding), call.clone(), waiting)
                    .await?
            }
            None => {
                owner
                    .upstream
                    .select_background_carrier(Some(binding), call.clone())
                    .await?
            }
        };
        let context = OpenContext {
            identity: RequestIdentity::Service(identity),
            deadline: call.deadline,
            cancel: call.cancel,
            upstream,
            observer: None,
        };
        let (io, control, original) = Handle(owner).open_parts(&context).await?;
        Ok(StreamLoan::new(Box::pin(io), control, original))
    }
}
