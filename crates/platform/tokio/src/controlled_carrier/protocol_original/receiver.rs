//! Historical exact strong-owner handoff; not native close or Scope/task ACK.
use super::*;
use crate::contract::scope::TaskJoinWitness;
#[derive(Clone)]
pub struct ProtocolOriginalReceiver {
    original: Receiver,
    protocol: TaskJoinWitness,
}
#[derive(Clone)]
enum Receiver {
    Metadata(crate::controlled_datagram::metadata_pool::MetadataDriverRetained),
    Carrier(CarrierDriverRetained),
    Loan {
        child: LoanChild,
        keeper: CarrierDriverRetained,
        _loan: super::super::CarrierLoanRetained,
    },
}
#[derive(Clone)]
enum LoanChild {
    Metadata(MetadataPoolOrigin),
    Carrier {
        slot: std::sync::Weak<super::super::slot::Slot>,
        instance: ComponentInstanceId,
    },
}
impl LoanChild {
    fn new(original: &Original) -> Self {
        match original {
            Original::Metadata(original) => Self::Metadata(original.clone()),
            Original::Carrier(original) => Self::Carrier {
                slot: Arc::downgrade(&original.slot),
                instance: original.instance(),
            },
        }
    }
    fn instance(&self) -> ComponentInstanceId {
        match self {
            Self::Metadata(original) => original.instance(),
            Self::Carrier { instance, .. } => *instance,
        }
    }
    fn names(&self, original: &Original) -> bool {
        match (self, original) {
            (Self::Metadata(a), Original::Metadata(b)) => a.same_original(b),
            (Self::Carrier { slot, .. }, Original::Carrier(b)) => {
                slot.ptr_eq(&Arc::downgrade(&b.slot))
            }
            _ => false,
        }
    }
}
impl ProtocolOriginalReceiver {
    pub(crate) fn metadata(
        original: crate::controlled_datagram::metadata_pool::MetadataDriverRetained,
    ) -> Self {
        let protocol = original.protocol_driver().clone();
        Self {
            original: Receiver::Metadata(original),
            protocol,
        }
    }
    pub(crate) fn carrier(original: CarrierDriverRetained, protocol: TaskJoinWitness) -> Self {
        Self {
            original: Receiver::Carrier(original),
            protocol,
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        match &self.original {
            Receiver::Metadata(original) => original.instance(),
            Receiver::Carrier(original) => original.instance(),
            Receiver::Loan { child, .. } => child.instance(),
        }
    }
    pub fn protocol_driver(&self) -> &TaskJoinWitness {
        match &self.original {
            Receiver::Loan { keeper, .. } => keeper.driver(),
            _ => &self.protocol,
        }
    }
    pub(crate) fn names_metadata(&self, original: &MetadataPoolOrigin) -> bool {
        match &self.original {
            Receiver::Metadata(actual) => actual.names(original),
            Receiver::Loan {
                child: LoanChild::Metadata(actual),
                ..
            } => actual.same_original(original),
            _ => false,
        }
    }
    pub(crate) fn metadata_for(
        &self,
        original: &MetadataPoolOrigin,
    ) -> Option<crate::controlled_datagram::metadata_pool::MetadataDriverRetained> {
        match &self.original {
            Receiver::Metadata(actual) if actual.names(original) => Some(actual.clone()),
            _ => None,
        }
    }
    pub(crate) fn names(&self, original: &ProtocolOriginal) -> bool {
        match (&self.original, &original.original) {
            (Receiver::Metadata(a), Original::Metadata(b)) => a.names(b),
            (Receiver::Carrier(a), Original::Carrier(b)) => a.names_original(b),
            (Receiver::Loan { child, .. }, original) => child.names(original),
            _ => false,
        }
    }
}

/// One-use publication of a receiver already validated against its unique IO
/// claim and original driver graph. Confirm only after releasing the owning
/// Entry gate: publication wakes the original Root/caller.
pub struct ProtocolReceiverPrepared {
    phase: std::sync::Weak<crate::original_protocol::Phase>,
    receiver: ProtocolOriginalReceiver,
}
impl ProtocolReceiverPrepared {
    pub(crate) fn new(
        phase: &Arc<crate::original_protocol::Phase>,
        receiver: ProtocolOriginalReceiver,
    ) -> Self {
        Self {
            phase: Arc::downgrade(phase),
            receiver,
        }
    }
    pub fn confirm(self) -> Result<ProtocolOriginalReceiver, ChannelError> {
        let phase = self.phase.upgrade().ok_or_else(|| {
            ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::Revoked,
                crate::contract::Impact::Component,
            )
        })?;
        phase.retain_receiver(self.receiver.clone())?;
        Ok(self.receiver)
    }
}

impl ProtocolOriginal {
    /// The parent's actual driver is already installed over this same unique
    /// retained LoanOwner. A same-Scope witness or equal endpoint is insufficient.
    /// The returned value is only a historical handoff; all child/parent native,
    /// logical views, Root N and original Host ACKs stay independently charged.
    pub fn prepare_loan_receiver(
        &self,
        loan: super::super::CarrierLoanRetained,
        keeper: CarrierDriverRetained,
    ) -> Result<ProtocolReceiverPrepared, ChannelError> {
        let fail = || {
            ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::StalePlan,
                crate::contract::Impact::Component,
            )
        };
        let parent = keeper.adoption().ok_or_else(fail)?;
        if !parent.has_original_root()
            || !loan
                .receiver_adoption()
                .is_some_and(|actual| actual.same_slot(&parent))
            || !loan.retains_protocol_original(self)
        {
            return Err(fail());
        }
        let receiver = ProtocolOriginalReceiver {
            original: Receiver::Loan {
                child: LoanChild::new(&self.original),
                keeper: keeper.clone(),
                _loan: loan,
            },
            protocol: keeper.driver().clone(),
        };
        let phase = self.retirement.original_phase().ok_or_else(fail)?;
        Ok(ProtocolReceiverPrepared::new(&phase, receiver))
    }
}
