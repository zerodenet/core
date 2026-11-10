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
    Accepted(crate::controlled_listener::accepted::AcceptedTcpCarrierOrigin),
    Carrier {
        slot: std::sync::Weak<super::super::slot::Slot>,
        instance: ComponentInstanceId,
    },
}
impl LoanChild {
    fn new(original: &Original) -> Self {
        match original {
            Original::Accepted(original) => Self::Accepted(original.clone()),
            Original::Metadata(original) => Self::Metadata(original.clone()),
            Original::Carrier(original) => Self::Carrier {
                slot: Arc::downgrade(&original.slot),
                instance: original.instance(),
            },
        }
    }
    fn instance(&self) -> ComponentInstanceId {
        match self {
            Self::Accepted(original) => original.instance(),
            Self::Metadata(original) => original.instance(),
            Self::Carrier { instance, .. } => *instance,
        }
    }
    fn names(&self, original: &Original) -> bool {
        match (self, original) {
            (Self::Accepted(a), Original::Accepted(b)) => a.same_original(b),
            (Self::Metadata(a), Original::Metadata(b)) => a.same_original(b),
            (Self::Carrier { slot, .. }, Original::Carrier(b)) => {
                slot.ptr_eq(&Arc::downgrade(&b.slot))
            }
            _ => false,
        }
    }
}
impl ProtocolOriginalReceiver {
    pub(crate) fn accepted_loan(
        original: crate::controlled_listener::accepted::AcceptedTcpCarrierOrigin,
        loan: super::super::CarrierLoanRetained,
        keeper: CarrierDriverRetained,
    ) -> Self {
        Self {
            protocol: keeper.driver().clone(),
            original: Receiver::Loan {
                child: LoanChild::Accepted(original),
                keeper,
                _loan: loan,
            },
        }
    }
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
    publication: Publication,
    receiver: ProtocolOriginalReceiver,
}
enum Publication {
    Phase(std::sync::Weak<crate::original_protocol::Phase>),
    Accepted {
        original: crate::controlled_listener::accepted::AcceptedTcpCarrierOrigin,
        proof: crate::controlled_carrier::AcceptedLoanReceiver,
    },
}
impl ProtocolReceiverPrepared {
    pub(crate) fn new(
        phase: &Arc<crate::original_protocol::Phase>,
        receiver: ProtocolOriginalReceiver,
    ) -> Self {
        Self {
            publication: Publication::Phase(Arc::downgrade(phase)),
            receiver,
        }
    }
    pub fn confirm(self) -> Result<ProtocolOriginalReceiver, ChannelError> {
        let phase = match self.publication {
            Publication::Phase(phase) => phase,
            Publication::Accepted { original, proof } => {
                original.install_receiver(proof)?;
                return Ok(self.receiver);
            }
        };
        let phase = phase.upgrade().ok_or_else(|| {
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
                _loan: loan.clone(),
            },
            protocol: keeper.driver().clone(),
        };
        if let Original::Accepted(original) = &self.original {
            let proof = loan.accepted_receiver(original, keeper).ok_or_else(fail)?;
            return Ok(ProtocolReceiverPrepared {
                publication: Publication::Accepted {
                    original: original.clone(),
                    proof,
                },
                receiver,
            });
        }
        let phase = self
            .retirement
            .as_ref()
            .and_then(CarrierProtocolRetirement::original_phase)
            .ok_or_else(fail)?;
        Ok(ProtocolReceiverPrepared::new(&phase, receiver))
    }
}

impl CarrierAdoption {
    /// One actual cached TCP input has exactly this installed native driver.
    /// Unlike a borrowed datagram endpoint, it has no additional Quinn graph.
    pub fn prepare_stream_protocol_receiver(
        &self,
        tasks: &crate::contract::scope::TaskRegistrar,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<ProtocolReceiverPrepared, ChannelError> {
        let fail = || {
            ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::StalePlan,
                crate::contract::Impact::Component,
            )
        };
        let owner = self.slot.owner.upgrade().ok_or_else(fail)?;
        let bound = { owner.state().binding.clone() }.ok_or_else(fail)?;
        let keeper = self.retained_driver().ok_or_else(fail)?;
        if !bound.permits_stream_protocol_control(tasks)
            || !keeper.retains(self)
            || !self.slot.root_bound
            || !keeper
                .driver()
                .scope_identity()
                .same_scope(&tasks.scope_identity())
        {
            return Err(fail());
        }
        let phase = {
            let state = self.slot.state();
            if state.closed || state.unknown.is_some() {
                return Err(fail());
            }
            state.protocol.clone()
        }
        .ok_or_else(fail)?;
        if !retirement.matches_phase(&phase) {
            return Err(fail());
        }
        let receiver = ProtocolOriginalReceiver::carrier(keeper.clone(), keeper.driver().clone());
        Ok(ProtocolReceiverPrepared::new(&phase, receiver))
    }
}
