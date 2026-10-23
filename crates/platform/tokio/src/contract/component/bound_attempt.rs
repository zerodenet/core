//! Structured pre-I/O outcome; diagnostic error sources never grant a retry.
use super::{BoundDatagramRequest, DatagramBindingIssuer};
use crate::{
    contract::ChannelError,
    controlled_datagram::{ControlledDatagramOwner, DatagramAcquisitionError},
};

#[derive(Debug)]
pub struct BoundDatagramAttemptFailure {
    error: ChannelError,
    retry: Option<BoundDatagramRetryReceipt>,
}
/// One failed acquisition of this exact binding/peer/generation. This does not
/// authorize another open: the host caller still supplies a finite attempt
/// budget and rechecks the actual current binding before each new attempt.
#[derive(Debug)]
pub struct BoundDatagramRetryReceipt {
    request: BoundDatagramRequest,
}
impl BoundDatagramAttemptFailure {
    pub fn denied(error: ChannelError) -> Self {
        Self { error, retry: None }
    }
    pub fn error(&self) -> &ChannelError {
        &self.error
    }
    pub fn into_error(self) -> ChannelError {
        self.error
    }
    pub fn into_parts(self) -> (ChannelError, Option<BoundDatagramRetryReceipt>) {
        (self.error, self.retry)
    }
}
impl BoundDatagramRetryReceipt {
    pub fn matches(&self, request: &BoundDatagramRequest) -> bool {
        self.request.binding == request.binding
            && self.request.peer == request.peer
            && self.request.expected_network_generation == request.expected_network_generation
    }
}
impl DatagramBindingIssuer {
    /// Only the actual provider's private issuer can tie native evidence to its
    /// binding. The provider calls this directly after its sole owner's failed
    /// acquire, before any I/O view exists or any datagram was submitted.
    pub fn acquisition_failure(
        &self,
        request: &BoundDatagramRequest,
        error: ChannelError,
        acquisition: DatagramAcquisitionError,
        owner: &ControlledDatagramOwner,
    ) -> BoundDatagramAttemptFailure {
        let settled = owner.confirms_settled_acquisition(&acquisition);
        BoundDatagramAttemptFailure {
            error: error.with_source(acquisition),
            retry: (self.owns(&request.binding) && settled).then(|| BoundDatagramRetryReceipt {
                request: request.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests;
