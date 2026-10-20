//! Publish without retaining a parent lock; panic is an unknown handoff outcome.
use super::*;
use zero_platform_tokio::contract::{DeliveryState, Impact, RetryClass, Stage};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Publication {
    Pending,
    Publishing,
    Uncertain,
    Released,
}
struct Attempt<'a> {
    state: &'a Mutex<Publication>,
    complete: bool,
}
impl Attempt<'_> {
    fn finish(mut self, next: Publication) {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        self.complete = true;
    }
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if !self.complete {
            // The callback may already have retained/forwarded the original
            // source. Unwinding cannot prove non-delivery and enable replay.
            *self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Publication::Uncertain;
        }
    }
}
impl Replacement {
    pub(super) fn released(&self) -> bool {
        *self
            .released
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == Publication::Released
    }
    pub(super) fn release(
        &self,
        receipt: DeclaredCloseReceipt,
        publish: impl FnOnce(&DeclaredComponentTerminal) -> Result<(), ChannelError>,
    ) -> Result<Option<ChannelError>, ChannelError> {
        if receipt.0.instances().len() != self.order.len()
            || !self
                .terminal
                .get()
                .is_some_and(|terminal| Arc::ptr_eq(terminal, &receipt.0))
        {
            return Err(invalid(Reason::StalePlan, "retirement receipt is foreign"));
        }
        {
            let mut state = self
                .released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match *state {
                Publication::Pending => *state = Publication::Publishing,
                Publication::Publishing => {
                    let mut error =
                        ChannelError::new(Stage::Persist, Reason::Capacity, Impact::Component);
                    error.delivery = DeliveryState::NotSubmitted;
                    return Err(error);
                }
                Publication::Uncertain => {
                    let mut error =
                        ChannelError::new(Stage::Persist, Reason::Internal, Impact::Component);
                    error.delivery = DeliveryState::Unknown;
                    error.retry = RetryClass::Never;
                    return Err(match receipt.0.failure() {
                        Some(failure) => error.with_source(failure.clone()),
                        None => error,
                    });
                }
                Publication::Released => {
                    return Err(invalid(
                        Reason::StalePlan,
                        "retirement receipt was already consumed",
                    ));
                }
            }
        }
        let attempt = Attempt {
            state: &self.released,
            complete: false,
        };
        // The receiver's Err contract is rejection before accepting publication.
        // A panic has no such contract: keep the original owner, terminal and
        // uncertainty instead of publishing the same source a second time.
        if let Err(error) = publish(&receipt.0) {
            attempt.finish(Publication::Pending);
            return Err(error);
        }
        for failure in receipt.0.failures() {
            failure.confirm_reported();
        }
        attempt.finish(Publication::Released);
        Ok(receipt.0.failure().map(|failure| failure.retained()))
    }
}
