use super::*;
use crate::controlled_datagram::ControlledDatagramClosed;
/// Same original root/query proof of settled pre-payload acquisition only.
pub struct RootDatagramRetryReceipt {
    request: RootDatagramRequest,
    query: TaskJoinWitness,
}
pub struct RootDatagramAttemptFailure {
    error: ChannelError,
    retry: Option<RootDatagramRetryReceipt>,
}
pub struct RootDatagramClosed {
    usage: RootDatagramUse,
}
impl RootDatagramRetryReceipt {
    pub(super) fn new(request: RootDatagramRequest, query: TaskJoinWitness) -> Self {
        Self { request, query }
    }
    pub fn permits(&self, next: &RootDatagramGrant, query: &TaskJoinWitness) -> bool {
        self.request.authority.matches_request(&next.request)
            && self.query.same_registration(query)
            && !query.is_joined()
    }
}
impl std::fmt::Debug for RootDatagramAttemptFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootDatagramAttemptFailure")
            .field("error", &self.error)
            .field("settled", &self.retry.is_some())
            .finish()
    }
}
impl RootDatagramAttemptFailure {
    pub fn denied(error: ChannelError) -> Self {
        Self { error, retry: None }
    }
    pub(super) fn acquired(error: ChannelError, retry: Option<RootDatagramRetryReceipt>) -> Self {
        Self { error, retry }
    }
    pub fn error(&self) -> &ChannelError {
        &self.error
    }
    pub fn into_error(self) -> ChannelError {
        self.error
    }
    pub fn retry_receipt(&self) -> Option<&RootDatagramRetryReceipt> {
        self.retry.as_ref()
    }
}
impl RootDatagramClosed {
    pub fn matches(&self, usage: &RootDatagramUse) -> bool {
        self.usage.same_use(usage)
    }
}
impl RootDatagramAuthority {
    /// Only the same persistent control's actual OS receipt can confirm native
    /// closure. Equal numeric reservations, view Drop or TaskJoined cannot.
    pub fn confirm_closed(
        &self,
        usage: &RootDatagramUse,
        control: &ControlledDatagramControl,
        closed: &ControlledDatagramClosed,
    ) -> Option<RootDatagramClosed> {
        (self.matches_request(usage.request())
            && usage.view_returned()
            && usage.matches_control(control)
            && closed.matches(control))
        .then(|| RootDatagramClosed {
            usage: usage.clone(),
        })
    }
}
