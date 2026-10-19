use std::{error::Error, fmt};

pub use zero_core::channel::{
    CloseReason, DeliveryState, Impact, Reason, RetryClass, SendReceipt, Stage,
};
use zero_core::component::{ComponentId, SessionId};

/// Decision fields are structured; the source chain is diagnostic only.
/// Sources must already be redacted by their owning protocol.
#[derive(Debug)]
pub struct ChannelError {
    pub component: Option<ComponentId>,
    pub session: Option<SessionId>,
    pub stage: Stage,
    pub reason: Reason,
    pub impact: Impact,
    pub delivery: DeliveryState,
    pub retry: RetryClass,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl ChannelError {
    pub fn new(stage: Stage, reason: Reason, impact: Impact) -> Self {
        Self {
            component: None,
            session: None,
            stage,
            reason,
            impact,
            // Lack of a recorded submission is not proof that none happened.
            delivery: DeliveryState::Unknown,
            retry: RetryClass::Never,
            source: None,
        }
    }

    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn retry_permitted(&self) -> bool {
        self.retry.permits(self.delivery)
    }
}

impl fmt::Display for ChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?}: {:?} ({:?}, {:?})",
            self.stage, self.reason, self.impact, self.delivery
        )
    }
}

impl Error for ChannelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn Error + 'static))
    }
}

#[derive(Debug)]
pub struct SendFailure {
    pub error: ChannelError,
    pub delivery: DeliveryState,
}

impl SendFailure {
    pub fn new(mut error: ChannelError, delivery: DeliveryState) -> Self {
        error.delivery = delivery;
        Self { error, delivery }
    }
}

impl fmt::Display for SendFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}
impl Error for SendFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}
