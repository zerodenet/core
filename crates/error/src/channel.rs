use std::{error::Error, fmt, sync::Arc};

mod cleanup_receipt;
pub use cleanup_receipt::CleanupFailureReceipt;

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
    source: Option<Source>,
    cleanup_failure_receipt: Option<CleanupFailureReceipt>,
}

/// An owned diagnostic may need its original Box. Forwarding an existing
/// exact error Arc does not allocate another Box on every observation/retry.
/// Both variants preserve the same diagnostic source shape as before.
enum Source {
    Owned(Box<dyn Error + Send + Sync>),
    Retained(Arc<ChannelError>),
}
impl Source {
    fn as_error(&self) -> &(dyn Error + 'static) {
        match self {
            Self::Owned(source) => source.as_ref(),
            Self::Retained(source) => source,
        }
    }
}
impl fmt::Debug for Source {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owned(source) => fmt::Debug::fmt(source, formatter),
            Self::Retained(source) => fmt::Debug::fmt(source, formatter),
        }
    }
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
            cleanup_failure_receipt: None,
        }
    }

    pub fn with_source(mut self, source: impl Error + Send + Sync + 'static) -> Self {
        self.source = Some(Source::Owned(Box::new(source)));
        self
    }

    /// Explicit local error-delivery metadata. Diagnostic source traversal is
    /// never an acceptance channel and grants no cleanup or retry permission.
    pub fn with_cleanup_failure_receipt(mut self, receipt: CleanupFailureReceipt) -> Self {
        self.cleanup_failure_receipt = Some(receipt);
        self
    }

    pub fn cleanup_failure_receipt(&self) -> Option<&CleanupFailureReceipt> {
        self.cleanup_failure_receipt.as_ref()
    }

    /// Preserve the same actual cause and all structured decision/delivery
    /// fields. The original source Arc remains diagnostic and strongly owned.
    pub fn retained(source: Arc<Self>) -> Self {
        let mut error = Self::new(source.stage, source.reason, source.impact);
        error.component = source.component;
        error.session = source.session;
        error.delivery = source.delivery;
        error.retry = source.retry;
        error.cleanup_failure_receipt = source.cleanup_failure_receipt.clone();
        error.source = Some(Source::Retained(source));
        error
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
        self.source.as_ref().map(Source::as_error)
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
