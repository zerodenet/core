//! Pure error classification and delivery facts shared by all data planes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Validate,
    Prepare,
    Dial,
    Handshake,
    Send,
    Receive,
    Close,
    Persist,
    Recover,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    Unsupported,
    InvalidConfig,
    Capacity,
    Timeout,
    Cancelled,
    Revoked,
    Io,
    Protocol,
    StalePlan,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Impact {
    Datagram,
    Packet,
    Session,
    Component,
    Host,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryState {
    NotSubmitted,
    LocallyAccepted,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Never,
    /// Only with an unexpired deadline and an explicitly allowed bounded policy.
    BeforeSubmission,
    /// A fresh plan is required in addition to proof of no submission.
    Revalidate,
}

impl RetryClass {
    pub const fn permits(self, delivery: DeliveryState) -> bool {
        matches!(delivery, DeliveryState::NotSubmitted) && !matches!(self, Self::Never)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    Completed,
    Cancelled,
    Revoked,
    Replaced,
    Shutdown,
    ProtocolFailure,
    DeliveryUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendReceipt {
    pub accepted_bytes: usize,
    pub delivery: DeliveryState,
}

impl SendReceipt {
    /// A successful datagram send accepts the entire payload, including zero bytes.
    pub const fn accepted(bytes: usize) -> Self {
        Self {
            accepted_bytes: bytes,
            delivery: DeliveryState::LocallyAccepted,
        }
    }
}
