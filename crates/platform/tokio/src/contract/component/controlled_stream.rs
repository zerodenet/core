//! One neutral classification for declared physical stream demand. This enum
//! never supplies a business admission receipt or permits erasing Some to None.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlledStreamDemandClass {
    /// Requires the actual host's sealed current business admission per demand.
    Business,
    /// Infrastructure work; never invent a business SessionId.
    Service,
    /// Explicit background control work, with no fabricated business session.
    BackgroundControl,
}
