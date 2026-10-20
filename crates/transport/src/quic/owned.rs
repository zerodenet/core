//! Opt-in native Quinn ownership libraries. Existing QUIC constructors keep
//! their legacy paths. No host resource grant or production activation is
//! inferred from these library types.
mod callbacks;
mod drivers;
mod socket;
pub use drivers::{
    DriverBody, DriverDelivery, DriverExitReceiver, DriverRegistrationFailure,
    DriverRegistrationReason, DriverTaskStatus, QuinnDriverOwner,
};
pub use socket::{MetadataSocketOwner, NativeSendDrops, RejectedMetadataSocket};

#[cfg(test)]
#[path = "../../tests/quic/owned/mod.rs"]
mod tests;
