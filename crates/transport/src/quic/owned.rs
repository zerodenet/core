//! Opt-in native Quinn ownership libraries. Existing QUIC constructors keep
//! their legacy paths. No host resource grant or production activation is
//! inferred from these library types.
mod callbacks;
mod drivers;
mod endpoint;
pub use endpoint::{
    OwnedConnection, OwnedEndpointHandle, OwnedEndpointLimits, OwnedEndpointOwner,
    OwnedEndpointReport, OwnedNativeFailure, OwnedRecvStream, OwnedSendStream,
};
mod socket;
pub use drivers::{
    DriverBody, DriverDelivery, DriverExitReceiver, DriverRegistrationFailure,
    DriverRegistrationReason, DriverTaskStatus, QuinnDriverOwner,
};
pub use socket::{MetadataSocketOwner, NativeSendDrops, RejectedMetadataSocket};

#[cfg(test)]
#[path = "../../tests/quic/owned/mod.rs"]
mod tests;
