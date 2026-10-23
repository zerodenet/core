//! Opt-in native Quinn ownership libraries. Existing QUIC constructors keep
//! their legacy paths. No host resource grant or production activation is
//! inferred from these library types.
mod callbacks;
mod drivers;
mod endpoint;
#[cfg(any(feature = "split_http", feature = "hysteria"))]
pub use endpoint::h3;
pub use endpoint::{
    MetadataUsersCompletion, OwnedBiStream, OwnedConnection, OwnedEndpointHandle,
    OwnedEndpointLimits, OwnedEndpointOwner, OwnedEndpointReport, OwnedEndpointUsersReport,
    OwnedNativeFailure, OwnedRecvStream, OwnedSendStream, RejectedClaimedEndpoint,
};
mod socket;
pub use drivers::{
    DriverBody, DriverDelivery, DriverExitReceiver, DriverRegistrationFailure,
    DriverRegistrationReason, DriverTaskStatus, OwnedDriverOrigin, QuinnDriverOwner,
};
pub use socket::{
    HostedMetadataSocket, MetadataSocketOwner, NativeSendDrops, RejectedHostedMetadataSocket,
    RejectedMetadataSocket,
};

#[cfg(test)]
#[path = "../../tests/quic/owned/mod.rs"]
mod tests;
