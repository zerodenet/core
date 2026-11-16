//! Both graphs originate from the actual native claim or adopted carrier loan.
use super::*;
use zero_platform_tokio::controlled_datagram::metadata::users::{
    drivers::Admission, LogicalUserPermit,
};
pub(super) enum Graph {
    Native(MetadataUserGraph<OwnedDriverOrigin, DriverBody>),
    Borrowed(CarrierEndpointUserGraph<OwnedDriverOrigin, DriverBody>),
}
// Admission is the same original task book for native and adopted graphs.
pub(super) struct User {
    _permit: LogicalUserPermit<Admission<OwnedDriverOrigin, DriverBody>>,
}
impl Graph {
    pub fn drivers(&self) -> &DriverOwner<OwnedDriverOrigin, DriverBody> {
        match self {
            Self::Native(g) => g.drivers(),
            Self::Borrowed(g) => g.drivers(),
        }
    }
    pub fn acquire(&self) -> Result<User, ChannelError> {
        match self {
            Self::Native(g) => g.users().acquire().map(|permit| User { _permit: permit }),
            Self::Borrowed(g) => g.users().acquire().map(|permit| User { _permit: permit }),
        }
    }
    pub fn revoke(&self) {
        match self {
            Self::Native(g) => g.users().cancellation().cancel(),
            Self::Borrowed(g) => g.users().cancellation().cancel(),
        }
        self.drivers().revoke_registration();
    }
    pub async fn wait_empty(&self) -> Result<(), ChannelError> {
        match self {
            Self::Native(g) => g.users().wait_empty_original().await,
            Self::Borrowed(g) => g.users().wait_empty_original().await,
        }
    }
}
