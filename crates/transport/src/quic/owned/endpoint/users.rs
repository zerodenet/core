use super::super::drivers::Admission;
use zero_platform_tokio::{
    contract::{ChannelError, Impact, Reason, Stage},
    controlled_datagram::metadata::users::{LogicalUserPermit, LogicalUsers, UserAdmission},
};
pub(super) type Users = LogicalUsers<Admission>;
pub(super) type Permit = LogicalUserPermit<Admission>;
impl UserAdmission for Admission {
    fn check(&self) -> Result<(), ChannelError> {
        Admission::check(self)
    }
}
pub(super) fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}
pub(super) fn protocol(
    stage: Stage,
    source: impl std::error::Error + Send + Sync + 'static,
) -> ChannelError {
    error(stage, Reason::Protocol).with_source(source)
}
