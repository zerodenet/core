use zero_core::{Address, Error, InboundUdpDispatch, SessionAuth};

pub(crate) struct PacketSessionUdpRelayRequest<'a, H> {
    pub(crate) handler: H,
    pub(crate) inbound_tag: &'a str,
    pub(crate) protocol: &'static str,
    pub(crate) auth: Option<SessionAuth>,
    pub(crate) failure_policy: PacketSessionUdpFailurePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    dead_code,
    reason = "test-only legacy relay contract retains the distinct error-policy cases"
)]
pub(crate) enum PacketSessionUdpFailurePolicy {
    ReturnError,
    #[cfg(feature = "managed-stream-runtime")]
    LogAndBreak,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    dead_code,
    reason = "test-only legacy relay contract retains the distinct read-failure cases"
)]
pub(crate) enum PacketSessionUdpReadFailureAction {
    #[cfg(feature = "managed-stream-runtime")]
    Continue,
    End,
}

pub(crate) struct PacketSessionUdpReadFailure {
    pub(crate) error: Error,
    pub(crate) action: PacketSessionUdpReadFailureAction,
}

#[expect(
    clippy::large_enum_variant,
    reason = "the actual dispatch stays inline in this test-only relay contract"
)]
pub(crate) enum PacketSessionUdpReadResult {
    Dispatch(InboundUdpDispatch),
    End,
}

pub(crate) trait PacketSessionUdpHandler {
    async fn read_inbound_dispatch(
        &mut self,
    ) -> Result<PacketSessionUdpReadResult, PacketSessionUdpReadFailure>;

    async fn write_response_for_target(
        &mut self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>;

    async fn finish(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
