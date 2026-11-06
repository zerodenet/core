//! The leaf keeps the exact existing fully-keyed runtime pool.
use super::*;
impl VlessOutboundLeaf {
    /// Library-only recipe over this leaf's original, fully keyed direct pool.
    #[cfg(feature = "reality")]
    pub(crate) fn native_grpc_options(
        &self,
        runtime: &VlessTransportRuntime,
        dial: zero_traits::DialPolicy,
        carriers: std::num::NonZeroUsize,
        streams: std::num::NonZeroUsize,
    ) -> Result<zero_transport::grpc::NativePoolOptions, RuntimeError> {
        self.transport
            .native_grpc_options(runtime, dial, carriers, streams)
    }

    #[cfg(feature = "reality")]
    pub(crate) fn background_grpc_options(
        &self,
        runtime: &VlessTransportRuntime,
        dial: zero_traits::DialPolicy,
        carriers: std::num::NonZeroUsize,
        streams: std::num::NonZeroUsize,
    ) -> Result<zero_transport::grpc::NativePoolOptions, RuntimeError> {
        self.transport
            .background_grpc_options(runtime, dial, carriers, streams)
    }
}
