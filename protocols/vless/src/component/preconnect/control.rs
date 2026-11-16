//! Existing registry entry; all native state is reached through its weak generation.
use zero_platform_tokio::contract::network::UpstreamBinding;
impl crate::transport::PreconnectPool {
    /// The existing upper MUX factory borrows the unframed warmed transport,
    /// not this protocol's business Stream (which would submit VLESS twice).
    pub async fn open_carrier(
        &self,
        expected: zero_core::component::ComponentInstanceId,
        binding: &UpstreamBinding,
        call: zero_platform_tokio::contract::network::CarrierContext,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
    ) -> Result<
        zero_platform_tokio::controlled_carrier::loan::StreamLoan,
        zero_platform_tokio::contract::ChannelError,
    > {
        let opened = self
            .native(expected)
            .map_err(|source| super::native::error(expected, source))?
            .take_observed(binding, call, waiting)
            .await
            .map_err(|source| super::native::error(expected, source))?;
        let bypass = zero_traits::AsyncSocket::transport_bypass_control(&opened.io);
        Ok(
            zero_platform_tokio::controlled_carrier::loan::StreamLoan::new(
                Box::pin(opened.io),
                opened.control,
                opened.original,
            )
            .with_transport_bypass_control(bypass),
        )
    }
    pub(in crate::component) fn matches(
        &self,
        expected: zero_core::component::ComponentInstanceId,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.native(expected)
            .map(|_| ())
            .map_err(|source| super::native::error(expected, source))
    }
    pub(in crate::component) async fn open(
        &self,
        expected: zero_core::component::ComponentInstanceId,
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<super::Opened, zero_platform_tokio::contract::ChannelError> {
        self.native(expected)
            .map_err(|source| super::native::error(expected, source))?
            .take(context)
            .await
            .map_err(|source| super::native::error(expected, source))
    }
}
