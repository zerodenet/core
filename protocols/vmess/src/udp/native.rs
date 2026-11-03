//! Compatibility packet framing over the actual host-owned MUX generation.
use super::*;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::RuntimeError;
impl PreparedVmessUdpFlowPlan {
    pub(crate) async fn open_native_mux_flow<S, F, Fut>(
        &self,
        session: &Session,
        server: &str,
        port: u16,
        pool: &crate::mux::VmessMuxConnectionPool,
        binding: UpstreamBinding,
        wrap: F,
    ) -> Result<VmessUdpFlowConnection, RuntimeError>
    where
        S: AsyncSocket
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Unpin
            + Send
            + Sync
            + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut,
        Fut: Future<Output = Result<S, RuntimeError>>,
    {
        let concurrency = self
            .plan
            .mux_concurrency()
            .ok_or(Error::Config("native UDP requires MUX"))?;
        let key = self.plan.resume().udp_mux_pool_key_from_transport_config(
            server,
            port,
            self.mux_transport_profile.as_borrowed(),
            self.plan.mux_idle_timeout_secs,
            self.plan.mux_response_backlog,
        )?;
        let stream = pool
            .open_native_udp_stream(
                key,
                concurrency,
                session.target.clone(),
                session.port,
                binding,
                wrap,
            )
            .await?;
        Ok(start_udp_flow(stream))
    }
}
