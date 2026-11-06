use super::*;
use std::num::NonZeroUsize;
use std::time::Duration;
use zero_core::{component::ComponentInstanceId, request::Destination};
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::pooled_driver::PoolOptions;
impl TrojanOutboundLeaf {
    pub fn native_mux_eligible(&self) -> bool {
        self.mux_pool.native_eligible()
    }

    pub(crate) fn bind_native_mux(
        &self,
        expected: ComponentInstanceId,
    ) -> Result<Self, RuntimeError> {
        let mut current = self.clone();
        current.mux_pool = current.mux_pool.bind_current(expected)?;
        Ok(current)
    }

    pub(crate) fn native_mux_options(
        &self,
        binding: UpstreamBinding,
        limit: NonZeroUsize,
        budget: Duration,
    ) -> Result<PoolOptions, RuntimeError> {
        let (key, concurrency) = self.protocol.native_mux_plan(&self.server, self.port)?;
        let ip = zero_core::address::parse_ip_literal(&self.server).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native MUX currently requires a literal controlled endpoint",
            )
        })?;
        let address = match ip {
            std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
        };
        self.mux_pool.native_options(
            &key,
            concurrency,
            Destination::new(address, self.port),
            binding,
            limit,
            budget,
        )
    }
    pub(crate) fn native_mux_instance(&self) -> Result<ComponentInstanceId, RuntimeError> {
        self.mux_pool.native_instance()
    }
    pub(crate) async fn open_native_mux(
        &self,
        destination: Destination,
        binding: UpstreamBinding,
    ) -> Result<zero_transport::pooled_driver::PooledStream, RuntimeError> {
        let (key, concurrency) = self.protocol.native_mux_plan(&self.server, self.port)?;
        self.mux_pool
            .open_native(
                key,
                concurrency,
                destination.address,
                destination.port,
                binding,
                |raw| async move {
                    let io = zero_transport::TcpRelayStream::new(
                        zero_transport::pooled_driver::ExclusiveIo::new(raw),
                    );
                    self.transport
                        .open_owned_relay_with_profile(io, self.protocol.owned_tls_profile())
                        .await
                },
            )
            .await
    }
    pub(crate) async fn open_native_mux_udp(
        &self,
        binding: UpstreamBinding,
    ) -> Result<
        zero_transport::pooled_driver::datagram::PooledDatagram<crate::mux::native::DatagramIo>,
        RuntimeError,
    > {
        let (key, concurrency) = self.protocol.native_mux_plan(&self.server, self.port)?;
        self.mux_pool
            .open_native_udp(key, concurrency, binding, |raw| async move {
                let io = zero_transport::TcpRelayStream::new(
                    zero_transport::pooled_driver::ExclusiveIo::new(raw),
                );
                self.transport
                    .open_owned_relay_with_profile(io, self.protocol.owned_tls_profile())
                    .await
            })
            .await
    }
}
