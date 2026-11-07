use super::*;
use std::num::NonZeroUsize;
use std::time::Duration;
use zero_core::{component::ComponentInstanceId, request::Destination};
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::pooled_driver::PoolOptions;
impl VlessOutboundLeaf {
    pub(in crate::transport) async fn open_native_mux_carrier(
        &self,
        io: zero_transport::TcpRelayStream,
        preparation: Option<zero_platform_tokio::controlled_carrier::CarrierPreparation>,
    ) -> Result<zero_transport::TcpRelayStream, RuntimeError> {
        super::super::navigation::open_carrier(
            &self.transport,
            &self.mux_pool,
            self.navigation_reality.as_ref(),
            io,
            preparation,
        )
        .await
    }

    pub(crate) fn with_preparation_reality(
        mut self,
        reality: Option<crate::reality::PreparedRealityClient>,
    ) -> Self {
        self.navigation_reality = reality;
        self
    }

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
        let address = match zero_core::address::parse_ip_literal(&self.server) {
            Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
            None => zero_core::Address::Domain(self.server.clone()),
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
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<zero_transport::pooled_driver::PooledStream, RuntimeError> {
        let (key, concurrency) = self.protocol.native_mux_plan(&self.server, self.port)?;
        let binding = context.upstream.binding().clone();
        let call = Some((
            zero_platform_tokio::contract::network::CarrierContext {
                session: context.session_identity().map_err(std::io::Error::other)?,
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            },
            context.upstream.observed_pending_carrier(),
        ));
        let leaf = self.clone();
        self.mux_pool
            .open_native(
                key,
                concurrency,
                destination.address,
                destination.port,
                binding,
                call,
                move |raw| async move {
                    let preparation = raw.preparation();
                    let io = zero_transport::TcpRelayStream::new(
                        zero_transport::pooled_driver::ExclusiveIo::new(raw),
                    );
                    leaf.open_native_mux_carrier(io, preparation).await
                },
            )
            .await
    }
    pub(crate) async fn open_native_mux_udp(
        &self,
        context: &zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<
        zero_transport::pooled_driver::datagram::PooledDatagram<
            crate::mux_pool::native::DatagramIo,
        >,
        RuntimeError,
    > {
        let (key, concurrency) = self.protocol.native_mux_plan(&self.server, self.port)?;
        let binding = context.upstream.binding().clone();
        let call = Some((
            zero_platform_tokio::contract::network::CarrierContext {
                session: context.session_identity().map_err(std::io::Error::other)?,
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            },
            context.upstream.observed_pending_carrier(),
        ));
        let leaf = self.clone();
        self.mux_pool
            .open_native_udp(key, concurrency, binding, call, move |raw| async move {
                let preparation = raw.preparation();
                let io = zero_transport::TcpRelayStream::new(
                    zero_transport::pooled_driver::ExclusiveIo::new(raw),
                );
                leaf.open_native_mux_carrier(io, preparation).await
            })
            .await
    }
}
