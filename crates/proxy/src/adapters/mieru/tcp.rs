use crate::runtime::tcp_dispatch::operation::{LazyTcpRelayCarrier, SocketTcpHandshake};

#[async_trait::async_trait]
impl SocketTcpHandshake for ::mieru::transport::MieruTransportLeaf {
    fn tag(&self) -> &str {
        self.tag()
    }

    fn server(&self) -> &str {
        self.server()
    }

    fn port(&self) -> u16 {
        self.port()
    }

    fn connect_stage(&self) -> &'static str {
        "connect_upstream_mieru"
    }

    async fn open_tcp_stream(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        session: &zero_core::Session,
    ) -> Result<
        (
            crate::transport::TcpRelayStream,
            zero_transport::StreamTraffic,
        ),
        zero_transport::RuntimeError,
    > {
        let factory = services.outbound_datagram_socket_factory();
        let stream = ::mieru::transport::MieruTransportLeaf::open_tcp_stream(
            self,
            session,
            move |server, port| {
                let services = services.clone();
                let server = server.to_owned();
                async move { services.connect_upstream_owned(server, port).await }
            },
            factory,
        )
        .await?;
        Ok((stream, zero_transport::StreamTraffic::default()))
    }

    async fn open_tcp_relay_hop(
        &self,
        stream: crate::transport::TcpRelayStream,
        session: &zero_core::Session,
    ) -> Result<crate::transport::TcpRelayStream, zero_transport::RuntimeError> {
        ::mieru::transport::MieruTransportLeaf::open_tcp_relay_hop(self, stream, session).await
    }
    async fn open_tcp_relay_carrier(
        &self,
        carrier: LazyTcpRelayCarrier<'_>,
        session: &zero_core::Session,
    ) -> Result<crate::transport::TcpRelayStream, zero_engine::EngineError> {
        let identity = carrier.identity().to_owned();
        let generation = carrier.generation();
        let mut carrier = Some(carrier);
        self.open_tcp_relay_hop_lazy(session, generation, &identity, move || {
            let carrier = carrier.take();
            async move {
                let carrier = carrier.ok_or_else(|| {
                    zero_transport::RuntimeError::Io(std::io::Error::other(
                        "mieru relay carrier factory reused",
                    ))
                })?;
                carrier
                    .open()
                    .await
                    .map_err(|error| zero_transport::RuntimeError::Io(std::io::Error::other(error)))
            }
        })
        .await
        .map_err(Into::into)
    }
}

#[cfg(test)]
#[path = "tcp/tests.rs"]
mod tests;
