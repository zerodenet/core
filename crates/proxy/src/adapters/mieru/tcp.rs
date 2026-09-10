use crate::adapters::mieru::MieruAdapter;
use crate::protocol_registry::{claim_socket_tcp_leaf_with_relay, ClaimedTcpOutboundLeaf};
use crate::runtime::tcp_dispatch::operation::{
    LazyTcpRelayCarrier, PreparedTcpRelayOperation, SocketTcpHandshake,
};

#[derive(Clone)]
struct PreparedMieruTcpRelay {
    leaf: ::mieru::transport::MieruTransportLeaf,
}

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
}

impl MieruAdapter {
    pub(super) fn claim_tcp_outbound_leaf_impl<'a>(
        &self,
        leaf: ::mieru::transport::MieruTransportLeaf,
    ) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a> {
        let relay_leaf = leaf.clone();
        claim_socket_tcp_leaf_with_relay(leaf, move || {
            Box::new(PreparedMieruTcpRelay {
                leaf: relay_leaf.clone(),
            })
        })
    }
}

impl PreparedTcpRelayOperation for PreparedMieruTcpRelay {
    fn execute<'a>(
        self: Box<Self>,
        stream: crate::transport::TcpRelayStream,
        session: &'a zero_core::Session,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::transport::TcpRelayStream, zero_engine::EngineError>,
                > + Send
                + 'a,
        >,
    >
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.leaf
                .open_tcp_relay_hop(stream, session)
                .await
                .map_err(Into::into)
        })
    }

    fn execute_lazy<'a>(
        self: Box<Self>,
        carrier: LazyTcpRelayCarrier<'a>,
        session: &'a zero_core::Session,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::transport::TcpRelayStream, zero_engine::EngineError>,
                > + Send
                + 'a,
        >,
    >
    where
        Self: 'a,
    {
        Box::pin(async move {
            let identity = carrier.identity().to_owned();
            let generation = carrier.generation();
            let mut carrier = Some(carrier);
            self.leaf
                .open_tcp_relay_hop_lazy(session, generation, &identity, move || {
                    let carrier = carrier.take();
                    async move {
                        let carrier = carrier.ok_or_else(|| {
                            zero_transport::RuntimeError::Io(std::io::Error::other(
                                "mieru relay carrier factory reused",
                            ))
                        })?;
                        carrier.open().await.map_err(|error| {
                            zero_transport::RuntimeError::Io(std::io::Error::other(
                                error.to_string(),
                            ))
                        })
                    }
                })
                .await
                .map_err(Into::into)
        })
    }
}
