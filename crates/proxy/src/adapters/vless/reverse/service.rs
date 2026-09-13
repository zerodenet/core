use super::super::*;
use crate::protocol_registry::{InboundServiceCapability, UpstreamConnectServices};
use crate::runtime::inbound_service::{
    DialedMuxSource, MuxServiceOperation, PreparedInboundServiceOperation,
};

impl DialedMuxSource for ::vless::transport::VlessReverseBridge {
    type Stream = crate::transport::TcpRelayStream;
    type Server = ::vless::mux::VlessInboundMuxServer;
    async fn next_connection(
        &self,
        services: UpstreamConnectServices,
    ) -> Result<(Self::Stream, Self::Server), EngineError> {
        let sockets = services.outbound_datagram_socket_factory();
        ::vless::transport::VlessReverseBridge::next_connection(
            self,
            move |server, port| {
                let server = server.to_owned();
                let services = services.clone();
                async move { services.connect_upstream_owned(server, port).await }
            },
            sockets,
        )
        .await
        .map_err(Into::into)
    }
}
impl InboundServiceCapability for VlessAdapter {
    fn prepare_inbound_service(
        &self,
        outbound: &zero_config::OutboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Option<Box<dyn PreparedInboundServiceOperation>>, EngineError> {
        let OutboundProtocolConfig::Vless {
            reverse_tag: Some(tag),
            reverse_sniffing,
            server,
            port,
            ..
        } = &outbound.protocol
        else {
            return Ok(None);
        };
        let options =
            outbound_options(outbound.tag(), (server.as_str(), *port), &outbound.protocol)
                .expect("VLESS outbound projection");
        // Active workers own their carrier pools. A rejected reload must not
        // retire pools used by the currently running ingress source.
        let runtime = VlessTransportRuntime::default();
        let source = VlessOutboundLeaf::from_options_refs(source_dir, options, &runtime)?
            .into_reverse_bridge();
        let sniffing = reverse_sniffing
            .as_deref()
            .map(|config| {
                crate::runtime::sniff::SniffingPolicy::new(
                    config.enabled,
                    &config.destination_override,
                    &config.domains_excluded,
                    config.metadata_only,
                    config.route_only,
                )
            })
            .transpose()
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        Ok(Some(Box::new(MuxServiceOperation {
            source,
            tag: tag.clone(),
            protocol: "vless",
            sniffing,
        })))
    }
}
