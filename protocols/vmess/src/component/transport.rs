//! The same native profile over the actual selected upstream original.
use super::*;

impl Outbound {
    pub(in crate::component) async fn wrap_transport(
        &self,
        io: BoxStream,
        context: &OpenContext,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<zero_transport::TcpRelayStream, ChannelError> {
        let call = GuardedCall::from_admitted(&self.admission, context, session);
        let prepared_tls = self
            .prepared_tls
            .lock()
            .expect("prepared VMess TLS state")
            .clone();
        if self.options.ca_file.is_some()
            && self.options.material.is_none()
            && prepared_tls.is_none()
        {
            return Err(call.error(Stage::Prepare, Reason::Revoked, DeliveryState::NotSubmitted));
        }
        let transport = async {
            let io = zero_transport::TcpRelayStream::new(carrier::Carrier::new(io));
            let stack = StreamTransportStack {
                tls: self.options.tls.as_ref(),
                ws: self.options.ws.as_ref(),
                grpc: self.options.grpc.as_ref(),
                h2: None::<&OwnedH2Profile>,
                http_upgrade: None::<&OwnedHttpUpgradeProfile>,
                source_dir: None,
            };
            let prepared_tls = if let Some(material) = &self.options.material {
                Some(material.prepare(self.ech_resolver.as_deref()).await?)
            } else {
                prepared_tls
            };
            if let Some(prepared) = prepared_tls {
                let io = prepared.connect(io).await?;
                connect_layered_transport_stack(
                    io,
                    stack,
                    &self.options.server,
                    self.options.endpoint.port,
                    "vmess: ws and grpc are mutually exclusive",
                )
                .await
            } else {
                connect_relay_transport_stack(
                    io,
                    stack,
                    &self.options.server,
                    self.options.endpoint.port,
                    "vmess: ws and grpc are mutually exclusive",
                )
                .await
            }
        };
        call.run(Stage::Handshake, DeliveryState::Unknown, transport)
            .await?
            .map_err(|source| {
                let mut error = call.error(Stage::Handshake, Reason::Io, DeliveryState::Unknown);
                if context.upstream.binding().prefix.is_empty() {
                    error.delivery = DeliveryState::NotSubmitted;
                    error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
                }
                error.with_source(source)
            })
    }
}
