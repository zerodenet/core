//! Original per-connection TLS/WS/gRPC over the same controlled root view.
use super::*;
impl Outbound {
    pub(in crate::component) async fn open_transport(
        &self,
        context: &OpenContext,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<zero_transport::TcpRelayStream, ChannelError> {
        let carrier = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let opened = context
            .upstream
            .open_stream(self.options.endpoint.clone(), carrier);
        let io = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session,Stage::Dial,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session,Stage::Dial,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,opened) => result.map_err(|source| self.admission.error(session,Stage::Dial,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|error| self.admission.attach(error,session))?;
        let transport = async {
            if let Some(grpc) = &self.options.grpc {
                let io = zero_transport::tls::connect_tls_stream(
                    zero_transport::TcpRelayStream::new(carrier::Carrier::new(io)),
                    &self.options.tls,
                    None,
                    &self.options.server,
                )
                .await?;
                let settings = io
                    .application_settings()
                    .filter(|settings| settings.protocol == b"h2")
                    .map(|settings| settings.peer.clone());
                let authority = self
                    .options
                    .tls
                    .server_name
                    .as_deref()
                    .filter(|name| !name.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        self.options
                            .server
                            .parse::<std::net::IpAddr>()
                            .map(|ip| {
                                std::net::SocketAddr::new(ip, self.options.endpoint.port)
                                    .to_string()
                            })
                            .unwrap_or_else(|_| self.options.server.clone())
                    });
                let io = zero_transport::grpc::connect_grpc_inline_with_settings(
                    io,
                    grpc,
                    &authority,
                    settings.as_deref(),
                )
                .await?;
                Ok(zero_transport::TcpRelayStream::new(io))
            } else {
                connect_relay_transport_stack(
                    zero_transport::TcpRelayStream::new(carrier::Carrier::new(io)),
                    StreamTransportStack {
                        tls: Some(&self.options.tls),
                        ws: self.options.ws.as_ref(),
                        grpc: self.options.grpc.as_ref(),
                        h2: None::<&OwnedH2Profile>,
                        http_upgrade: None::<&OwnedHttpUpgradeProfile>,
                        source_dir: None,
                    },
                    &self.options.server,
                    self.options.endpoint.port,
                    "trojan: ws and grpc are mutually exclusive",
                )
                .await
            }
        };
        let io = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,transport) => result.map_err(|source| self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| {
            let mut error = self.admission.error(session, Stage::Handshake, Reason::Io, DeliveryState::Unknown);
            // No Trojan request was written. Only an empty prefix proves that
            // another proxy has not already submitted its own CONNECT.
            if context.upstream.binding().prefix.is_empty() {
                error.delivery = DeliveryState::NotSubmitted;
                error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
            }
            error.with_source(source)
        })?;
        Ok(io)
    }
}
