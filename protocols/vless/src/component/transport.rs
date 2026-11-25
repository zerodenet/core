//! The original transport plan over the exact already-owned carrier.
use super::*;
use zero_transport::runner::GuardedCall;

impl Outbound {
    pub(in crate::component) async fn open_transport(
        &self,
        context: &OpenContext,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<zero_platform_tokio::TcpRelayStream, ChannelError> {
        let call = GuardedCall::from_admitted(&self.admission, context, session);
        let carrier = call.carrier_context();
        let io = call.open_stream(self.options.endpoint.clone()).await?;
        let transport = async {
            let io = zero_transport::TcpRelayStream::new(carrier::Carrier::new(io));
            let io = if self.datagram_budget.is_some() {
                zero_transport::finalmask::tcp::wrap_hosted(io, self.masks.tcp(), false).await?
            } else {
                zero_transport::finalmask::tcp::wrap_prepared(io, self.masks.tcp(), false).await?
            };
            let io = if let Some(reality) = &self.options.reality {
                let stream = reality
                    .connect_with_navigation(io, |stream, host, plan| {
                        navigation::start_owned(
                            stream,
                            host,
                            plan,
                            self.tasks.clone(),
                            self.admission.clone(),
                            context.upstream.clone(),
                            carrier.clone(),
                        )
                    })
                    .await
                    .map_err(zero_transport::RuntimeError::Io)?;
                let control = stream.transport_bypass_control();
                Ok(zero_transport::TcpRelayStream::with_transport_bypass_control(stream, control))
            } else {
                let stack = StreamTransportStack {
                    tls: self.options.tls.as_ref(),
                    ws: self.options.ws.as_ref(),
                    grpc: self.options.grpc.as_ref(),
                    h2: self.options.h2.as_ref(),
                    http_upgrade: self.options.http_upgrade.as_ref(),
                    source_dir: None,
                };
                if let Some(material) = &self.options.material {
                    let prepared = material
                        .prepare(self.ech_resolver.as_deref())
                        .await
                        .map_err(zero_transport::RuntimeError::Io)?;
                    let io = prepared.connect(io).await?;
                    connect_layered_transport_stack(
                        io,
                        stack,
                        &self.options.server,
                        self.options.endpoint.port,
                        "VLESS stream carriers are mutually exclusive",
                    )
                    .await
                } else {
                    connect_relay_transport_stack(
                        io,
                        stack,
                        &self.options.server,
                        self.options.endpoint.port,
                        "VLESS stream carriers are mutually exclusive",
                    )
                    .await
                }
            }?;
            match &self.options.encryption {
                Some(encryption) => encryption
                    .handshake(io)
                    .await
                    .map(|stream| stream.into_relay())
                    .map_err(zero_transport::RuntimeError::Io),
                None => Ok(io),
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
