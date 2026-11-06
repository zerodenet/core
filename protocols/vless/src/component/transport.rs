//! The original transport plan over the exact already-owned carrier.
use super::*;
impl Outbound {
    pub(in crate::component) async fn open_transport(
        &self,
        context: &OpenContext,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<zero_platform_tokio::TcpRelayStream, ChannelError> {
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
            let io = zero_transport::TcpRelayStream::new(carrier::Carrier::new(io));
            if let Some(reality) = &self.options.reality {
                let stream = reality
                    .connect_with_navigation(io, |stream, host, plan| {
                        navigation::start(
                            stream,
                            host,
                            plan,
                            self.tasks.clone(),
                            self.admission.clone(),
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
            }
        };
        let io = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,transport) => result.map_err(|source| self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| {
            let mut error = self.admission.error(session, Stage::Handshake, Reason::Io, DeliveryState::Unknown);
            // No VLESS request was written. Only an empty prefix proves that
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
