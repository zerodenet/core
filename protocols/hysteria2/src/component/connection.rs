//! Authenticated connection continuations expose only standard request roles.
use super::*;
use std::io;
use tokio::io::AsyncWriteExt;
use zero_platform_tokio::{
    contract::{
        context::{AcceptContext, HandshakeContext, InboundConnectionFacts},
        stream::BoxStream,
        BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream,
};
use zero_transport::quic::owned::{OwnedBiStream, OwnedConnection, OwnedIncoming};
pub(super) fn error(source: impl Into<String>) -> ChannelError {
    failure(io::Error::other(source.into()))
}
pub(super) fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Handshake, Reason::Protocol, Impact::Session).with_source(source)
}
pub(super) struct Incoming {
    pub incoming: OwnedIncoming,
    pub profile: Hysteria2AuthenticatedInboundProfile,
    pub resources: Arc<dyn ResourceAccess>,
}
impl InboundHandshake for Incoming {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let local = self.incoming.local_addr();
        let connection = self.incoming.establish().await?;
        let (sni, alpn) = connection.handshake_metadata();
        let peer = connection.remote_address();
        let facts = InboundConnectionFacts {
            original_source: Some(peer),
            effective_source: Some(peer),
            local_endpoint: Some(local),
            sni,
        };
        let cancel = context.cancel.child_token();
        // Install the connection guard before awaiting either authentication path.
        let mut source = Source {
            connection,
            streams: None,
            auth: None,
            facts,
            cancel,
            udp_delivered: false,
            pending_udp: None,
        };
        let caller = context
            .accepted
            .as_ref()
            .ok_or_else(|| error("missing carrier caller"))?;
        let watched = source.connection.try_clone()?;
        let stop = source.cancel.clone();
        caller.spawn(
            zero_platform_tokio::contract::lifecycle::TaskClass::Auxiliary,
            move |_| async move {
                tokio::select! {biased;_=stop.cancelled()=>{},_=watched.closed()=>{}}
                stop.cancel();
                Ok(())
            },
        )?;
        let auth = if alpn.as_deref() == Some("h3") {
            let (auth, streams) = web::accept(
                source.connection.try_clone()?,
                self.profile,
                caller,
                self.resources,
                source.cancel.clone(),
            )
            .await?;
            source.streams = Some(streams);
            auth
        } else {
            let (connection, send, recv) = source.connection.accept_bi_with_connection().await?;
            let mut stream = OwnedBiStream::from_streams(connection, send, recv);
            let mut salt = [0; 32];
            source
                .connection
                .export_keying_material(&mut salt, b"hysteria2 auth", &[])?;
            let mut frame = [0; 35];
            tokio::io::AsyncReadExt::read_exact(&mut stream, &mut frame)
                .await
                .map_err(failure)?;
            let auth = crate::shared::parse_auth_frame(&frame)
                .and_then(|hmac| self.profile.protocol.authenticate_hmac(&salt, &hmac));
            match auth {
                Ok(auth) => {
                    stream
                        .write_all(&crate::shared::build_auth_ok())
                        .await
                        .map_err(failure)?;
                    auth
                }
                Err(err) => {
                    let _ = stream
                        .write_all(&crate::shared::build_auth_error("authentication failed"))
                        .await;
                    return Err(failure(err));
                }
            }
        };
        source.auth = Some(auth);
        Ok(AcceptedRequest::Multiplex(Box::new(source)))
    }
}
struct Source {
    connection: OwnedConnection,
    streams: Option<tokio::sync::mpsc::Receiver<TcpRelayStream>>,
    auth: Option<zero_core::SessionAuth>,
    facts: InboundConnectionFacts,
    cancel: CancellationToken,
    udp_delivered: bool,
    pending_udp: Option<bytes::Bytes>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.connection.close(0x100u32.into(), b"");
    }
}
impl RequestMultiplexer for Source {
    fn auth(&self) -> Option<&zero_core::SessionAuth> {
        self.auth.as_ref()
    }
    fn max_in_flight(&self) -> NonZeroUsize {
        n(256)
    }
    fn cancellation(&self) -> Option<CancellationToken> {
        Some(self.cancel.clone())
    }
    fn close(&mut self, _: &str) {
        self.cancel.cancel();
        self.connection.close(0x100u32.into(), b"");
    }
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>> {
        Box::pin(async move {
            let tcp = async {
                match &mut self.streams {
                    Some(streams) => Ok(streams.recv().await),
                    None => {
                        let (connection, send, recv) =
                            self.connection.accept_bi_with_connection().await?;
                        Ok::<_, ChannelError>(Some(TcpRelayStream::new(
                            OwnedBiStream::from_streams(connection, send, recv),
                        )))
                    }
                }
            };
            let datagram = async {
                if self.pending_udp.is_none() {
                    self.pending_udp = Some(self.connection.read_datagram().await?);
                }
                // A cancelled select keeps the same first packet while waiting
                // for its real connection view; it is never silently consumed.
                let connection = self.connection.try_clone_wait().await?;
                Ok::<_, ChannelError>((connection, self.pending_udp.take().unwrap()))
            };
            tokio::select! {biased;
                _=self.cancel.cancelled()=>Ok(None),
                _=context.cancel.cancelled()=>Ok(None),
            _=async {match context.deadline {Some(deadline)=>tokio::time::sleep_until(deadline).await,None=>std::future::pending().await}}=>Err(ChannelError::new(Stage::Receive,Reason::Timeout,Impact::Session)),
                result=tcp=>{let Some(stream)=result? else{return Ok(None)};Ok(Some(PendingInbound::new(Tcp {stream,auth:self.auth.clone().unwrap(),facts:self.facts.clone()})))},
                first=datagram,if !self.udp_delivered=>{let (connection,first)=first?;self.udp_delivered=true;Ok(Some(PendingInbound::new(datagram::Incoming {connection,first,auth:self.auth.clone().unwrap(),facts:self.facts.clone()})))},
            }
        })
    }
}
struct Tcp {
    stream: TcpRelayStream,
    auth: zero_core::SessionAuth,
    facts: InboundConnectionFacts,
}
impl InboundHandshake for Tcp {
    async fn complete(
        mut self,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        let (address, port) = crate::shared::read_tcp_connect_header(&mut self.stream)
            .await
            .map_err(failure)?;
        let facts = self.facts.request(
            &context,
            zero_core::ProtocolType::new("hysteria2"),
            Some(Destination { address, port }),
            Some(self.auth),
        )?;
        Ok(AcceptedRequest::Stream(accept_stream(Reply {
            stream: self.stream,
            facts,
        })))
    }
}
struct Reply {
    stream: TcpRelayStream,
    facts: zero_core::request::RequestFacts,
}
impl StreamReply for Reply {
    type Stream = BoxStream;
    fn facts(&self) -> &zero_core::request::RequestFacts {
        &self.facts
    }
    async fn respond(mut self, outcome: ReplyOutcome) -> Result<Option<BoxStream>, ChannelError> {
        let bytes = match outcome {
            ReplyOutcome::LocalChannelReady => crate::shared::build_connect_ok(),
            ReplyOutcome::Blocked => crate::shared::build_connect_error("blocked"),
            _ => crate::shared::build_connect_error("outbound failed"),
        };
        self.stream.write_all(&bytes).await.map_err(failure)?;
        Ok(matches!(outcome, ReplyOutcome::LocalChannelReady)
            .then(|| Box::pin(self.stream) as BoxStream))
    }
}
