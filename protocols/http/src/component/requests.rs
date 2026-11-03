//! A real HTTP connection is a sequential request source. The continuation
//! returns the same stream after a fully framed response, never a second copy.
use super::connection::Connection;
use crate::{HttpConnectInbound, HttpInboundCommand};
use alloc::boxed::Box;
use tokio::sync::oneshot;
use zero_platform_tokio::{
    contract::{
        context::{with_deadline, AcceptContext, HandshakeContext, InboundConnectionFacts},
        inbound::{
            accept_message, accept_stream, AcceptedRequest, InboundHandshake, PendingInbound,
            RequestMultiplexer,
        },
        BoxFuture, ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream,
};

pub struct Requests {
    connection: InboundConnectionFacts,
    client: Option<Connection>,
    started: bool,
    returned: Option<oneshot::Receiver<Option<Connection>>>,
}
impl Requests {
    pub fn new(client: TcpRelayStream) -> Self {
        let connection = InboundConnectionFacts::from_stream(&client);
        Self {
            connection,
            client: Some(Connection::new(client)),
            started: false,
            returned: None,
        }
    }
    pub fn with_connection_facts(mut self, connection: InboundConnectionFacts) -> Self {
        self.connection = connection;
        self
    }
}
impl RequestMultiplexer for Requests {
    fn max_in_flight(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::MIN
    }

    fn end_disposition(&self) -> zero_platform_tokio::contract::inbound::RequestSourceEnd {
        zero_platform_tokio::contract::inbound::RequestSourceEnd::RequestsExhausted
    }
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>> {
        Box::pin(async move {
            if context.cancel.is_cancelled() {
                return Ok(None);
            }
            if context
                .deadline
                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            {
                return Err(wait_expired());
            }
            if self.started {
                if self.client.is_none() {
                    let Some(returned) = self.returned.as_mut() else {
                        return Ok(None);
                    };
                    self.client = tokio::select! { biased;
                        _ = context.cancel.cancelled() => return Ok(None),
                        result = with_deadline(context.deadline, returned) => result.map_err(|_| wait_expired())?.ok().flatten(),
                    };
                    self.returned = None;
                }
                let Some(client) = self.client.as_mut() else {
                    return Ok(None);
                };
                let started = tokio::select! { biased;
                    _ = context.cancel.cancelled() => return Ok(None),
                    started = with_deadline(context.deadline, client.request_started()) => started.map_err(|_| wait_expired())?.map_err(|source| ChannelError::new(Stage::Receive,Reason::Io,Impact::Session).with_source(source))?,
                };
                if !started {
                    self.client = None;
                    return Ok(None);
                }
            }
            self.started = true;
            let Some(client) = self.client.take() else {
                return Ok(None);
            };
            let (continuation, returned) = oneshot::channel();
            self.returned = Some(returned);
            Ok(Some(PendingInbound::new(Handshake {
                client,
                connection: self.connection.clone(),
                continuation,
            })))
        })
    }
}
struct Handshake {
    client: Connection,
    connection: InboundConnectionFacts,
    continuation: oneshot::Sender<Option<Connection>>,
}
impl InboundHandshake for Handshake {
    async fn complete(
        mut self,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        let command = match HttpConnectInbound
            .accept_next_command(&mut self.client)
            .await
        {
            Ok(Some(command)) => command,
            Ok(None) => {
                return Err(ChannelError::new(
                    Stage::Handshake,
                    Reason::Revoked,
                    Impact::Session,
                ))
            }
            Err(source) => {
                let _ = HttpConnectInbound
                    .send_accept_error_response(&mut self.client, &source)
                    .await;
                return Err(
                    ChannelError::new(Stage::Handshake, Reason::Protocol, Impact::Session)
                        .with_source(source),
                );
            }
        };
        match command {
            HttpInboundCommand::Connect(target) => {
                let facts = self.connection.request(
                    &context,
                    crate::HttpConnectInbound.protocol(),
                    Some(target),
                    None,
                )?;
                super::ConnectReply::new(self.client, facts)
                    .map(accept_stream)
                    .map(AcceptedRequest::Stream)
            }
            HttpInboundCommand::Forward(request) => {
                let facts = self.connection.request(
                    &context,
                    crate::HttpConnectInbound.protocol(),
                    Some(request.destination().clone()),
                    None,
                )?;
                super::message::ForwardReply::new(self.client, request, facts, self.continuation)
                    .map(accept_message)
                    .map(AcceptedRequest::Message)
            }
        }
    }
}

fn wait_expired() -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Timeout, Impact::Session)
}
