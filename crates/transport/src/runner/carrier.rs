//! Stateless transport-stack wrapping over an already opened `BoxStream`.

use super::{ExclusiveIo, GuardedCall};
use crate::{
    outbound_stack::{connect_relay_transport_stack, StreamTransportStack},
    profile::{
        OwnedClientTlsProfile, OwnedGrpcProfile, OwnedH2Profile, OwnedHttpUpgradeProfile,
        OwnedWebSocketProfile,
    },
    RuntimeError, TcpRelayStream,
};
use zero_platform_tokio::contract::{
    stream::BoxStream, ChannelError, DeliveryState, Reason, RetryClass, Stage,
};

/// Borrowed transport-stack description for a taskless/stateless outbound.
#[derive(Clone, Copy)]
pub struct StatelessTransportStack<'a> {
    pub server: &'a str,
    pub port: u16,
    pub tls: Option<&'a OwnedClientTlsProfile>,
    pub ws: Option<&'a OwnedWebSocketProfile>,
    pub grpc: Option<&'a OwnedGrpcProfile>,
    pub h2: Option<&'a OwnedH2Profile>,
    pub http_upgrade: Option<&'a OwnedHttpUpgradeProfile>,
    pub invalid_message: &'static str,
}

impl<'a> StatelessTransportStack<'a> {
    pub fn new(server: &'a str, port: u16, invalid_message: &'static str) -> Self {
        Self {
            server,
            port,
            tls: None,
            ws: None,
            grpc: None,
            h2: None,
            http_upgrade: None,
            invalid_message,
        }
    }
}

/// Apply the declared transport layers over an opened `BoxStream` without
/// spawning background carrier tasks (using inline gRPC when `grpc` is set).
pub async fn connect_stateless_transport_stack(
    io: BoxStream,
    stack: &StatelessTransportStack<'_>,
) -> Result<TcpRelayStream, RuntimeError> {
    let raw = TcpRelayStream::new(ExclusiveIo::new(io));
    #[cfg(feature = "grpc")]
    if let Some(grpc) = stack.grpc {
        if stack.ws.is_some() || stack.h2.is_some() || stack.http_upgrade.is_some() {
            return Err(RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                stack.invalid_message,
            )));
        }
        #[cfg(feature = "tls")]
        let carrier = match stack.tls {
            Some(tls) => crate::tls::connect_tls_stream(raw, tls, None, stack.server).await?,
            None => raw,
        };
        #[cfg(not(feature = "tls"))]
        let carrier = {
            if stack.tls.is_some() {
                return Err(RuntimeError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    stack.invalid_message,
                )));
            }
            raw
        };
        let settings = carrier
            .application_settings()
            .filter(|settings| settings.protocol == b"h2")
            .map(|settings| settings.peer.clone());
        let authority = stack
            .tls
            .and_then(|tls| tls.server_name.as_deref())
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                stack
                    .server
                    .parse::<std::net::IpAddr>()
                    .map(|ip| std::net::SocketAddr::new(ip, stack.port).to_string())
                    .unwrap_or_else(|_| stack.server.to_owned())
            });
        let io = crate::grpc::connect_grpc_inline_with_settings(
            carrier,
            grpc,
            &authority,
            settings.as_deref(),
        )
        .await?;
        return Ok(TcpRelayStream::new(io));
    }

    connect_relay_transport_stack(
        raw,
        StreamTransportStack {
            tls: stack.tls,
            ws: stack.ws,
            grpc: stack.grpc,
            h2: stack.h2,
            http_upgrade: stack.http_upgrade,
            source_dir: None,
        },
        stack.server,
        stack.port,
        stack.invalid_message,
    )
    .await
}

/// Wrap an opened carrier through `stack` under `call`'s cancellation/deadline
/// guard, marking pre-prefix transport failures as `BeforeSubmission`.
pub async fn wrap_stateless_carrier(
    call: &GuardedCall<'_>,
    io: BoxStream,
    stack: &StatelessTransportStack<'_>,
) -> Result<TcpRelayStream, ChannelError> {
    call.run(
        Stage::Handshake,
        DeliveryState::Unknown,
        connect_stateless_transport_stack(io, stack),
    )
    .await?
    .map_err(|source| {
        let mut error = call.error(Stage::Handshake, Reason::Io, DeliveryState::Unknown);
        // No protocol request bytes were written yet. Only an empty prefix proves
        // that a preceding proxy hop has not already submitted its own CONNECT.
        if call.context().upstream.binding().prefix.is_empty() {
            error.delivery = DeliveryState::NotSubmitted;
            error.retry = RetryClass::BeforeSubmission;
        }
        error.with_source(source)
    })
}
