use std::io;
use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
mod record_boundary;
pub use record_boundary::TlsRecordBoundary;
pub(crate) mod openssl;
pub use openssl::TlsAcceptor;
use tokio_rustls::TlsConnector;
use zero_platform_tokio::TokioSocket;
use zero_traits::{ClientTlsProfile, ServerTlsProfile};

use crate::RuntimeError;
use zero_platform_tokio::TcpRelayStream;

mod authority;
mod certificates;
mod client_hello;
mod inbound_stream;

mod cache;
pub mod config;
pub mod ech;
mod fingerprint;
mod groups;
mod ocsp;
mod refresh;
mod server;
mod verifier;
pub use server::server as build_server_config;
mod peek;
pub use inbound_stream::InboundTlsStream;
pub use peek::peek_client_hello;
mod switchable;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InboundClientHello {
    pub sni: Option<String>,
    pub alpn: Vec<String>,
    pub consumed: Vec<u8>,
}

pub fn build_tls_acceptor<T>(tls: &T, base_dir: Option<&Path>) -> Result<TlsAcceptor, RuntimeError>
where
    T: ServerTlsProfile + ?Sized,
{
    if openssl::use_openssl_server(tls).map_err(RuntimeError::Io)? {
        Ok(TlsAcceptor::OpenSsl(
            openssl::OpenSslServerContext::build(tls, base_dir).map_err(RuntimeError::Io)?,
        ))
    } else {
        Ok(TlsAcceptor::Rustls(tokio_rustls::TlsAcceptor::from(
            Arc::new(server::server(tls, base_dir, false)?),
        )))
    }
}

pub async fn accept_tls_handshake<S>(
    acceptor: &TlsAcceptor,
    stream: S,
) -> io::Result<InboundTlsStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match acceptor {
        TlsAcceptor::Rustls(acceptor) => {
            let stream = record_boundary::accept_rustls_handshake(acceptor, stream).await?;
            Ok(InboundTlsStream::new_generic(stream))
        }
        TlsAcceptor::OpenSsl(context) => {
            let stream = openssl::OpenSslTlsStream::accept(context, stream).await?;
            Ok(InboundTlsStream::new_openssl(stream))
        }
    }
}

pub async fn accept_tls_inbound(
    stream: TokioSocket,
    acceptor: &TlsAcceptor,
) -> Result<InboundTlsStream<TokioSocket>, RuntimeError> {
    accept_tls_handshake(acceptor, stream)
        .await
        .map_err(RuntimeError::Io)
}

pub async fn connect_tls_upstream_with_profile<P>(
    socket: TokioSocket,
    tls: &P,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> Result<TcpRelayStream, RuntimeError>
where
    P: ClientTlsProfile + ?Sized,
{
    let server_name = tls.server_name().unwrap_or(default_server_name).to_owned();

    let server_name_str = server_name.clone();
    if openssl::use_openssl_client(tls).map_err(RuntimeError::Io)? {
        let stream = openssl::connect(socket.into_inner(), tls, base_dir, default_server_name)
            .await
            .map_err(RuntimeError::Io)?;
        let control = stream.control();
        return Ok(match control {
            Some(control) => TcpRelayStream::with_transport_bypass_control(stream, control),
            None => TcpRelayStream::new(stream),
        });
    }
    let config = config::client(tls, base_dir, false)?;

    let connector = TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from(server_name.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid tls server_name"))?
        .to_owned();

    let peer_addr = socket.peer_addr().ok();
    tracing::debug!(
        sni = %server_name_str,
        peer = ?peer_addr,
        insecure = tls.insecure(),
        alpn = ?tls.alpn(),
        "tls connecting"
    );

    let stream = connector
        .connect(server_name, TlsRecordBoundary::new(socket.into_inner()))
        .await
        .map_err(|e| {
            tracing::warn!(
                error = %e,
                sni = %server_name_str,
                peer = ?peer_addr,
                "tls handshake failed"
            );
            e
        })?;

    let stream = switchable::SwitchableTlsStream::client(stream);
    Ok(match stream.control() {
        Some(control) => TcpRelayStream::with_transport_bypass_control(stream, control),
        None => TcpRelayStream::new(stream),
    })
}

pub async fn connect_tls_upstream<T>(
    socket: TokioSocket,
    tls: &T,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> Result<TcpRelayStream, RuntimeError>
where
    T: ClientTlsProfile + ?Sized,
{
    connect_tls_upstream_with_profile(socket, tls, base_dir, default_server_name).await
}

pub async fn connect_tls_stream_with_profile<S, P>(
    stream: S,
    tls: &P,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> Result<TcpRelayStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    P: ClientTlsProfile + ?Sized,
{
    let server_name = tls.server_name().unwrap_or(default_server_name).to_owned();

    if openssl::use_openssl_client(tls).map_err(RuntimeError::Io)? {
        let stream = openssl::connect(stream, tls, base_dir, default_server_name)
            .await
            .map_err(RuntimeError::Io)?;
        let control = stream.control();
        return Ok(match control {
            Some(control) => TcpRelayStream::with_transport_bypass_control(stream, control),
            None => TcpRelayStream::new(stream),
        });
    }
    let config = config::client(tls, base_dir, false)?;

    let connector = TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from(server_name.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid tls server_name"))?
        .to_owned();

    let stream = connector
        .connect(server_name, TlsRecordBoundary::new(stream))
        .await?;

    let stream = switchable::SwitchableTlsStream::client(stream);
    Ok(match stream.control() {
        Some(control) => TcpRelayStream::with_transport_bypass_control(stream, control),
        None => TcpRelayStream::new(stream),
    })
}

pub async fn connect_tls_stream<S, T>(
    stream: S,
    tls: &T,
    base_dir: Option<&Path>,
    default_server_name: &str,
) -> Result<TcpRelayStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    T: ClientTlsProfile + ?Sized,
{
    connect_tls_stream_with_profile(stream, tls, base_dir, default_server_name).await
}
