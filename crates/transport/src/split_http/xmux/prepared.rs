//! Original HTTP connection futures before selecting a lifecycle executor.
//! H2 also delegates Task/Pipe/Send futures to the explicit executor, including
//! during handshake. Returning the outer future does not join those delegates.
use super::super::{body::Body, client::carrier};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{future::Future, io, pin::Pin, time::Duration};
use tokio::io::{AsyncRead, AsyncWrite};

pub(in crate::split_http) struct PreparedConnection {
    pub(in crate::split_http) sender: carrier::Sender,
    pub(in crate::split_http) driver: Pin<Box<dyn Future<Output = io::Result<()>> + Send>>,
}

pub(in crate::split_http) async fn http1<S>(stream: S) -> io::Result<PreparedConnection>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (sender, driver) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(io::Error::other)?;
    Ok(PreparedConnection {
        sender: carrier::Sender::Http1(sender),
        driver: Box::pin(async move { driver.await.map_err(io::Error::other) }),
    })
}

/// No default executor is installed here. A managed owner must supervise all
/// futures handed to E as well as the returned outer connection future, and
/// prepare inside its actual controlled-I/O adoption before polling handshake.
pub(in crate::split_http) async fn http2<S, E>(
    stream: S,
    settings: Option<&[u8]>,
    keepalive: i64,
    executor: E,
) -> io::Result<PreparedConnection>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    E: hyper::rt::bounds::Http2ClientConnExec<Body, TokioIo<S>> + Unpin + Send + Sync + 'static,
{
    let period = if keepalive == 0 {
        Some(Duration::from_secs(45))
    } else if keepalive > 0 {
        Some(Duration::from_secs(keepalive as u64))
    } else {
        None
    };
    let mut builder = hyper::client::conn::http2::Builder::new(executor);
    if let Some(settings) = settings {
        builder.peer_application_settings(settings);
    }
    let (sender, driver) = builder
        .timer(TokioTimer::new())
        .keep_alive_interval(period)
        .keep_alive_timeout(Duration::from_secs(15))
        .keep_alive_while_idle(true)
        .handshake::<_, Body>(TokioIo::new(stream))
        .await
        .map_err(io::Error::other)?;
    Ok(PreparedConnection {
        sender: carrier::Sender::Http2(sender),
        driver: Box::pin(async move { driver.await.map_err(io::Error::other) }),
    })
}

#[cfg(test)]
#[path = "../../../tests/xhttp_prepared.rs"]
mod tests;
