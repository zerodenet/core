//! HTTP/3 carrier for the same XHTTP request/session contract as H1 and H2.
pub(super) mod client;
mod scoped;
mod server;
pub use client::connect_xhttp_h3;
pub use scoped::accept as accept_xhttp_h3_connection_scoped;
pub use server::accept_xhttp_h3_connection;

use super::body::Body;
use bytes::Bytes;
use http_body_util::BodyExt;
use std::{future::Future, io};

fn peer_request(id: h3::quic::StreamId) -> quinn::StreamId {
    quinn::StreamId::new(quinn::Side::Client, quinn::Dir::Bi, id.index())
}

async fn send_response<S: h3::quic::SendStream<Bytes>>(
    mut send: h3::server::RequestStream<S, Bytes>,
    response: http::Response<Body>,
    stopped: impl Future<Output = Result<Option<quinn::VarInt>, quinn::StoppedError>>,
) -> io::Result<()> {
    let (parts, mut body) = response.into_parts();
    let reply = async {
        send.send_response(http::Response::from_parts(parts, ()))
            .await
            .map_err(io::Error::other)?;
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame?.into_data() {
                send.send_data(data).await.map_err(io::Error::other)?;
            }
        }
        send.finish().await.map_err(io::Error::other)
    };
    let peer = async {
        if stopped.await.map_err(io::Error::other)?.is_none() {
            std::future::pending::<()>().await;
        }
        Ok(())
    };
    // The request's original send side can be stopped even when no response
    // DATA is available. Returning here drops its body in the same request.
    tokio::select! { biased; result = reply => result, result = peer => result }
}
