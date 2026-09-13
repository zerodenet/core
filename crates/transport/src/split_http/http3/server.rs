use super::super::{
    body::Body,
    request::Profile,
    server::{exchange, status, XhttpIncoming},
    SplitHttpRegistry,
};
use bytes::Buf;
use http_body_util::BodyExt;
use std::{io, time::Duration};
use tokio::sync::mpsc;
use zero_traits::SplitHttpTransportProfile;

pub fn accept_xhttp_h3_connection<P: SplitHttpTransportProfile + ?Sized>(
    connection: quinn::Connection,
    config: &P,
    registry: &SplitHttpRegistry,
) -> XhttpIncoming {
    let profile = Profile::new(config);
    let registry = registry.clone();
    let (accepted, receiver) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        let mut builder = h3::server::builder();
        builder.max_field_section_size(profile.options.server_max_header_bytes as u64);
        let Ok(mut server) = builder
            .build(h3_quinn::Connection::new(connection.clone()))
            .await
        else {
            return;
        };
        let mut tasks = tokio::task::JoinSet::new();
        let mut accepting = true;
        loop {
            if !accepting && tasks.is_empty() {
                break;
            }
            tokio::select! {
                request = server.accept(), if accepting && tasks.len() < 64 => {
                    let resolver = match request {
                        Ok(Some(resolver)) => resolver,
                        Ok(None) => { accepting = false; continue; },
                        Err(error) => { tracing::debug!(%error, "XHTTP H3 request accept failed"); break; },
                    };
                    let profile = profile.clone(); let registry = registry.clone(); let accepted = accepted.clone();
                    tasks.spawn(async move {
                        let (request, stream) = tokio::time::timeout(Duration::from_secs(30), resolver.resolve_request()).await.map_err(io::Error::other)?.map_err(io::Error::other)?;
                        let (mut send, mut recv) = stream.split();
                        let (sender, receiver) = mpsc::channel(8);
                        let upload = tokio::spawn(async move {
                            let result = async {
                                while let Some(mut data) = recv.recv_data().await.map_err(io::Error::other)? {
                                    let bytes = data.copy_to_bytes(data.remaining());
                                    sender.send(Ok(bytes)).await.map_err(io::Error::other)?;
                                }
                                Ok::<_,io::Error>(())
                            }.await;
                            if let Err(error) = result { let _ = sender.send(Err(error)).await; }

                        });
                        let request = request.map(|_| Body { receiver, task:Some(upload.abort_handle()), life:None, progress:None });
                        let response = match registry.requests.clone().try_acquire_owned() {
                            Ok(permit) => exchange::handle(request,profile,registry.sessions,accepted,permit).await.unwrap_or_else(|error| { tracing::debug!(%error, "XHTTP H3 request rejected"); status(400) }),
                            Err(_) => status(503),
                        };
                        let (parts,mut body) = response.into_parts();
                        send.send_response(http::Response::from_parts(parts,())).await.map_err(io::Error::other)?;
                        while let Some(frame) = body.frame().await {
                            if let Ok(data) = frame?.into_data() { send.send_data(data).await.map_err(io::Error::other)?; }
                        }
                        send.finish().await.map_err(io::Error::other)
                    });
                }
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Ok(Err(error))) = completed { tracing::debug!(%error, "XHTTP H3 exchange failed"); }
                }
                reason = connection.closed() => { tracing::debug!(%reason, "XHTTP H3 connection closed"); break; },
            }
        }
        connection.close(0x100u32.into(), b"");
    });
    XhttpIncoming {
        receiver: tokio::sync::Mutex::new(receiver),
        task: Some(task.abort_handle()),
    }
}
