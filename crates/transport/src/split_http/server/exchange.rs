use super::super::{io::Lifetime, sessions::Sessions};
use super::*;
use tokio::sync::OwnedSemaphorePermit;
struct UploadGuard {
    life: Arc<Lifetime>,
    complete: bool,
}
impl UploadGuard {
    fn finish(&mut self, complete: bool) {
        self.complete = complete;
    }
}
impl Drop for UploadGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.life.fail("xhttp upload interrupted");
        }
    }
}

pub(in crate::split_http) async fn handle<B>(
    request: http::Request<B>,
    profile: Profile,
    sessions: Sessions,
    accepted: mpsc::Sender<XhttpStream>,
    permit: OwnedSemaphorePermit,
) -> io::Result<http::Response<Body>>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let host = request
        .uri()
        .authority()
        .map(|a| a.as_str())
        .or_else(|| request.headers().get("host").and_then(|v| v.to_str().ok()))
        .unwrap_or("");
    if profile.check_host && !crate::http_early_data::host_matches(host, &profile.host) {
        return Ok(status(404));
    }
    if !request.uri().path().starts_with(&profile.path) {
        return Ok(status(404));
    }
    let size: usize = request
        .headers()
        .iter()
        .map(|(k, v)| k.as_str().len() + v.len() + 4)
        .sum();
    if size > profile.options.server_max_header_bytes as usize {
        return Ok(status(431));
    }
    let headers = profile.response_headers(&request)?;
    let no_sse = profile.options.no_sse_header;
    let response = if request.method() == http::Method::OPTIONS {
        Ok(status(200))
    } else if let Err(error) = profile.validate_padding(&request) {
        tracing::debug!(%error, "XHTTP padding rejected");
        Ok(status(400))
    } else {
        dispatch(request, profile, sessions, accepted, permit).await
    };
    let mut response = response?;
    response.headers_mut().extend(headers);
    if no_sse {
        response.headers_mut().remove("content-type");
    }
    Ok(response)
}
async fn dispatch<B>(
    request: http::Request<B>,
    profile: Profile,
    sessions: Sessions,
    accepted: mpsc::Sender<XhttpStream>,
    permit: OwnedSemaphorePermit,
) -> io::Result<http::Response<Body>>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let (id, sequence) = profile.meta(&request)?;
    tracing::trace!(session = %id, ?sequence, method = %request.method(), "XHTTP request dispatch");
    let method = request.method().clone();
    if method == http::Method::OPTIONS {
        return Ok(status(200));
    }
    let stream_one = id.is_empty();
    let upload = method != http::Method::GET || sequence.is_some();
    let mode = if stream_one {
        XhttpMode::StreamOne
    } else if sequence.is_some() {
        XhttpMode::PacketUp
    } else {
        XhttpMode::StreamUp
    };
    let allowed = if stream_one {
        matches!(
            profile.mode,
            XhttpMode::Auto | XhttpMode::StreamOne | XhttpMode::StreamUp
        )
    } else {
        !upload || profile.mode == XhttpMode::Auto || profile.mode == mode
    };
    if !allowed {
        return Ok(status(400));
    }
    if upload && !stream_one {
        let session =
            sessions.get_with_limit(&id, profile.options.sc_max_buffered_posts as usize)?;
        if let Some(sequence) = sequence {
            return super::packet::upload(request, profile, session, sequence, permit).await;
        }
        session.claim_stream()?;
        let life = session.life.clone();
        let (sender, receiver) = mpsc::channel(1);
        let mut guard = UploadGuard {
            life: life.clone(),
            complete: false,
        };
        let heartbeat = request.headers().contains_key("referer");
        let interval = profile.options.sc_stream_up_server_secs;
        let padding = profile.options.x_padding_bytes;
        let task = tokio::spawn(async move {
            let _permit = permit;
            let keepalive = async {
                if !heartbeat {
                    std::future::pending::<()>().await;
                }
                loop {
                    sender
                        .send(Ok(Bytes::from(
                            "X".repeat(super::super::request::sample(padding) as usize),
                        )))
                        .await
                        .map_err(io::Error::other)?;
                    tokio::time::sleep(Duration::from_secs(
                        super::super::request::sample(interval) as u64,
                    ))
                    .await;
                }
                #[allow(unreachable_code)]
                Ok::<(), io::Error>(())
            };
            let result = tokio::select! {
                result = stream_upload(request.into_body(), &session) => result,
                result = keepalive => result,
                _ = life.cancelled() => Ok(()),
            };
            guard.finish(result.is_ok());
            if let Err(error) = result {
                life.fail(&error);
                let _ = sender.send(Err(error)).await;
            }
        });
        // The response owns upload cancellation but successful upload EOF does
        // not terminate the independent download (TCP half-close).
        return Ok(response(Body {
            receiver,
            task: Some(task.abort_handle()),
            life: None,
            progress: None,
        }));
    }
    let session = if stream_one {
        None
    } else {
        let session =
            sessions.get_with_limit(&id, profile.options.sc_max_buffered_posts as usize)?;
        session.claim_download()?;
        tracing::debug!(session = %id, state = ?Arc::as_ptr(&session.life), "XHTTP download attached");
        Some(session)
    };
    let life = session
        .as_ref()
        .map(|s| s.life.clone())
        .unwrap_or_else(|| Arc::new(Lifetime::default()));
    let mut admission = UploadGuard {
        life: life.clone(),
        complete: false,
    };
    let (app, net) = tokio::io::duplex(64 * 1024);
    let stream = XhttpStream {
        inner: app,
        life: life.clone(),
        tasks: Vec::new(),
        usages: Vec::new(),
        drain_on_drop: true,
    };
    accepted
        .try_send(stream)
        .map_err(|_| io::Error::other("xhttp accept capacity exhausted"))?;
    let (mut reader, mut writer) = tokio::io::split(net);
    let (sender, receiver) = mpsc::channel(8);
    let state = life.clone();
    let task = tokio::spawn(async move {
        let _permit = permit;
        let upload = async {
            match session {
                Some(session) => {
                    while let Some(bytes) = session.next().await? {
                        writer.write_all(&bytes.data).await?;
                    }
                }
                None => {
                    let mut body = request.into_body();
                    while let Some(frame) = body.frame().await {
                        if let Ok(bytes) = frame.map_err(io::Error::other)?.into_data() {
                            writer.write_all(&bytes).await?;
                        }
                    }
                }
            }
            writer.shutdown().await
        };
        let download = async {
            let mut buffer = vec![0; 16 * 1024];
            loop {
                let size = reader.read(&mut buffer).await?;
                if size == 0 {
                    return Ok::<_, io::Error>(());
                }
                sender
                    .send(Ok(Bytes::copy_from_slice(&buffer[..size])))
                    .await
                    .map_err(io::Error::other)?;
            }
        };
        // Upload EOF is a half-close. Keep download alive until the route ends.
        let transfer = async {
            tokio::pin!(download);
            tokio::select! {
                result = upload => { result?; download.await }
                result = &mut download => result,
            }
        };
        tokio::select! {
            result = transfer => { if let Err(error) = result { state.fail(&error); let _ = sender.send(Err(error)).await; } }
            _ = state.cancelled() => {}
        }
    });
    admission.complete = true;
    Ok(response(Body {
        receiver,
        task: Some(task.abort_handle()),
        life: Some(life),
        progress: None,
    }))
}
async fn stream_upload<B>(mut body: B, session: &Session) -> io::Result<()>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let mut sequence = 0u64;
    while let Some(frame) = body.frame().await {
        if let Ok(bytes) = frame.map_err(io::Error::other)?.into_data() {
            // Bound individual frames as well as total queue capacity.
            for chunk in bytes.chunks(64 * 1024) {
                session
                    .push_stream(sequence, Bytes::copy_from_slice(chunk))
                    .await?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("xhttp sequence exhausted"))?;
            }
        }
    }
    session.finish();
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/xhttp_server/authority.rs"]
mod tests;
