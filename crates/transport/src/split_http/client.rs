//! Official path/session/sequence uploads: HTTP/1.1 packets and HTTP/2 streams.
mod browser;
pub(super) mod carrier;
use super::{
    body::{Body, ReceivedBody},
    io::{Lifetime, XhttpStream},
    request::Profile,
    XhttpMode,
};
pub use browser::connect_split_http_with_browser;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper_util::rt::TokioIo;
use std::{io, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zero_traits::SplitHttpTransportProfile;

pub async fn connect_split_http<S, P>(
    post: S,
    get: S,
    config: &P,
) -> Result<XhttpStream, crate::RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    P: SplitHttpTransportProfile + ?Sized,
{
    let profile = Profile::new(config);
    let (mut stream, network) = stream_pair();
    let http2 = profile.mode == XhttpMode::StreamUp;
    let upload = carrier::open(post, http2, &mut stream).await?;
    let download = carrier::open(get, http2, &mut stream).await?;
    connect_channels(upload, download, stream, network, profile).await
}
pub(super) fn stream_pair() -> (XhttpStream, tokio::io::DuplexStream) {
    let (application, network) = tokio::io::duplex(64 * 1024);
    (
        XhttpStream {
            inner: application,
            life: Arc::new(Lifetime::default()),
            tasks: Vec::new(),
            usages: Vec::new(),
            drain_on_drop: false,
        },
        network,
    )
}
pub(super) async fn connect_channels(
    upload: carrier::Sender,
    download: carrier::Sender,
    stream: XhttpStream,
    network: tokio::io::DuplexStream,
    profile: Profile,
) -> Result<XhttpStream, crate::RuntimeError> {
    connect_profiles(upload, download, stream, network, profile.clone(), profile).await
}
pub(super) async fn connect_profiles(
    mut upload: carrier::Sender,
    mut download: carrier::Sender,
    mut stream: XhttpStream,
    network: tokio::io::DuplexStream,
    profile: Profile,
    download_profile: Profile,
) -> Result<XhttpStream, crate::RuntimeError> {
    let life = stream.life.clone();
    let session = format!("{:032x}", rand::random::<u128>());
    let request = download_profile.request("GET", &session, None, Body::empty())?;
    let response = tokio::time::timeout(Duration::from_secs(30), download.send_request(request))
        .await
        .map_err(io::Error::other)?
        .map_err(io::Error::other)?;
    if response.status() != 200 {
        return Err(
            io::Error::other(format!("xhttp download status {}", response.status())).into(),
        );
    }
    let (mut reader, mut writer) = tokio::io::split(network);
    let state = life.clone();
    stream.tasks.push(
        tokio::spawn(async move {
            let transfer = async {
                let mut body = response.into_body();
                while let Some(frame) = body.frame().await {
                    if let Ok(data) = frame.map_err(io::Error::other)?.into_data() {
                        writer.write_all(&data).await?;
                    }
                }
                writer.shutdown().await
            };
            tokio::select! {
                result = transfer => { if let Err(error) = result { state.fail(error); } }
                _ = state.cancelled() => {}
            }
        })
        .abort_handle(),
    );
    let state = life.clone();
    stream.tasks.push(
        tokio::spawn(async move {
            let transfer = async {
                if profile.mode == XhttpMode::StreamUp {
                    let (sender, receiver) = tokio::sync::mpsc::channel(8);
                    let producer = tokio::spawn(async move {
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
                    });
                    let body = Body {
                        receiver,
                        task: Some(producer.abort_handle()),
                        life: None,
                        progress: Some(state.clone()),
                    };
                    let response = upload
                        .send_request(profile.request(
                            &profile.options.uplink_http_method,
                            &session,
                            None,
                            body,
                        )?)
                        .await
                        .map_err(io::Error::other)?;
                    check_response(response).await?;
                } else {
                    let mut buffer = vec![0; profile.options.sc_max_each_post_bytes.to as usize];
                    let mut next_post = tokio::time::Instant::now();
                    let mut sequence = 0u64;
                    loop {
                        let cap = super::request::sample(profile.options.sc_max_each_post_bytes)
                            .max(1) as usize;
                        let size = reader.read(&mut buffer[..cap]).await?;
                        if size == 0 {
                            break;
                        }
                        tokio::time::sleep_until(next_post).await;
                        let request =
                            profile.packet_request(&session, sequence, &buffer[..size])?;
                        next_post = tokio::time::Instant::now()
                            + Duration::from_millis(super::request::sample(
                                profile.options.sc_min_posts_interval_ms,
                            ) as u64);
                        // Never retry a POST containing business bytes. A failed acknowledgement
                        // closes the logical stream instead of ambiguously replaying it.
                        let response = tokio::time::timeout(
                            Duration::from_secs(30),
                            upload.send_request(request),
                        )
                        .await
                        .map_err(io::Error::other)?
                        .map_err(io::Error::other)?;
                        tokio::time::timeout(Duration::from_secs(30), check_response(response))
                            .await
                            .map_err(io::Error::other)??;
                        state.commit(size);
                        sequence = sequence
                            .checked_add(1)
                            .ok_or_else(|| io::Error::other("xhttp sequence exhausted"))?;
                    }
                }
                Ok::<_, io::Error>(())
            };
            tokio::select! {
                result = transfer => { if let Err(error) = result { state.fail(error); } }
                _ = state.cancelled() => {}
            }
        })
        .abort_handle(),
    );
    Ok(stream)
}
async fn check_response(response: http::Response<ReceivedBody>) -> io::Result<()> {
    if response.status() != 200 {
        return Err(io::Error::other(format!(
            "xhttp upload status {}",
            response.status()
        )));
    }
    let mut body = response.into_body();
    while let Some(frame) = body.frame().await {
        frame.map_err(io::Error::other)?;
    }
    Ok(())
}
