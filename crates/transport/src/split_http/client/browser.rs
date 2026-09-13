use super::{stream_pair, Body, Profile, XhttpMode, XhttpStream};
use crate::browser_dialer::BrowserDialer;
use http_body_util::BodyExt;
use std::{io, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::SplitHttpTransportProfile;

/// Open the Xray Browser Dialer subset: one streaming GET for downlink and
/// ordered packet requests for uplink. The browser does not implement
/// `stream-up` or `stream-one`, so those modes fail before any task is sent.
pub async fn connect_split_http_with_browser<P: SplitHttpTransportProfile + ?Sized>(
    dialer: &BrowserDialer,
    config: &P,
    scheme: &str,
    authority: &str,
) -> Result<XhttpStream, crate::RuntimeError> {
    if !matches!(scheme, "http" | "https") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Browser Dialer XHTTP scheme must be http or https",
        )
        .into());
    }
    let profile = Profile::new(config);
    if !matches!(profile.mode, XhttpMode::Auto | XhttpMode::PacketUp) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Browser Dialer XHTTP supports packet-up only",
        )
        .into());
    }
    let authority = normalize_authority(authority)?;
    let (mut stream, network) = stream_pair();
    let life = stream.life.clone();
    let session = format!("{:032x}", rand::random::<u128>());
    let request = profile.request("GET", &session, None, Body::empty())?;
    let download_url = absolute_url(scheme, &authority, request.uri())?;
    let download = tokio::time::timeout(
        Duration::from_secs(30),
        dialer.dial_get(&download_url, request.headers()),
    )
    .await
    .map_err(io::Error::other)??;
    let (mut reader, mut writer) = tokio::io::split(network);
    let state = life.clone();
    stream.tasks.push(
        tokio::spawn(async move {
            let mut download = download;
            let transfer = tokio::io::copy(&mut download, &mut writer);
            tokio::select! {
                result = transfer => match result {
                    Ok(_) => { let _ = writer.shutdown().await; }
                    Err(error) => state.fail(error),
                },
                _ = state.cancelled() => {}
            }
        })
        .abort_handle(),
    );
    let state = life.clone();
    let dialer = dialer.clone();
    let scheme = scheme.to_owned();
    stream.tasks.push(
        tokio::spawn(async move {
            let result = upload_packets(
                &dialer,
                &profile,
                &scheme,
                &authority,
                &session,
                &mut reader,
                &state,
            )
            .await;
            if let Err(error) = result {
                state.fail(error);
            }
        })
        .abort_handle(),
    );
    Ok(stream)
}

async fn upload_packets(
    dialer: &BrowserDialer,
    profile: &Profile,
    scheme: &str,
    authority: &str,
    session: &str,
    reader: &mut tokio::io::ReadHalf<tokio::io::DuplexStream>,
    state: &Arc<super::super::io::Lifetime>,
) -> io::Result<()> {
    let mut buffer = vec![0; profile.options.sc_max_each_post_bytes.to.max(1) as usize];
    let mut next_post = tokio::time::Instant::now();
    let mut sequence = 0u64;
    loop {
        let transfer = async {
            let cap = crate::split_http::request::sample(profile.options.sc_max_each_post_bytes)
                .max(1) as usize;
            let cap = cap.min(buffer.len());
            let size = reader.read(&mut buffer[..cap]).await?;
            if size == 0 {
                return Ok::<bool, io::Error>(false);
            }
            tokio::time::sleep_until(next_post).await;
            let request = profile.packet_request(session, sequence, &buffer[..size])?;
            let (parts, body) = request.into_parts();
            let url = absolute_url(scheme, authority, &parts.uri)?;
            let payload = request_body(body).await?;
            // Never retry after handing bytes to the browser. A lost final ACK
            // is ambiguous and closes this logical stream.
            dialer
                .send_packet(&parts.method, &url, &parts.headers, &payload)
                .await?;
            state.commit(size);
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| io::Error::other("xhttp sequence exhausted"))?;
            next_post = tokio::time::Instant::now()
                + Duration::from_millis(crate::split_http::request::sample(
                    profile.options.sc_min_posts_interval_ms,
                ) as u64);
            Ok(true)
        };
        let more = tokio::select! {
            result = transfer => result?,
            _ = state.cancelled() => return Ok(()),
        };
        if !more {
            return Ok(());
        }
    }
}

async fn request_body(mut body: Body) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame?.into_data() {
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}

fn absolute_url(scheme: &str, authority: &str, uri: &http::Uri) -> io::Result<String> {
    let url = format!(
        "{scheme}://{authority}{}",
        uri.path_and_query()
            .map(|value| value.as_str())
            .unwrap_or("/")
    );
    crate::browser_dialer::task::validate_url(&url, &["http", "https"])?;
    Ok(url)
}

fn normalize_authority(authority: &str) -> io::Result<String> {
    let url = url::Url::parse(&format!("http://{authority}/")).map_err(io::Error::other)?;
    if url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Browser Dialer XHTTP authority",
        ));
    }
    Ok(url[url::Position::BeforeHost..url::Position::AfterPort].to_owned())
}
