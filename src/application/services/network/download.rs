//! Bootstrap rule HTTP uses the same actual TCP owner and task scope as delivery.
//! Each exchange is retired before a redirect, stale-cache fallback or startup.
use super::*;
use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use zero_platform_tokio::contract::{scope::TaskJoinWitness, stream::BoxStream};

pub(crate) enum DownloadEvent {
    Data(Bytes),
    Redirect(url::Url),
}
pub(crate) struct RuleSetDownload {
    epoch: Arc<Epoch>,
    events: tokio::sync::mpsc::Receiver<io::Result<DownloadEvent>>,
    request: TaskJoinWitness,
}
impl RuleSetDownload {
    pub(crate) async fn next(&mut self) -> io::Result<Option<DownloadEvent>> {
        let result = tokio::select! {
            biased;
            event = self.events.recv() => event,
            _ = self.epoch.drive() => self.events.recv().await,
        };
        if let Some(result) = result {
            return result.map(Some);
        }
        let joined = tokio::select! {
            joined = self.request.wait() => joined,
            _ = self.epoch.drive() => self.request.wait().await,
        };
        joined.result().map_err(io::Error::other)?;
        Ok(None)
    }
}
impl ApplicationEventSinkTcpDialer {
    pub(crate) fn download(&self, url: url::Url) -> io::Result<RuleSetDownload> {
        self.download_using(url, None)
    }
    pub(super) fn download_using(
        &self,
        url: url::Url,
        tls: Option<Arc<rustls::ClientConfig>>,
    ) -> io::Result<RuleSetDownload> {
        let epoch = self.current()?;
        let (tx, events) = tokio::sync::mpsc::channel(4);
        let owner = epoch.clone();
        let egress = self.egress.clone();
        let request = epoch.spawn(Box::pin(async move {
            if let Err(error) = exchange(&owner, &egress, url, tls, &tx).await {
                let _ = tx.send(Err(error)).await;
            }
        }))?;
        Ok(RuleSetDownload {
            epoch,
            events,
            request,
        })
    }
}

#[cfg(test)]
mod tests;

async fn exchange(
    epoch: &Arc<Epoch>,
    egress: &EgressInterfaceControl,
    mut url: url::Url,
    tls: Option<Arc<rustls::ClientConfig>>,
    tx: &tokio::sync::mpsc::Sender<io::Result<DownloadEvent>>,
) -> io::Result<()> {
    let secure = match url.scheme() {
        "https" => true,
        "http" => false,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rule URL requires HTTP or HTTPS",
            ))
        }
    };
    let host = url
        .host_str()
        .ok_or_else(|| io::Error::other("rule URL host missing"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned();
    let port = url
        .port_or_known_default()
        .ok_or_else(|| io::Error::other("rule URL port missing"))?;
    let authentication =
        if !url.username().is_empty() || url.password().is_some_and(|value| !value.is_empty()) {
            Some(format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(format!(
                    "{}:{}",
                    url.username(),
                    url.password().unwrap_or("")
                ))
            ))
        } else {
            None
        };
    let redirect_base = url.clone();
    url.set_username("")
        .map_err(|()| io::Error::other("invalid URL user"))?;
    url.set_password(None)
        .map_err(|()| io::Error::other("invalid URL password"))?;
    url.set_fragment(None);
    let mut stream = None;
    let mut failure = None;
    for candidate in epoch.resolve(host.clone(), port).await? {
        match epoch
            .connect(candidate, egress, Instant::now() + Duration::from_secs(30))
            .await
        {
            Ok(connected) => {
                stream = Some(connected);
                break;
            }
            Err(error) => {
                // The failed original was installed before connect. Its actual
                // view/join/native receipts gate the next address candidate.
                epoch.reap().await?;
                failure = Some(error);
            }
        }
    }
    let stream = stream.ok_or_else(|| {
        failure.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "rule URL resolved no addresses")
        })
    })?;
    let stream: BoxStream = if secure {
        let config = match tls {
            Some(config) => config,
            None => Arc::new(
                rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .map_err(io::Error::other)?
                .with_root_certificates(rustls::RootCertStore::from_iter(
                    webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
                ))
                .with_no_client_auth(),
            ),
        };
        let name = rustls::pki_types::ServerName::try_from(host).map_err(io::Error::other)?;
        Box::pin(
            tokio_rustls::TlsConnector::from(config)
                .connect(name, stream)
                .await?,
        )
    } else {
        Box::pin(stream)
    };
    let (mut sender, driver) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .map_err(io::Error::other)?;
    let driver_result = tx.clone();
    epoch.spawn(Box::pin(async move {
        if let Err(source) = driver.await {
            let _ = driver_result.send(Err(io::Error::other(source))).await;
        }
    }))?;
    let uri: http::Uri = url.as_str().parse().map_err(io::Error::other)?;
    let authority = uri
        .authority()
        .ok_or_else(|| io::Error::other("rule URL authority missing"))?
        .as_str();
    let mut request = http::Request::builder()
        .method("GET")
        .uri(
            uri.path_and_query()
                .map(|value| value.as_str())
                .unwrap_or("/"),
        )
        .header(http::header::HOST, authority)
        .header(http::header::CONNECTION, "close")
        .header(http::header::USER_AGENT, "ureq/2.12.1");
    if let Some(authentication) = authentication {
        request = request.header(http::header::AUTHORIZATION, authentication);
    }
    let mut response = sender
        .send_request(
            request
                .body(Empty::<Bytes>::new())
                .map_err(io::Error::other)?,
        )
        .await
        .map_err(io::Error::other)?;
    if response.status().as_u16() >= 400 {
        return Err(io::Error::other(format!(
            "rule fetch returned HTTP {}",
            response.status()
        )));
    }
    if matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
        if let Some(location) = response.headers().get(http::header::LOCATION) {
            let target = redirect_base
                .join(location.to_str().map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
            tx.send(Ok(DownloadEvent::Redirect(target)))
                .await
                .map_err(|_| io::Error::other("rule download consumer retired"))?;
            return Ok(());
        }
    }
    let mut bytes = 0usize;
    while let Some(frame) = response.body_mut().frame().await {
        if let Ok(data) = frame.map_err(io::Error::other)?.into_data() {
            bytes = bytes
                .checked_add(data.len())
                .ok_or_else(|| io::Error::other("rule size overflow"))?;
            if bytes as u64 > zero_runtime::configuration::MAX_RULE_SET_CACHE_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "downloaded rule set exceeds {} bytes",
                        zero_runtime::configuration::MAX_RULE_SET_CACHE_BYTES
                    ),
                ));
            }
            tx.send(Ok(DownloadEvent::Data(data)))
                .await
                .map_err(|_| io::Error::other("rule download consumer retired"))?;
        }
    }
    Ok(())
}
