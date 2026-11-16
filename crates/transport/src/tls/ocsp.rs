//! Bounded automatic OCSP retrieval using the shared verified HTTP carrier.
mod request;
use crate::http_client::HttpClient;
pub(super) enum Client<'a> {
    Library(&'a HttpClient),
    Hosted {
        resources: &'a dyn zero_platform_tokio::contract::component::ResourceAccess,
        driver: &'a zero_platform_tokio::contract::scope::TaskJoinWitness,
        tasks: &'a zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    },
}
impl Client<'_> {
    async fn send(
        &self,
        request: http::Request<Bytes>,
        limit: usize,
    ) -> io::Result<http::Response<Vec<u8>>> {
        match self {
            Self::Hosted {
                resources,
                driver,
                tasks,
            } => super::source::http_client::send(*resources, driver, tasks, request, limit).await,
            Self::Library(client) => {
                let mut response = client.send(request).await?;
                let mut bytes = Vec::new();
                if !matches!(response.status().as_u16(), 301..=303 | 307 | 308)
                    && response.status().is_success()
                {
                    while let Some(chunk) = response.body_mut().data().await? {
                        if chunk.len() > limit.saturating_sub(bytes.len()) {
                            return Err(invalid("OCSP HTTP response exceeds size limit"));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                }
                let mut result = http::Response::builder().status(response.status());
                *result.headers_mut().unwrap() = response.headers().clone();
                result.body(bytes).map_err(io::Error::other)
            }
        }
    }
}
use bytes::Bytes;
use std::{io, time::Duration};

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
async fn fetch(
    client: &Client<'_>,
    uri: &str,
    body: Option<Vec<u8>>,
    limit: usize,
) -> io::Result<Vec<u8>> {
    let mut url = url::Url::parse(uri).map_err(io::Error::other)?;
    let mut body = body;
    for redirects in 0..=10 {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(invalid("OCSP URL must use HTTP or HTTPS"));
        }
        let mut request = http::Request::builder().uri(url.as_str());
        if body.is_some() {
            request = request
                .method(http::Method::POST)
                .header(http::header::CONTENT_TYPE, "application/ocsp-request");
        }
        let response = client
            .send(
                request
                    .body(Bytes::from(body.clone().unwrap_or_default()))
                    .map_err(io::Error::other)?,
                limit,
            )
            .await?;
        if matches!(response.status().as_u16(), 301..=303 | 307 | 308) {
            if redirects == 10 {
                return Err(invalid("too many OCSP HTTP redirects"));
            }
            let location = response
                .headers()
                .get(http::header::LOCATION)
                .ok_or_else(|| invalid("OCSP redirect has no location"))?
                .to_str()
                .map_err(io::Error::other)?;
            url = url.join(location).map_err(io::Error::other)?;
            if matches!(response.status().as_u16(), 301..=303) {
                body = None;
            }
            continue;
        }
        if !response.status().is_success() {
            return Err(invalid("OCSP HTTP response was not successful"));
        }
        let bytes = response.into_body();
        if bytes.is_empty() {
            return Err(invalid("empty OCSP HTTP response"));
        }
        return Ok(bytes);
    }
    unreachable!()
}
pub(super) async fn retrieve(
    client: &HttpClient,
    certificates: &[rustls::pki_types::CertificateDer<'_>],
) -> io::Result<Vec<u8>> {
    retrieve_with(&Client::Library(client), certificates).await
}
pub(super) async fn retrieve_with(
    client: &Client<'_>,
    certificates: &[rustls::pki_types::CertificateDer<'_>],
) -> io::Result<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let leaf = certificates
            .first()
            .ok_or_else(|| invalid("missing OCSP leaf certificate"))?;
        let (responder, issuer_url) = request::endpoints(leaf)?;
        let issuer = match certificates.get(1) {
            Some(issuer) => issuer.to_vec(),
            None => {
                fetch(
                    client,
                    &issuer_url.ok_or_else(|| invalid("certificate has no issuer URL"))?,
                    None,
                    1024 * 1024,
                )
                .await?
            }
        };
        let request = request::encode(leaf, &issuer)?;
        fetch(client, &responder, Some(request), 64 * 1024).await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "OCSP retrieval deadline exceeded"))?
}

#[cfg(test)]
#[path = "../../tests/tls/ocsp.rs"]
mod tests;
