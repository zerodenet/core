use super::*;
#[cfg(any(feature = "dot", feature = "doh"))]
pub(super) async fn exchange(
    config: &DnsServerConfig,
    backend: &crate::backends::ResolverBackend,
    io: zero_platform_tokio::controlled_carrier::loan::LoanIo,
    query: &[u8],
) -> io::Result<Vec<u8>> {
    let name = match config {
        DnsServerConfig::Dot {
            host, server_name, ..
        }
        | DnsServerConfig::Doh {
            host, server_name, ..
        } => server_name.as_ref().unwrap_or(host),
        _ => return Err(unsupported("unsupported native stream DNS transport")),
    };
    let tls = backend.native_tls_config()?;
    let name = rustls::pki_types::ServerName::try_from(name.clone()).map_err(io::Error::other)?;
    let mut stream = tokio_rustls::TlsConnector::from(tls)
        .connect(name, io)
        .await?;
    if let DnsServerConfig::Doh { .. } = config {
        #[cfg(feature = "doh")]
        {
            return Box::pin(doh(stream, backend, query)).await;
        }
        #[cfg(not(feature = "doh"))]
        {
            return Err(unsupported("DoH feature is disabled"));
        }
    }
    crate::backends::write_framed(&mut stream, query).await?;
    crate::backends::read_framed(&mut stream).await
}
#[cfg(feature = "doh")]
async fn doh<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: S,
    backend: &crate::backends::ResolverBackend,
    query: &[u8],
) -> io::Result<Vec<u8>> {
    let (mut sender, driver) = h2::client::handshake(stream)
        .await
        .map_err(io::Error::other)?;
    let work = async {
        sender = sender.ready().await.map_err(io::Error::other)?;
        let request = backend.native_doh_request(query.len())?;
        let (response, mut sending) = sender
            .send_request(request, false)
            .map_err(io::Error::other)?;
        sending
            .send_data(bytes::Bytes::copy_from_slice(query), true)
            .map_err(io::Error::other)?;
        let response = response.await.map_err(io::Error::other)?;
        let answer = crate::backends::ResolverBackend::native_doh_response(response).await?;
        crate::message::parse_response(query, &answer)?;
        Ok(answer)
    };
    tokio::pin!(driver);
    // Existing query task drives H2 inline; no unsupervised driver is spawned.
    tokio::select! {biased;result=work=>result,result=&mut driver=>{result.map_err(io::Error::other)?;Err(io::Error::new(io::ErrorKind::UnexpectedEof,"DoH driver ended before response"))}}
}
