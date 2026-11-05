//! DoT is inline; DoH uses its existing persistent host-owned pool.
use super::*;
pub(super) async fn exchange(
    config: &DnsServerConfig,
    backend: &crate::backends::ResolverBackend,
    io: zero_platform_tokio::controlled_carrier::loan::LoanIo,
    query: &[u8],
) -> io::Result<Vec<u8>> {
    let DnsServerConfig::Dot {
        host, server_name, ..
    } = config
    else {
        return Err(unsupported("native stream DNS is not DoT"));
    };
    let name =
        rustls::pki_types::ServerName::try_from(server_name.as_ref().unwrap_or(host).clone())
            .map_err(io::Error::other)?;
    let mut stream = tokio_rustls::TlsConnector::from(backend.native_tls_config()?)
        .connect(name, io)
        .await?;
    crate::backends::write_framed(&mut stream, query).await?;
    crate::backends::read_framed(&mut stream).await
}
