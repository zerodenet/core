use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

const DNS_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct DoqDnsResolver {
    addrs: Vec<SocketAddr>,
    server_name: String,
    client: quinn::ClientConfig,
    egress: zero_platform_tokio::EgressInterfaceControl,
}

impl DoqDnsResolver {
    pub(crate) fn new(
        host: String,
        port: u16,
        bootstrap: Vec<IpAddr>,
        server_name: Option<String>,
        egress: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        use quinn::crypto::rustls::QuicClientConfig;

        let ips = if bootstrap.is_empty() {
            host.parse().map(|ip| vec![ip]).unwrap_or_default()
        } else {
            bootstrap
        };
        if ips.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("DoQ host `{host}` requires a bootstrap address"),
            ));
        }

        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| io::Error::other(format!("DoQ TLS protocol: {error}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"doq".to_vec()];
        let quic = QuicClientConfig::try_from(tls)
            .map_err(|error| io::Error::other(format!("DoQ client config: {error}")))?;

        Ok(Self {
            addrs: ips
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect(),
            server_name: server_name.unwrap_or(host),
            client: quinn::ClientConfig::new(Arc::new(quic)),
            egress,
        })
    }

    pub(crate) async fn exchange(&self, query: &[u8]) -> io::Result<Vec<u8>> {
        let mut last_error = None;
        for addr in &self.addrs {
            match self.exchange_with(*addr, query).await {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "DoQ backend has no endpoint")
        }))
    }

    async fn exchange_with(&self, addr: SocketAddr, query: &[u8]) -> io::Result<Vec<u8>> {
        let interface = self.egress.current_for_peer(addr);
        let socket =
            zero_platform_tokio::bind_std_datagram_socket_for_peer(addr, interface.as_ref())?;
        let mut endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            None,
            socket,
            Arc::new(quinn::TokioRuntime),
        )
        .map_err(|error| io::Error::other(format!("DoQ endpoint: {error}")))?;
        endpoint.set_default_client_config(self.client.clone());

        let connecting = endpoint
            .connect(addr, &self.server_name)
            .map_err(|error| io::Error::other(format!("DoQ connect: {error}")))?;
        let connection = tokio::time::timeout(DNS_TIMEOUT, connecting)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoQ connect timeout"))?
            .map_err(|error| io::Error::other(format!("DoQ connect: {error}")))?;
        let (mut send, mut recv) = tokio::time::timeout(DNS_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoQ stream timeout"))?
            .map_err(|error| io::Error::other(format!("DoQ stream: {error}")))?;

        let length: u16 = query
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DNS message is too large"))?;
        send.write_all(&length.to_be_bytes())
            .await
            .map_err(|error| io::Error::other(format!("DoQ write length: {error}")))?;
        send.write_all(query)
            .await
            .map_err(|error| io::Error::other(format!("DoQ write query: {error}")))?;
        send.finish()
            .map_err(|error| io::Error::other(format!("DoQ finish query: {error}")))?;

        let response = tokio::time::timeout(DNS_TIMEOUT, async {
            let mut length = [0_u8; 2];
            recv.read_exact(&mut length)
                .await
                .map_err(|error| io::Error::other(format!("DoQ read response length: {error}")))?;
            let length = u16::from_be_bytes(length) as usize;
            if length < 12 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DoQ response is too short",
                ));
            }
            let mut response = vec![0_u8; length];
            recv.read_exact(&mut response)
                .await
                .map_err(|error| io::Error::other(format!("DoQ read response: {error}")))?;
            Ok(response)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoQ response timeout"))??;
        endpoint.close(0_u32.into(), b"complete");
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use zero_traits::IpAddress;

    #[tokio::test]
    async fn exchanges_a_framed_dns_message_over_quic() {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate certificate");
        let certificate = certified.cert.der().clone();
        let key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());

        let mut server_tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key.into())
        .unwrap();
        server_tls.alpn_protocols = vec![b"doq".to_vec()];
        let server_quic = QuicServerConfig::try_from(server_tls).unwrap();
        let endpoint = quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(server_quic)),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let addr = endpoint.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let size = recv.read_u16().await.unwrap() as usize;
            let mut query = vec![0_u8; size];
            recv.read_exact(&mut query).await.unwrap();
            let response = crate::message::build_address_response(
                &query,
                &[IpAddress::V4([203, 0, 113, 53])],
                120,
            );
            send.write_u16(response.len() as u16).await.unwrap();
            send.write_all(&response).await.unwrap();
            send.finish().unwrap();
            send.stopped().await.unwrap();
        });

        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let mut client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        client_tls.alpn_protocols = vec![b"doq".to_vec()];
        let resolver = DoqDnsResolver {
            addrs: vec![addr],
            server_name: "localhost".to_owned(),
            client: quinn::ClientConfig::new(Arc::new(
                QuicClientConfig::try_from(client_tls).unwrap(),
            )),
            egress: zero_platform_tokio::EgressInterfaceControl::default(),
        };
        let query = crate::message::build_query("doq.example", crate::message::TYPE_A).unwrap();

        let response = resolver.exchange(&query).await.unwrap();

        let parsed = crate::message::parse_response(&query, &response).unwrap();
        assert_eq!(parsed.addresses, vec![IpAddress::V4([203, 0, 113, 53])]);
        server.await.unwrap();
    }
}
