use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

pub(super) mod owner;
mod resources;

const DNS_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct DoqDnsResolver {
    addrs: Vec<SocketAddr>,
    server_name: String,
    client: quinn::ClientConfig,
    egress: zero_platform_tokio::EgressInterfaceControl,
    owners: std::sync::Mutex<Arc<crate::transport_owners::TransportOwners>>,
    resources: resources::EndpointSet,
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
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"doq".to_vec()];
        let quic = QuicClientConfig::try_from(tls).map_err(io::Error::other)?;

        Ok(Self {
            addrs: ips
                .into_iter()
                .map(|ip| SocketAddr::new(ip, port))
                .collect(),
            server_name: server_name.unwrap_or(host),
            client: quinn::ClientConfig::new(Arc::new(quic)),
            egress,
            owners: std::sync::Mutex::new(Arc::default()),
            resources: resources::EndpointSet::default(),
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

    pub(crate) fn endpoint_labels(&self) -> Vec<String> {
        self.addrs.iter().map(ToString::to_string).collect()
    }

    pub(super) fn attach_registry(&self, owners: Arc<crate::transport_owners::TransportOwners>) {
        *self.owners.lock().expect("DoQ resource registry poisoned") = owners;
    }

    pub(super) fn resource_admission_open(&self) -> bool {
        self.resources.open()
    }
    pub(super) fn retire_resources(&self) {
        self.resources.retire();
    }
    pub(super) async fn shutdown_resources(
        &self,
        deadline: tokio::time::Instant,
    ) -> io::Result<()> {
        self.resources.shutdown(deadline).await
    }

    async fn exchange_with(&self, addr: SocketAddr, query: &[u8]) -> io::Result<Vec<u8>> {
        let owners = self
            .owners
            .lock()
            .expect("DoQ resource registry poisoned")
            .clone();
        let owner = owner::EndpointOwner::new();
        self.resources.register(owner.clone(), &owners)?;
        let _close = owner::CloseOnDrop(owner.clone());
        let deadline = super::ATTEMPT_DEADLINE
            .try_with(|end| *end)
            .unwrap_or_else(|_| tokio::time::Instant::now() + DNS_TIMEOUT);
        owners.drain_retired_doq(deadline).await?;
        let work = async {
            let _user = owner.query_user()?;
            owner.setup(addr, self.client.clone(), &self.egress)?;
            self.exchange_owned(&owner, addr, query).await
        };
        let result = tokio::select! {biased;
            _=owner.status.cancel.cancelled()=>match owner.status.check() {Err(source)=>Err(source),Ok(())=>Err(io::Error::new(io::ErrorKind::Interrupted,"DoQ query endpoint was cancelled"))},
            result=tokio::time::timeout_at(deadline, work)=>result
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))
                .and_then(std::convert::identity),
        };
        let closed = owner.shutdown(deadline).await;
        match (result, closed) {
            (Ok(response), Ok(())) => Ok(response),
            (Err(source), _) => Err(source),
            (Ok(_), Err(source)) => Err(source),
        }
    }

    async fn exchange_owned(
        &self,
        owner: &owner::EndpointOwner,
        addr: SocketAddr,
        query: &[u8],
    ) -> io::Result<Vec<u8>> {
        let connecting = owner.connect(addr, &self.server_name)?;
        let connection = tokio::time::timeout(DNS_TIMEOUT, connecting)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoQ connect timeout"))?
            .map_err(io::Error::other)?;
        let (mut send, mut recv) = tokio::time::timeout(DNS_TIMEOUT, connection.open_bi())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoQ stream timeout"))?
            .map_err(io::Error::other)?;

        let length: u16 = query
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DNS message is too large"))?;
        send.write_all(&length.to_be_bytes())
            .await
            .map_err(io::Error::other)?;
        send.write_all(query).await.map_err(io::Error::other)?;
        send.finish().map_err(io::Error::other)?;

        let response = tokio::time::timeout(DNS_TIMEOUT, async {
            let mut length = [0_u8; 2];
            recv.read_exact(&mut length)
                .await
                .map_err(io::Error::other)?;
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
                .map_err(io::Error::other)?;
            Ok(response)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DoQ response timeout"))??;
        Ok(response)
    }
}

#[cfg(test)]
mod tests;
