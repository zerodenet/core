//! New physical connections are selected exactly once, after old slot drain.
use super::*;
impl DohDnsResolver {
    pub(super) async fn connect_client(
        &self,
        key: &ConnectionKey,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<ReadyClient> {
        let carrier = crate::backends::connect_tcp_carrier(
            key.addr,
            &self.egress,
            key.detour.as_deref(),
            connector,
        )
        .await
        .map_err(|error| {
            io::Error::new(error.kind(), DohError::new("DoH connect failed", error))
        })?;
        let binding = carrier.binding;
        if let Some(binding) = &binding {
            binding.check_current()?;
        }
        let stream = carrier.stream;
        key.ensure_current(&self.egress)?;
        // The common DNS dial operation logs the actual pre-connect selection;
        // do not re-probe the route afterwards and misreport a new selection
        // as the binding of this already-open socket.
        tracing::debug!(server = %key.addr, outbound = ?key.detour,
            egress_generation = key.generation, "DoH TCP socket connected");

        let (sender, connection) = tokio::select! {biased;
            source=state::revoked(binding.as_deref())=>return Err(source),
            result=self.handshake_client(key, stream)=>result?,
        };
        if let Some(binding) = &binding {
            binding.check_current()?;
        }
        let mut clients = self.clients.lock().await;
        key.ensure_current(&self.egress)?;
        let entry = DohClient {
            key: key.clone(),
            sender,
            state: self.owner.client(
                binding,
                key.detour.is_none(),
                self.egress.clone(),
                key.generation,
            ),
        };
        let client = entry.lease();
        clients.retain(|entry| {
            entry.key.addr != key.addr
                && entry.key.generation == key.generation
                && entry.state.is_reusable()
        });
        // Spawn admission and closing share the actual owner lock. A failed
        // registration drops this unstarted connection without publishing it.
        self.owner.spawn(entry.state.clone(), connection)?;
        clients.push(entry);
        Ok(client)
    }
    async fn handshake_client(
        &self,
        key: &ConnectionKey,
        stream: zero_platform_tokio::TcpRelayStream,
    ) -> io::Result<(
        h2::client::SendRequest<Bytes>,
        h2::client::Connection<zero_platform_tokio::TcpRelayStream, Bytes>,
    )> {
        let stream = if let Some(tls) = &self.tls {
            let server_name = rustls::pki_types::ServerName::try_from(self.server_name.clone())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let connector = tokio_rustls::TlsConnector::from(Arc::clone(tls));
            let tls = connector
                .connect(server_name, stream)
                .await
                .map_err(|error| context_error("DoH TLS failed", error))?;
            zero_platform_tokio::TcpRelayStream::new(tls)
        } else {
            stream
        };
        key.ensure_current(&self.egress)?;
        h2::client::handshake(stream)
            .await
            .map_err(|error| context_error("DoH HTTP/2 handshake failed", error))
    }
}
