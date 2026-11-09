//! Genuine standalone requests retain their actual driver through retirement.
use super::*;
impl DohDnsResolver {
    pub(crate) async fn exchange(
        &self,
        query: &[u8],
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<Vec<u8>> {
        let addrs = self.endpoint_addresses();
        let mut last_error = None;
        for addr in addrs {
            match self.exchange_with(addr, query, detour, connector).await {
                Ok(response) => return Ok(response),
                // Only our own completed request + failed actual retirement
                // constructs this top-level sealed payload. Another candidate
                // cannot overtake that original owner or its retained source.
                Err(error)
                    if error
                        .get_ref()
                        .is_some_and(|source| source.is::<ExchangeCleanupFailure>()) =>
                {
                    return Err(error)
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "DoH backend has no endpoint")
        }))
    }

    async fn exchange_with(
        &self,
        addr: SocketAddr,
        query: &[u8],
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<Vec<u8>> {
        let original_ceiling = tokio::time::Instant::now() + DNS_TIMEOUT;
        let deadline = super::super::ATTEMPT_DEADLINE
            .try_with(|deadline| *deadline)
            .unwrap_or(original_ceiling)
            .min(original_ceiling);
        let result = tokio::time::timeout_at(
            deadline,
            self.exchange_with_timeout(addr, query, detour, connector),
        )
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "DoH request to {addr} timed out after {}ms",
                    DNS_TIMEOUT.as_millis()
                ),
            )
        })
        .and_then(std::convert::identity);
        match result {
            Ok(response) => Ok(response),
            Err(opening) => {
                // The actual request frame/ReadyClient has returned above.
                // Join its retired original driver and affected users inline,
                // before handing the failure to a caller or another endpoint.
                // Use this same deadline; do not manufacture a new grace period.
                match self.owner.drain_retired(deadline).await {
                    Ok(()) => Err(opening),
                    Err(cleanup) => Err(io::Error::new(
                        cleanup.kind(),
                        ExchangeCleanupFailure { opening, cleanup },
                    )),
                }
            }
        }
    }
    async fn exchange_with_timeout(
        &self,
        addr: SocketAddr,
        query: &[u8],
        detour: Option<&str>,
        connector: Option<&dyn crate::DnsOutboundConnector>,
    ) -> io::Result<Vec<u8>> {
        let mut client = self.ready_client(addr, detour, connector).await?;
        let request = self.request(query.len())?;
        let (response, mut body) = client
            .sender
            .send_request(request, false)
            .map_err(|error| context_error("DoH send request failed", error))?;
        body.send_data(Bytes::copy_from_slice(query), true)
            .map_err(|error| context_error("DoH send body failed", error))?;

        let response = response
            .await
            .map_err(|error| context_error("DoH response failed", error))?;
        let response = read_response(response).await?;
        client.complete()?;
        Ok(response)
    }
}
/// Both actual errors remain owned if original retirement is still incomplete.
/// No mutable pool/receiver/controller is stored in this returned projection.
#[derive(Debug)]
struct ExchangeCleanupFailure {
    opening: io::Error,
    cleanup: io::Error,
}
impl std::fmt::Display for ExchangeCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; original DoH retirement: {}",
            self.opening, self.cleanup
        )
    }
}
impl std::error::Error for ExchangeCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.opening)
    }
}
