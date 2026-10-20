//! The selected host capability owns QUIC. DNS owns its unchanged DoQ framing.
use super::*;
use crate::{DnsConnectContext, DnsQuicBidiConnector, DnsQuicBidiRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

impl DoqDnsResolver {
    pub(crate) async fn exchange_hosted(
        &self,
        query: &[u8],
        provider: &dyn DnsQuicBidiConnector,
    ) -> io::Result<Vec<u8>> {
        let query_user = self.resources.query_user()?;
        let mut last_error = None;
        for endpoint in &self.addrs {
            let deadline = super::super::ATTEMPT_DEADLINE
                .try_with(|end| *end)
                .unwrap_or_else(|_| tokio::time::Instant::now() + DNS_TIMEOUT);
            let context = DnsConnectContext {
                deadline: Some(deadline),
                cancel: query_user.cancellation(),
            };
            // A dropped query actively cancels its original inline connector.
            // Its persistent host native/driver owners remain independently held.
            let _cancel = context.cancel.clone().drop_guard();
            let work = self.exchange_hosted_with(*endpoint, query, provider, context.clone());
            let result = tokio::select! { biased;
                _ = context.cancel.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted,"DoQ query cancelled")),
                result = tokio::time::timeout_at(deadline, work) => result
                    .map_err(|source|io::Error::new(io::ErrorKind::TimedOut,source))
                    .and_then(std::convert::identity),
            };
            match result {
                Ok(response) => return Ok(response),
                Err(source) => last_error = Some(source),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "DoQ backend has no endpoint")
        }))
    }
    async fn exchange_hosted_with(
        &self,
        endpoint: SocketAddr,
        query: &[u8],
        provider: &dyn DnsQuicBidiConnector,
        context: DnsConnectContext,
    ) -> io::Result<Vec<u8>> {
        let carrier = provider
            .connect_bidi(
                DnsQuicBidiRequest {
                    endpoint,
                    server_name: self.server_name.clone(),
                    alpn: vec![b"doq".to_vec()],
                },
                context,
            )
            .await?;
        if let Some(binding) = &carrier.binding {
            binding.check_current()?;
        }
        let mut stream = carrier.stream;
        let length: u16 = query
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "DNS message is too large"))?;
        let exchange = async {
            stream.write_all(&length.to_be_bytes()).await?;
            stream.write_all(query).await?;
            stream.shutdown().await?; // The actual QUIC send half emits FIN.
            let mut length = [0; 2];
            stream.read_exact(&mut length).await?;
            let length = usize::from(u16::from_be_bytes(length));
            if length < 12 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "DoQ response is too short",
                ));
            }
            let mut response = vec![0; length];
            stream.read_exact(&mut response).await?;
            Ok(response)
        };
        let response = if let Some(binding) = &carrier.binding {
            tokio::select! { biased;
                source = binding.revoked() => Err(source),
                response = exchange => response,
            }
        } else {
            exchange.await
        }?;
        if let Some(binding) = &carrier.binding {
            binding.check_current()?;
        }
        Ok(response)
    }
}
