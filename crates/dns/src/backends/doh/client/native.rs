//! Native setup and H2 driving occur under the same original registration.
use super::*;
use crate::{native::NativeDnsProvider, OwnedDnsQueryTasks};
use tokio::time::Instant;
impl DohDnsResolver {
    pub(in super::super) async fn ready_native_client(
        &self,
        addr: SocketAddr,
        detour: Option<&str>,
        provider: Arc<dyn NativeDnsProvider>,
        end: Instant,
    ) -> io::Result<ReadyClient> {
        self.owner.ensure_open()?;
        let key = ConnectionKey {
            addr,
            generation: self.egress.generation(),
            detour: detour.map(str::to_owned),
        };
        if let Some(client) = self.cached_client(&key).await {
            if let Ok(client) = client.ready().await {
                return Ok(client);
            }
        }
        let _opening = tokio::time::timeout_at(end, self.connect_lock.lock())
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        key.ensure_current(&self.egress)?;
        if let Some(client) = self.cached_client(&key).await {
            if let Ok(client) = client.ready().await {
                return Ok(client);
            }
        }
        self.clients
            .lock()
            .await
            .retain(|entry| entry.key.addr != key.addr);
        self.owner.drain_retired(end).await?;
        self.owner.ensure_open()?;
        #[cfg(feature = "test-support")]
        self.test_io_started
            .store(true, std::sync::atomic::Ordering::Release);
        let client = self
            .owner
            .client(None, true, self.egress.clone(), key.generation);
        client
            .native
            .set(super::super::native::NativeClient::new(provider.clone()))
            .map_err(|_| io::Error::other("DoH original installed twice"))?;
        let (ready, answer) = tokio::sync::oneshot::channel();
        let tls = self.tls.clone();
        let name = self.server_name.clone();
        let egress = self.egress.clone();
        let opened = key.clone();
        let driver = client.clone();
        let tasks = provider.tasks();
        let job = async move {
            let setup = async {
                let caller = OwnedDnsQueryTasks::current_witness()?;
                let cancel = OwnedDnsQueryTasks::current_cancellation()?;
                let (loan, binding) = match &opened.detour {
                    Some(outbound) => {
                        let (loan, binding) =
                            OwnedDnsQueryTasks::native_io(provider.detour_stream(
                                outbound.clone(),
                                opened.addr,
                                caller,
                                cancel,
                                end,
                            ))
                            .await??;
                        (loan, Some(binding))
                    }
                    None => (
                        OwnedDnsQueryTasks::native_io(provider.stream(
                            opened.addr,
                            caller,
                            cancel,
                            end,
                        ))
                        .await??,
                        None,
                    ),
                };
                OwnedDnsQueryTasks::record_native(loan.owner.original())?;
                let (io, owner) = (loan.io, loan.owner);
                // Retain the actual LoanOwner before TLS/H2 can suspend.
                *driver
                    .native
                    .get()
                    .expect("installed DoH owner")
                    .original
                    .lock()
                    .await = Some(owner);
                if let Some(binding) = binding {
                    driver.bind(binding)?;
                }
                drop(provider);
                let stream = Box::pin(io) as zero_platform_tokio::contract::stream::BoxStream;
                OwnedDnsQueryTasks::native_io(handshake(&opened, stream, tls, name, &egress))
                    .await?
            };
            match tokio::time::timeout_at(end, setup).await {
                Ok(Ok((sender, connection))) => {
                    if ready.send(Ok(sender)).is_ok() {
                        let result = tokio::select! {biased;
                            _=driver.cancel.cancelled()=>Ok(()),
                            source=driver.revoked()=>Err(source),
                            result=OwnedDnsQueryTasks::native_io(connection)=>result.and_then(|result|result.map_err(io::Error::other)),
                        };
                        if let Err(source) = result {
                            tracing::debug!(%source,"owned DoH driver ended");
                        }
                    }
                }
                Ok(Err(source)) => {
                    let _ = ready.send(Err(source));
                }
                Err(source) => {
                    let _ = ready.send(Err(io::Error::new(io::ErrorKind::TimedOut, source)));
                }
            }
            driver.retire();
        };
        self.owner
            .spawn_native(&tasks, client.clone(), key.generation, job)?;
        let mut opening = Opening(Some(client.clone()));
        let sender = tokio::time::timeout_at(end, answer)
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
            .map_err(io::Error::other)??;
        key.ensure_current(&self.egress)?;
        let entry = DohClient {
            key,
            sender,
            state: client,
        };
        let lease = entry.lease().ready().await?;
        self.clients.lock().await.push(entry);
        opening.0.take();
        Ok(lease)
    }
}

pub(super) async fn handshake(
    key: &ConnectionKey,
    stream: zero_platform_tokio::contract::stream::BoxStream,
    tls: Option<Arc<rustls::ClientConfig>>,
    name: String,
    egress: &zero_platform_tokio::EgressInterfaceControl,
) -> io::Result<(
    h2::client::SendRequest<Bytes>,
    h2::client::Connection<zero_platform_tokio::contract::stream::BoxStream, Bytes>,
)> {
    let stream = if let Some(tls) = tls {
        let name = rustls::pki_types::ServerName::try_from(name).map_err(io::Error::other)?;
        let stream = tokio_rustls::TlsConnector::from(tls)
            .connect(name, stream)
            .await
            .map_err(io::Error::other)?;
        Box::pin(stream) as zero_platform_tokio::contract::stream::BoxStream
    } else {
        stream
    };
    key.ensure_current(egress)?;
    h2::client::handshake(stream)
        .await
        .map_err(io::Error::other)
}

struct Opening(Option<Arc<ClientState>>);
impl Drop for Opening {
    fn drop(&mut self) {
        if let Some(client) = &self.0 {
            client.retire();
        }
    }
}
