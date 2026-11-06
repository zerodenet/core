//! Native operations use this provider's original Scope and native ledger.
use super::*;
impl NativeDnsProvider for Provider {
    fn host_scope(
        &self,
    ) -> Option<(
        zero_core::component::ComponentInstanceId,
        zero_platform_tokio::contract::scope::TaskScopeIdentity,
    )> {
        Some((self.instance, self.registrar.scope_identity()))
    }

    fn source_roots(
        &self,
    ) -> Option<(
        Arc<zero_dns::native::NativeDnsSource>,
        Arc<zero_dns::native::NativeDnsSource>,
    )> {
        Some((
            self.network.native_dns.clone(),
            self.network.query_dns.clone(),
        ))
    }
    fn prepare_ech_endpoint(
        &self,
        endpoint: zero_dns::EchDnsEndpoint,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<Vec<SocketAddr>>> {
        Box::pin(self.prepare_ech(endpoint, caller, cancel, end))
    }
    fn budget(&self) -> Option<Arc<super::Budget>> {
        Some(self.budget.clone())
    }
    fn fence_old_network(
        &self,
        generation: u64,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async move {
            self.tasks.fence_old_network(generation, end).await?;
            self.reap(end).await
        })
    }
    fn query_native_retirement_complete(&self, caller: &TaskJoinWitness) -> bool {
        self.caller_native_retirement_complete(caller)
    }
    fn finish_query(&self, caller: TaskJoinWitness) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async move {
            let end = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            self.close_attempt_originals(&caller, end).await
        })
    }
    fn close_failed_stream_originals(
        &self,
        caller: TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async move { self.close_attempt_originals(&caller, end).await })
    }
    fn owns_detour(&self, outbound: &str) -> bool {
        self.detour_admitted(outbound)
    }
    fn detour_stream(
        &self,
        outbound: String,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<(StreamLoan, Arc<dyn zero_dns::DnsCarrierBinding>)>> {
        Box::pin(self.open_detour(outbound, peer, caller, cancel, end))
    }
    fn detour_udp(
        &self,
        outbound: String,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<zero_dns::native::NativeDnsUdpLoan>> {
        Box::pin(self.open_udp_detour(outbound, peer, caller, cancel, end))
    }
    fn close_stream_originals(
        &self,
        caller: TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(self.close_caller_originals(caller, end))
    }

    fn system(
        &self,
        name: String,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<zero_dns::NativeSystemLookup>> {
        Box::pin(async move {
            let epoch = self.admit(&caller, &cancel, end, 0).await?;
            drop(epoch);
            self.tasks.lookup_system(name, caller, cancel, end).await
        })
    }
    #[cfg(feature = "dns")]
    fn quic(
        &self,
        peer: SocketAddr,
        server_name: String,
        client: quinn::ClientConfig,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<zero_dns::native::NativeDnsQuicLoan>> {
        Box::pin(self.open_quic(peer, server_name, client, caller, cancel, end))
    }
    fn tasks(&self) -> OwnedDnsQueryTasks {
        self.tasks.clone()
    }
    fn stream(
        &self,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<StreamLoan>> {
        Box::pin(async move {
            if !self
                .peers
                .iter()
                .any(|(address, packet, _)| *address == peer && !packet)
            {
                return self.open_ech_stream(peer, caller, cancel, end).await;
            }
            let (grant, view, opener, binding) =
                self.grant(peer, false, caller, cancel.clone(), end).await?;
            let original = grant.adoption();
            let result = async {
                let carrier = opener
                    .prepare_controlled_stream(
                        &binding,
                        destination(peer),
                        CarrierContext {
                            session: None,
                            deadline: Some(end),
                            cancel,
                        },
                        grant,
                    )
                    .map_err(std::io::Error::other)?
                    .open(end)
                    .await
                    .map_err(std::io::Error::other)?;
                carrier
                    .into_parts()
                    .0
                    .into_stream_loan(view)
                    .map_err(std::io::Error::other)
            }
            .await;
            match result {
                Ok(loan) => Ok(loan),
                Err(source) => Err(self.failed_open(source, &original, end).await),
            }
        })
    }
    fn datagram(
        &self,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<DatagramLoan>> {
        Box::pin(async move {
            if !self
                .peers
                .iter()
                .any(|(address, packet, _)| *address == peer && *packet)
            {
                return self.open_ech_datagram(peer, caller, cancel, end).await;
            }
            let (grant, view, opener, binding) =
                self.grant(peer, true, caller, cancel.clone(), end).await?;
            let original = grant.adoption();
            let result = (|| {
                let carrier = opener
                    .prepare_controlled_datagram(
                        &binding,
                        destination(peer),
                        CarrierContext {
                            session: None,
                            deadline: Some(end),
                            cancel,
                        },
                        grant,
                    )
                    .map_err(std::io::Error::other)?;
                carrier
                    .into_datagram_loan(view, zero_core::component::AssociationId(0))
                    .map_err(std::io::Error::other)
            })();
            match result {
                Ok(loan) => Ok(loan),
                Err(source) => Err(self.failed_open(source, &original, end).await),
            }
        })
    }
}
