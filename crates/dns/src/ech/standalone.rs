//! The explicit UDP library root may query only its actual granted originals.
use super::*;
impl DnsSystem {
    pub(super) async fn query_root_ech(
        &self,
        name: &str,
        server: &str,
    ) -> io::Result<EchDnsRecord> {
        let name = crate::message::normalize_domain(name)?;
        self.wire_query_coordinator.ensure_local_open()?;
        if crate::localhost::is_localhost(&name) {
            return Ok(EchDnsRecord {
                config_list: None,
                ttl_seconds: crate::message::DEFAULT_NEGATIVE_TTL_SECONDS,
            });
        }
        let endpoint = EchDnsEndpoint::parse(server)?;
        if !endpoint.is_datagram() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "this actual standalone root has no ECH Stream grant",
            ));
        }
        #[cfg(feature = "udp")]
        {
            let addresses = self.bootstrap(endpoint.host()).await?;
            let config = zero_config::DnsServerConfig::Udp {
                host: endpoint.host().to_owned(),
                port: endpoint.port(),
                bootstrap: addresses,
                detour: None,
            };
            let backend = self
                .owned_root
                .as_ref()
                .expect("actual standalone root")
                .build_backend(&config, self.egress_interface.clone())?;
            let query = crate::message::build_query(&name, crate::message::TYPE_HTTPS)?;
            let original = query.clone();
            let mut scope = crate::coordinator::QueryScope::new(
                crate::DnsQueryRole::Node,
                self.egress_interface.generation(),
            );
            scope.config_generation = self
                .config_generation
                .load(std::sync::atomic::Ordering::Acquire);
            let key = crate::coordinator::QueryKey::new(&name, crate::message::TYPE_HTTPS, scope)
                .with_wire_query(&query)
                .with_endpoint(endpoint.as_str());
            let end = self.ech_deadline();
            let mut response = self
                .wire_query_coordinator
                .resolve(key, async move {
                    let resolver = backend.owned_udp().expect("actual UDP root backend");
                    crate::backends::ATTEMPT_DEADLINE
                        .scope(end, resolver.exchange_owned(&query))
                        .await
                        .into_parts(None)
                        .0
                })
                .await?;
            response[..2].copy_from_slice(&original[..2]);
            let parsed = crate::message::parse_ech_config_response(&original, &response)?;
            Ok(EchDnsRecord {
                config_list: parsed.config_list,
                ttl_seconds: parsed.ttl_seconds,
            })
        }
        #[cfg(not(feature = "udp"))]
        Err(unsupported("UDP"))
    }
}
