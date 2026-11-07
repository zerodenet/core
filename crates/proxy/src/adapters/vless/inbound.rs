//! Validated config projection; supported sources use the existing standard factory.
use super::*;
impl VlessAdapter {
    pub(crate) fn inbound_options_in_runtime(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> Result<::vless::component::inbound::listener::Options, crate::component::RegistrationError>
    {
        let n = |value| std::num::NonZeroUsize::new(value).expect("validated runtime budget");
        self.inbound_options(inbound, source_dir)?
            .with_metadata_limits(
                n(runtime.effective_metadata_max_probe_sockets(2)),
                n(runtime.effective_metadata_max_probe_attempts(8)),
                n(runtime.effective_metadata_max_send_calls(2)),
            )
            .and_then(|options| {
                options.with_quic_limits(
                    n(runtime.effective_quic_source_max_connections()),
                    n(runtime.effective_quic_shared_endpoint_max_users()),
                )
            })
            .map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::InvalidConfig,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source)
                .into()
            })
    }

    pub(super) fn listener_request(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<VlessInboundListenerRequest, EngineError> {
        match &inbound.protocol {
            InboundProtocolConfig::Vless {
                final_mask,
                mkcp,
                hysteria,
                decryption,
                users,
                reality,
                tls,
                ws,
                grpc,
                h2,
                http_upgrade,
                split_http,
                fallback,
                mux_response_backlog_frames,
                mux_response_backlog_bytes,
                ..
            } => {
                let user_refs = inbound_user_refs(users);
                let request = VlessInboundListenerRequest::from_options_refs(
                    source_dir,
                    VlessInboundOptionsRef {
                        decryption: decryption.as_deref(),
                        users: user_refs.iter().copied(),
                        reality: reality
                            .as_deref()
                            .map(|reality| VlessRealityServerOptionsRef {
                                target: reality.target.as_ref().map(self::reality::target),
                                policy: vless::reality_policy::PolicyRef {
                                    server_names: &reality.server_names,
                                    min_client_version: reality.min_client_version.as_deref(),
                                    max_client_version: reality.max_client_version.as_deref(),
                                    max_time_diff_ms: reality.max_time_diff_ms,
                                },
                                mldsa65_seed: reality.mldsa65_seed.as_deref(),
                                private_key: reality.private_key.as_str(),
                                short_ids: reality.short_ids.as_slice(),
                                server_name: reality.server_name.as_deref(),
                                cipher_suites: reality.cipher_suites.as_slice(),
                            }),
                        tls: tls.as_deref(),
                        ws: ws.as_deref(),
                        grpc: grpc.as_deref(),
                        h2: h2.as_deref(),
                        http_upgrade: http_upgrade.as_deref(),
                        split_http: split_http.as_deref(),
                        fallback: fallback.as_deref(),
                        mux_response_backlog_frames: *mux_response_backlog_frames,
                        mux_response_backlog_bytes: *mux_response_backlog_bytes,
                    },
                )
                .map_err(EngineError::from)?
                .with_transport_runtime(&self.runtime)
                .with_final_mask(
                    final_mask
                        .as_deref()
                        .map(finalmask::options)
                        .unwrap_or_default(),
                )?
                .with_mkcp_masks(
                    mkcp.as_deref().map(mkcp::settings),
                    &final_mask
                        .as_deref()
                        .map(finalmask::udp)
                        .unwrap_or_default(),
                )?
                .with_hysteria(
                    hysteria
                        .as_deref()
                        .map(|config| hysteria::inbound(config, source_dir))
                        .transpose()?,
                );
                Ok(request)
            }
            _ => Err(std::io::Error::other("VLESS source received another protocol").into()),
        }
    }
    pub(crate) fn inbound_options(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<::vless::component::inbound::listener::Options, crate::component::RegistrationError>
    {
        let request = self
            .listener_request(inbound, source_dir)
            .map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::InvalidConfig,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source)
            })?;
        let endpoint =
            zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port);
        let listen = zero_core::request::Destination::new(
            match endpoint.map(|a| a.ip()) {
                Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                None => zero_core::Address::Domain(inbound.listen.address.clone()),
            },
            inbound.listen.port,
        );
        let mut identity = serde_json::to_vec(inbound).map_err(|source| {
            zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            )
            .with_source(source)
        })?;
        if let Some(path) = source_dir {
            identity.extend_from_slice(path.to_string_lossy().as_bytes());
        }
        if matches!(listen.address, zero_core::Address::Domain(_))
            && matches!(&inbound.protocol, InboundProtocolConfig::Vless {mkcp:Some(_),final_mask:Some(mask),..}
                if finalmask::udp(mask).iter().any(|mask|matches!(mask,zero_transport::finalmask::udp::Mask::Xdns {..}|zero_transport::finalmask::udp::Mask::Xicmp {..})))
        {
            return Err(standard_prepare_error(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "masked mKCP listener requires a literal endpoint",
            ))
            .into());
        }
        let options = ::vless::component::inbound::listener::Options::new(
            listen,
            request.prepare_publication(&self.runtime, &inbound.tag),
            &identity,
        );
        if matches!(
            &inbound.protocol,
            InboundProtocolConfig::Vless { quic: Some(_), .. }
        ) {
            let plan = self
                .quic_bind_plan(inbound, source_dir)
                .map_err(standard_prepare_error)?;
            return Ok(options.with_quic(plan).map_err(standard_prepare_error)?);
        }
        Ok(options)
    }
    pub(super) fn quic_bind_plan(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<VlessInboundBindPlan, EngineError> {
        let InboundProtocolConfig::Vless {
            quic,
            split_http,
            hysteria,
            final_mask,
            ..
        } = &inbound.protocol
        else {
            return Err(std::io::Error::other("invalid VLESS bind input").into());
        };
        Ok(VlessInboundBindPlan::from_options_refs(
            source_dir,
            quic.as_deref().map(|q| VlessQuicBindOptionsRef {
                tls: q,
                cert_path: q.cert_path.as_deref(),
                key_path: q.key_path.as_deref(),
            }),
        )
        .with_http3(quic.is_some() && (split_http.is_some() || hysteria.is_some()))
        .with_hysteria(hysteria.as_deref().map(hysteria::options))?
        .with_final_mask(
            final_mask
                .as_deref()
                .map(finalmask::udp)
                .unwrap_or_default(),
        )?)
    }
}
fn standard_prepare_error(
    source: impl std::error::Error + Send + Sync + 'static,
) -> zero_platform_tokio::contract::ChannelError {
    zero_platform_tokio::contract::ChannelError::new(
        zero_platform_tokio::contract::Stage::Prepare,
        zero_platform_tokio::contract::Reason::InvalidConfig,
        zero_platform_tokio::contract::Impact::Component,
    )
    .with_source(source)
}
