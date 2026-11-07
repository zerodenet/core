//! Thin validated config projection into the protocol-owned standard component.
use super::*;
impl Hysteria2Adapter {
    pub(crate) fn inbound_options_in_runtime(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> Result<::hysteria2::component::Options, crate::component::RegistrationError> {
        let n = |value| std::num::NonZeroUsize::new(value).expect("validated runtime budget");
        let queues = ::hysteria2::transport::Hysteria2H3QueueLimits::new(
            n(runtime.effective_hysteria2_h3_max_pending_classifications()),
            n(runtime.effective_hysteria2_h3_max_pending_tcp_streams()),
            n(runtime.effective_hysteria2_h3_max_pending_web_streams()),
            n(runtime.effective_hysteria2_h3_max_pending_web_requests()),
        )
        .map_err(|source| {
            crate::component::RegistrationError::Planning(
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::InvalidConfig,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source),
            )
        })?;
        self.inbound_options(inbound, source_dir)?
            .with_h3_queue_limits(queues)
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

    pub(crate) fn inbound_options(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<::hysteria2::component::Options, crate::component::RegistrationError> {
        self.standard_options(inbound, source_dir)
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
    fn standard_options(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<::hysteria2::component::Options, EngineError> {
        let InboundProtocolConfig::Hysteria2 {
            password,
            users,
            cert_path,
            key_path,
            transport,
            up_bps,
            down_bps,
            masquerade,
            ..
        } = &inbound.protocol
        else {
            return Err(std::io::Error::other("Hysteria2 source received another protocol").into());
        };
        let listen = zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port)
            .ok_or_else(|| {
                std::io::Error::other("Hysteria2 QUIC listener requires a literal IP address")
            })?;
        let settings = transport
            .validated(*down_bps, *up_bps)
            .map_err(std::io::Error::other)?;
        let bind = Hysteria2InboundBindPlan::from_options_refs(
            source_dir,
            Hysteria2InboundBindOptionsRef {
                cert_path: cert_path.as_deref(),
                key_path: key_path.as_deref(),
            },
        )
        .with_settings(settings)
        .with_salamander_password(
            transport
                .obfs
                .as_ref()
                .map(zero_config::Hysteria2ObfsConfig::salamander_password),
        );
        let users = inbound_user_refs(password, users);
        let profile =
            Hysteria2AuthenticatedInboundProfile::from_options_refs(Hysteria2InboundOptionsRef {
                users: users.iter().copied(),
            })
            .with_settings(settings)
            .with_masquerade(prepare_masquerade(masquerade, source_dir)?);
        let destination = |listen: &zero_config::ListenConfig| {
            zero_core::request::Destination::new(
                match zero_core::address::parse_ip_literal(&listen.address) {
                    Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                    Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                    None => zero_core::Address::Domain(listen.address.clone()),
                },
                listen.port,
            )
        };
        let mut identity = serde_json::to_vec(inbound).map_err(std::io::Error::other)?;
        if let Some(dir) = source_dir {
            identity.extend_from_slice(dir.as_os_str().as_encoded_bytes());
        }
        Ok(::hysteria2::component::Options::new(
            listen,
            bind,
            profile,
            self.inbound_profiles.clone(),
            &inbound.tag,
            masquerade.http.as_ref().map(destination),
            masquerade.https.as_ref().map(destination),
            masquerade.force_https,
            &identity,
        )?)
    }
}
pub(super) fn prepare_masquerade(
    config: &zero_config::Hysteria2MasqueradeConfig,
    source_dir: Option<&std::path::Path>,
) -> Result<::hysteria2::transport::Hysteria2Masquerade, EngineError> {
    use ::hysteria2::transport::Hysteria2Masquerade as M;
    use zero_config::Hysteria2MasqueradeResponseConfig as C;
    Ok(match &config.response {
        C::NotFound {} => M::NotFound,
        C::File { dir } => M::file(dir, source_dir)?,
        C::Proxy {
            url,
            rewrite_host,
            insecure,
            x_forwarded,
        } => M::proxy_with_options(url, *rewrite_host, *insecure, *x_forwarded)?,
        C::String {
            content,
            status,
            content_type,
        } => M::content(content, *status, content_type)?,
    })
}
