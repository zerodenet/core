//! Thin configuration projection into the actual protocol-owned standard source.
use super::*;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
impl VmessAdapter {
    pub(crate) fn inbound_options(
        &self,
        inbound: &InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<::vmess::component::inbound::listener::Options, crate::component::RegistrationError>
    {
        let InboundProtocolConfig::Vmess {
            users,
            tls,
            ws,
            grpc,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
        } = &inbound.protocol
        else {
            return Err(zero_config::ConfigError::InvalidInbound(
                "VMess factory received another protocol".into(),
            )
            .into());
        };
        let users = inbound_user_refs(users);
        let request = VmessInboundListenerRequest::from_options_refs_staged(
            source_dir,
            VmessInboundOptionsRef {
                users: users.iter().copied(),
                tls: tls.as_deref(),
                ws: ws.as_deref(),
                grpc: grpc.as_deref(),
                mux_response_backlog_frames: *mux_response_backlog_frames,
                mux_response_backlog_bytes: *mux_response_backlog_bytes,
            },
            &self.runtime,
            &inbound.tag,
        )
        .map_err(|source| {
            ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                .with_source(source)
        })?;
        let endpoint =
            zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port);
        let listen = zero_core::request::Destination::new(
            match endpoint.map(|endpoint| endpoint.ip()) {
                Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                None => zero_core::Address::Domain(inbound.listen.address.clone()),
            },
            inbound.listen.port,
        );
        let mut identity = serde_json::to_vec(inbound).map_err(|source| {
            ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                .with_source(source)
        })?;
        if let Some(directory) = source_dir {
            identity.extend_from_slice(directory.to_string_lossy().as_bytes());
        }
        Ok(::vmess::component::inbound::listener::Options::new(
            listen, request, &identity,
        ))
    }
}

#[cfg(test)]
mod tests;
