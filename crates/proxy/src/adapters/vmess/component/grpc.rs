//! Cold production role selection over the protocol's actual gRPC owner.
mod production;
use super::*;
use crate::component::background::stateless;
pub(crate) use production::streams_selected;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};
pub(crate) enum Options {
    Ordinary(Arc<stateless::Options<::vmess::component::Options>>),
    Grpc(Arc<::vmess::component::grpc::Options>),
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        match options {
            Options::Ordinary(options) => {
                stateless::Factory(::vmess::component::Factory).describe(options)
            }
            Options::Grpc(options) => ::vmess::component::grpc::Factory.describe(options),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        match options.as_ref() {
            Options::Ordinary(options) => {
                stateless::Factory(::vmess::component::Factory).build(options.clone(), context)
            }
            Options::Grpc(options) => {
                ::vmess::component::grpc::Factory.build(options.clone(), context)
            }
        }
    }
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    if super::native_input(input) {
        return true;
    }
    matches!(&input.protocol, OutboundProtocolConfig::Vmess {server, mux_concurrency:None, grpc:Some(_), ws:None, tls,..}
        if !server.is_empty()
        && tls.as_ref().is_none_or(|tls|!tls.options.ech_config_list.contains("://")
            && !zero_transport::tls::PreparedClient::validate_profile(tls.as_ref()).is_err_and(|error|error.kind()==std::io::ErrorKind::Unsupported)))
}
impl Options {
    pub(crate) fn bind_node_dns(&mut self, dependency: zero_core::component::ComponentId) {
        if let Self::Grpc(options) = self {
            *options = Arc::new((**options).clone_with_node_dns(dependency));
        }
    }
}
pub(crate) fn is_domain(input: &OutboundConfig) -> bool {
    matches!(&input.protocol, OutboundProtocolConfig::Vmess { server, grpc: Some(_), mux_concurrency: None, .. } if zero_core::address::parse_ip_literal(server).is_none())
}
pub(crate) fn selected(
    input: &OutboundConfig,
    candidate: Option<&zero_config::RuntimeConfig>,
    native: bool,
) -> bool {
    eligible(input)
        && (!is_domain(input)
            || (native
                && candidate.is_some_and(|c| {
                    zero_dns::native::validate_node_config(c.runtime.dns.as_ref()).is_ok()
                })))
}
impl super::super::VmessAdapter {
    #[cfg(test)]
    pub(crate) fn standard_options(&self, input: &OutboundConfig) -> Result<Options, ConfigError> {
        self.standard_options_for(input, None)
    }
    pub(crate) fn standard_options_for(
        &self,
        input: &OutboundConfig,
        candidate: Option<&zero_config::RuntimeConfig>,
    ) -> Result<Options, ConfigError> {
        let OutboundProtocolConfig::Vmess {
            server, port, grpc, ..
        } = &input.protocol
        else {
            unreachable!("owning protocol projection")
        };
        if grpc.is_none() {
            return super::component_options(input).and_then(|options| {
                Ok(Options::Ordinary(Arc::new(stateless::Options::new(
                    options,
                    zero_core::request::Destination::new(
                        match zero_core::address::parse_ip_literal(server) {
                            Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                            Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                            None => zero_core::Address::Domain(server.clone()),
                        },
                        *port,
                    ),
                    input.dial.to_policy(),
                    NonZeroUsize::new(
                        candidate
                            .map(|config| config.runtime.effective_native_stream_max_originals())
                            .unwrap_or(4),
                    )
                    .filter(|limit| limit.get() <= 4096)
                    .ok_or_else(|| {
                        ConfigError::InvalidRuntime("invalid taskless Stream original limit".into())
                    })?,
                ))))
            });
        }
        let refs = super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
            .expect("owning protocol projection");
        // Same ceiling as the existing active+retiring Datagram host admissions.
        // Neither the task limit nor native limit allocates idle sockets/tasks.
        ::vmess::component::grpc::Options::from_node_options_refs_with_streams_at(
            refs,
            input.dial.to_policy(),
            candidate.and_then(|config| config.source_dir()),
            NonZeroUsize::new(
                candidate
                    .map(|config| config.runtime.effective_file_read_max_bytes())
                    .unwrap_or_else(|| {
                        zero_config::RuntimeOptionsConfig::default().effective_file_read_max_bytes()
                    }),
            ),
            NonZeroUsize::new(
                candidate
                    .map(|config| config.runtime.effective_vmess_grpc_max_originals())
                    .unwrap_or(4096),
            )
            .filter(|limit| limit.get() <= 4096)
            .ok_or_else(|| {
                ConfigError::InvalidRuntime("invalid actual VMess gRPC original limit".into())
            })?,
            Duration::from_secs(5),
            &self.runtime,
            streams_selected(input, candidate, self.native_node_dns),
        )
        .map(|options| Options::Grpc(Arc::new(options)))
        .map_err(|source| {
            ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })
    }
}
