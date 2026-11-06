//! Cold production role selection over the protocol's actual gRPC owner.
mod material;
mod production;
use super::*;
use crate::component::background::stateless;
pub(crate) use production::{has_grpc_nodes, streams_selected, whole_streams_selected};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};
#[derive(Clone)]
pub(crate) enum Options {
    Owned(
        Arc<Options>,
        Arc<crate::component::background::BackgroundNetwork>,
    ),
    Ordinary(Arc<stateless::Options<::vmess::component::Options>>),
    Grpc(Arc<::vmess::component::grpc::Options>),
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        match options {
            Options::Owned(options, _) => self.describe(options),
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
        Self::build_resolved(options, context, None)
    }
}
impl Factory {
    fn build_resolved(
        options: Arc<Options>,
        context: BuildContext,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        match options.as_ref() {
            Options::Owned(options, network) => {
                let resolver = crate::component::background::grpc::tls_resolver(
                    network.clone(),
                    context.instance(),
                );
                Self::build_resolved(options.clone(), context, Some(resolver))
            }
            Options::Ordinary(options) => {
                stateless::Factory(material::Factory { resolver }).build(options.clone(), context)
            }
            Options::Grpc(options) => match resolver {
                Some(resolver) => ::vmess::component::grpc::Factory.build_with_ech_resolver(
                    options.clone(),
                    context,
                    resolver,
                ),
                None => ::vmess::component::grpc::Factory.build(options.clone(), context),
            },
        }
    }
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    if super::native_input(input) {
        return true;
    }
    matches!(&input.protocol,OutboundProtocolConfig::Vmess{server,mux_concurrency:None,grpc:Some(_),ws:None,tls,..}
        if !server.is_empty() && tls.as_ref().is_none_or(|tls|!tls.options.ech_config_list.contains("://")
            && !zero_transport::tls::PreparedClient::validate_profile(tls.as_ref()).is_err_and(|error|error.kind()==std::io::ErrorKind::Unsupported)))
}
pub(crate) fn owned_eligible(input: &OutboundConfig) -> bool {
    if super::owned_input(input) {
        return true;
    }
    matches!(&input.protocol,OutboundProtocolConfig::Vmess{server,mux_concurrency:None,grpc:Some(_),ws:None,tls,..}
        if !server.is_empty() && tls.as_ref().is_none_or(|tls|zero_transport::tls::OwnedPreparedClient::validate_profile(tls.as_ref()).is_ok()))
}

impl Options {
    pub(crate) fn bind_node_dns(&mut self, dependency: zero_core::component::ComponentId) {
        match self {
            Self::Owned(options, _) => Arc::make_mut(options).bind_node_dns(dependency),
            Self::Grpc(options) => *options = Arc::new((**options).clone_with_node_dns(dependency)),
            Self::Ordinary(options) => {
                *options = Arc::new((**options).clone().with_node_dns(dependency))
            }
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
    (if native {
        owned_eligible(input)
    } else {
        eligible(input)
    }) && (!is_domain(input)
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

pub(crate) fn taskless_domain(input: &OutboundConfig) -> bool {
    super::native_input(input)
        && matches!(&input.protocol,OutboundProtocolConfig::Vmess{server,..} if zero_core::address::parse_ip_literal(server).is_none())
}
