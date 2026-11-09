//! Actual plain-QUIC owning recipe; other real carrier selections stay explicit.
use super::*;
use crate::component::{
    background::{BackgroundCarrier, BackgroundNetwork},
    RegistrationError, ResourcePreparation,
};
use std::{num::NonZeroUsize, sync::Arc};
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};
pub(crate) struct Options {
    pub inner: Arc<::vless::component::quic::Options>,
    tag: String,
    network: Arc<BackgroundNetwork>,
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ::vless::component::quic::Factory.describe(options.inner.as_ref())
    }
    fn build(
        &self,
        options: Arc<Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        context.background_carrier = Some(
            BackgroundCarrier {
                network: options.network.clone(),
                instance,
                initial: UpstreamBinding {
                    prefix: vec![],
                    dial: options.inner.dial().clone(),
                    network_generation: NetworkGeneration(options.network.egress.generation()),
                },
                endpoint: options.inner.endpoint().clone(),
                wire_endpoint: options.inner.endpoint().clone(),
                tag: options.tag.clone(),
            }
            .transport(),
        );
        ::vless::component::quic::Factory.build_with_ech_resolver(
            options.inner.clone(),
            context,
            Some(crate::component::background::grpc::tls_resolver(
                options.network.clone(),
                instance,
            )),
        )
    }
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    matches!(&input.protocol,OutboundProtocolConfig::Vless {quic:Some(_),grpc:None,split_http:None,mkcp:None,hysteria:None,testpre:0,mux_concurrency:None,xudp_concurrency:None,reverse_tag:None,reverse_sniffing:None,ws,..} if ws.as_ref().is_none_or(|ws|ws.browser_dialer.is_none()))
}
impl super::super::VlessAdapter {
    pub(crate) fn quic_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
    ) -> Result<super::mux::Options, RegistrationError> {
        let OutboundProtocolConfig::Vless {
            server,
            port,
            quic: Some(quic),
            ..
        } = &input.protocol
        else {
            unreachable!("same actual QUIC selection")
        };
        let config = resources
            .candidate()
            .map(|config| config.runtime.clone())
            .unwrap_or_default();
        let refs = super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
            .expect("same VLESS projection");
        let leaf = ::vless::transport::VlessOutboundLeaf::from_options_refs(
            resources.candidate().and_then(|config| config.source_dir()),
            refs,
            &self.runtime,
        )
        .map_err(|source| {
            ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?;
        let bound = |value| {
            NonZeroUsize::new(value)
                .ok_or_else(|| ConfigError::InvalidRuntime("zero actual QUIC dimension".into()))
        };
        let limits = zero_platform_tokio::controlled_datagram::metadata::MetadataLimits {
            main_sockets: bound(1)?,
            probe_sockets: bound(config.effective_metadata_max_probe_sockets(2))?,
            probe_attempts_per_initialization: bound(
                config.effective_metadata_max_probe_attempts(8),
            )?,
            concurrent_send_calls: bound(config.effective_metadata_max_send_calls(2))?,
            write_pollers: bound(2)?,
        };
        let mut options = ::vless::component::quic::Options::from_accepted_leaf(
            &leaf,
            &self.runtime,
            input.dial.to_policy(),
            bound(config.effective_native_stream_max_originals())?,
            bound(config.effective_quic_shared_endpoint_max_users())?,
            limits,
            resources.candidate().and_then(|config| config.source_dir()),
            bound(config.effective_file_read_max_bytes())?,
        )
        .map_err(|source| {
            ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?
        .ok_or_else(|| {
            ConfigError::InvalidOutbound(format!(
                "outbound `{}` actual earlier carrier differs from plain QUIC",
                input.tag
            ))
        })?;
        options = options
            .with_buffer_policy(
                zero_platform_tokio::controlled_carrier::CarrierBufferLimits {
                    max_queue_packets: config.effective_quic_max_queue_packets(),
                    total_max_bytes: config.effective_quic_total_max_buffered_bytes(),
                },
            )
            .map_err(|source| ConfigError::InvalidRuntime(source.to_string()))?;
        let ech = zero_traits::ClientTlsProfile::tls_options(quic.as_ref())
            .ech
            .source()
            .map_err(|source| ConfigError::InvalidOutbound(source.into()))?;
        if zero_core::address::parse_ip_literal(server).is_none()
            || matches!(ech, Some(zero_traits::EchConfigSource::Dns { .. }))
        {
            let node = crate::component::native_dns::prepare_node(resources, network.clone())?;
            options = options.with_node_dns(node);
        }
        if resources.same_source_as_previous() {
            if let Some(previous) = resources.previous_named_instance(&input.tag) {
                let inherited = options
                    .reuse_file_material_from(previous)
                    .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
                if !inherited {
                    if let Some(identity) = resources.previous_resource_identity(previous.component)
                    {
                        options
                            .reuse_cold_file_material_from(identity)
                            .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
                    }
                }
            }
        }
        options
            .remember_cold_file_material()
            .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
        Ok(super::mux::Options::Quic(Arc::new(Options {
            inner: Arc::new(options),
            tag: input.tag.clone(),
            network,
        })))
    }
}
