//! Actual packet-carrier recipe; mKCP and QUIC share host original ownership.
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
    matches!(
        &input.protocol,
        OutboundProtocolConfig::Vless {
            flow,
            quic: Some(_),
            hysteria: Some(_),
            grpc: None,
            split_http: None,
            mkcp: None,
            testpre: 0,
            mux_concurrency: None,
            xudp_concurrency: None,

            ..
        } if flow.as_deref().is_none_or(|value|value=="zero-aead-v1")
    ) || matches!(&input.protocol,OutboundProtocolConfig::Vless {flow,quic:Some(_),grpc:None,split_http,mkcp:None,hysteria:None,testpre:0,mux_concurrency:None,xudp_concurrency:None,ws,..}
        if flow.as_deref().is_none_or(|value|value=="zero-aead-v1") && ws.as_ref().is_none_or(|ws|ws.browser_dialer.is_none())
        && split_http.as_ref().is_none_or(|http|http.browser_dialer.is_none() && http.download_settings.is_none()))
        || matches!(
            &input.protocol,
            OutboundProtocolConfig::Vless {
                flow,
                mkcp: Some(_),
                quic: None,
                hysteria: None,
                grpc: None,
                split_http: None,
                testpre: 0,
                mux_concurrency: None,
                xudp_concurrency: None,

                ..
            } if flow.as_deref().is_none_or(|value|value=="zero-aead-v1")
        )
}
pub(super) fn carrier_eligible(input: &OutboundConfig) -> bool {
    let mut carrier = input.clone();
    let OutboundProtocolConfig::Vless {
        testpre,
        mux_concurrency,
        xudp_concurrency,
        flow,
        split_http,
        ..
    } = &mut carrier.protocol
    else {
        return false;
    };
    if split_http.is_some() {
        return false;
    }
    *testpre = 0;
    *mux_concurrency = None;
    *xudp_concurrency = None;
    *flow = None;
    eligible(&carrier)
}
impl super::super::VlessAdapter {
    pub(crate) fn quic_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
    ) -> Result<super::mux::Options, RegistrationError> {
        Ok(super::mux::Options::Quic(
            self.packet_options(input, resources, network, false, None)?,
        ))
    }
    pub(super) fn packet_pool_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
        originals: NonZeroUsize,
    ) -> Result<
        (
            Arc<::vless::component::quic::Options>,
            zero_core::component::ComponentId,
        ),
        RegistrationError,
    > {
        let options = self.packet_options(input, resources, network, true, Some(originals))?;
        let inner = options.inner.clone();
        let owner = resources.share("vless.packet.actual_carrier", Arc::new(Factory), options)?;
        Ok((inner, owner))
    }
    fn packet_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
        raw: bool,
        originals: Option<NonZeroUsize>,
    ) -> Result<Arc<Options>, RegistrationError> {
        let OutboundProtocolConfig::Vless {
            server,
            port,
            quic,
            tls,
            ..
        } = &input.protocol
        else {
            unreachable!("same actual packet-carrier selection")
        };
        let config = resources
            .candidate()
            .map(|config| config.runtime.clone())
            .unwrap_or_default();
        let refs = super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
            .expect("same VLESS projection");
        let (leaf, masks) = ::vless::transport::VlessOutboundLeaf::from_owned_options_refs(
            resources.candidate().and_then(|config| config.source_dir()),
            refs,
            &self.runtime,
        )
        .map_err(|source| {
            ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?;
        let bound = |value| {
            NonZeroUsize::new(value).ok_or_else(|| {
                ConfigError::InvalidRuntime("zero actual packet-carrier dimension".into())
            })
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
            originals.unwrap_or(bound(config.effective_native_stream_max_originals())?),
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
                "outbound `{}` actual earlier carrier differs from the packet recipe",
                input.tag
            ))
        })?;
        options = options.with_owned_mask_settings(masks);
        options = options
            .with_buffer_policy(
                zero_platform_tokio::controlled_carrier::CarrierBufferLimits {
                    max_queue_packets: config.effective_quic_max_queue_packets(),
                    total_max_bytes: config.effective_quic_total_max_buffered_bytes(),
                },
            )
            .map_err(|source| ConfigError::InvalidRuntime(source.to_string()))?;
        let tls = match (quic, tls) {
            (Some(quic), _) => Some(zero_traits::ClientTlsProfile::tls_options(quic.as_ref())),
            (_, Some(tls)) => Some(zero_traits::ClientTlsProfile::tls_options(tls.as_ref())),
            _ => None,
        };
        let ech = tls
            .as_ref()
            .map(|tls| tls.ech.source())
            .transpose()
            .map_err(|source| ConfigError::InvalidOutbound(source.into()))?
            .flatten();
        if zero_core::address::parse_ip_literal(server).is_none()
            || matches!(ech, Some(zero_traits::EchConfigSource::Dns { .. }))
        {
            let node = crate::component::native_dns::prepare_node(resources, network.clone())?;
            options = options.with_node_dns(node);
        }
        if resources.same_source_as_previous() {
            for previous in resources.previous_named_resource_instances(&input.tag) {
                let mut inherited = options
                    .reuse_file_material_from(previous)
                    .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
                if !inherited {
                    if let Some(identity) = resources.previous_resource_identity(previous.component)
                    {
                        inherited = options
                            .reuse_cold_file_material_from(identity)
                            .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
                    }
                }
                if inherited {
                    break;
                }
            }
        }
        if raw {
            options = options.into_carrier_owner(&self.runtime);
        }
        options
            .remember_cold_file_material()
            .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
        Ok(Arc::new(Options {
            inner: Arc::new(options),
            tag: input.tag.clone(),
            network,
        }))
    }
}
