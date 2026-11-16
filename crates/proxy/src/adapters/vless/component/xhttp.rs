//! One accepted XHTTP upload/download value, with both real background sources.
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
    pub inner: Arc<::vless::component::quic::mixed::Options>,
    tag: String,
    network: Arc<BackgroundNetwork>,
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ::vless::component::quic::mixed::Factory.describe(options.inner.as_ref())
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        let source = |endpoint: &zero_core::request::Destination,
                      dial: &zero_traits::DialPolicy| {
            BackgroundCarrier {
                network: options.network.clone(),
                instance,
                initial: UpstreamBinding {
                    prefix: vec![],
                    dial: dial.clone(),
                    network_generation: NetworkGeneration(options.network.egress.generation()),
                },
                endpoint: endpoint.clone(),
                wire_endpoint: endpoint.clone(),
                tag: options.tag.clone(),
            }
            .transport()
        };
        let upload = source(options.inner.endpoint(), options.inner.dial());
        let download = options
            .inner
            .download_endpoint()
            .zip(options.inner.download_dial())
            .map(|(endpoint, dial)| source(endpoint, dial));
        ::vless::component::quic::mixed::Factory.build_with_upstreams(
            options.inner.clone(),
            context,
            upload,
            download,
            Some(crate::component::background::grpc::tls_resolver(
                options.network.clone(),
                instance,
            )),
        )
    }
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    matches!(&input.protocol, OutboundProtocolConfig::Vless {
        split_http:Some(http), grpc:None, mkcp:None, hysteria:None, ws:None,
        testpre:0, mux_concurrency:None, xudp_concurrency:None,   ..
    } if http.browser_dialer.is_none()
        && http.download_settings.as_ref().is_none_or(|down| down.split_http.browser_dialer.is_none()))
}
pub(super) fn carrier_eligible(input: &OutboundConfig) -> bool {
    let mut carrier = input.clone();
    let OutboundProtocolConfig::Vless {
        testpre,
        mux_concurrency,
        xudp_concurrency,
        ..
    } = &mut carrier.protocol
    else {
        return false;
    };
    *testpre = 0;
    *mux_concurrency = None;
    *xudp_concurrency = None;
    eligible(&carrier)
}
impl super::super::VlessAdapter {
    pub(super) fn xhttp_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
    ) -> Result<super::mux::Options, RegistrationError> {
        Ok(super::mux::Options::Xhttp(
            self.xhttp_recipe(input, resources, network, false, None)?,
        ))
    }
    pub(super) fn xhttp_pool_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
        originals: NonZeroUsize,
    ) -> Result<
        (
            Arc<::vless::component::quic::mixed::Options>,
            zero_core::component::ComponentId,
        ),
        RegistrationError,
    > {
        let options = self.xhttp_recipe(input, resources, network, true, Some(originals))?;
        let inner = options.inner.clone();
        let owner = resources.share("vless.xhttp.actual_carrier", Arc::new(Factory), options)?;
        Ok((inner, owner))
    }
    fn xhttp_recipe(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
        raw: bool,
        originals: Option<NonZeroUsize>,
    ) -> Result<Arc<Options>, RegistrationError> {
        let OutboundProtocolConfig::Vless { server, port, .. } = &input.protocol else {
            unreachable!("actual accepted XHTTP selection")
        };
        let config = resources
            .candidate()
            .map(|config| config.runtime.clone())
            .unwrap_or_default();
        let refs = super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
            .expect("same VLESS accepted projection");
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
                ConfigError::InvalidRuntime("zero actual XHTTP owner dimension".into())
            })
        };
        let mut options = ::vless::component::quic::mixed::Options::from_accepted_leaf(
            &leaf,
            ::vless::component::quic::mixed::Policy {
                runtime: &self.runtime,
                dial: input.dial.to_policy(),
                origins: originals
                    .unwrap_or(bound(config.effective_native_stream_max_originals())?),
                users: bound(config.effective_quic_shared_endpoint_max_users())?,
                limits: zero_platform_tokio::controlled_datagram::metadata::MetadataLimits {
                    main_sockets: bound(1)?,
                    probe_sockets: bound(config.effective_metadata_max_probe_sockets(2))?,
                    probe_attempts_per_initialization: bound(
                        config.effective_metadata_max_probe_attempts(8),
                    )?,
                    concurrent_send_calls: bound(config.effective_metadata_max_send_calls(2))?,
                    write_pollers: bound(2)?,
                },
                source_dir: resources.candidate().and_then(|config| config.source_dir()),
                file_bytes: bound(config.effective_file_read_max_bytes())?,
                buffers: zero_platform_tokio::controlled_carrier::CarrierBufferLimits {
                    max_queue_packets: config.effective_quic_max_queue_packets(),
                    total_max_bytes: config.effective_quic_total_max_buffered_bytes(),
                },
            },
        )
        .map_err(|source| {
            ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?
        .ok_or_else(|| {
            ConfigError::InvalidOutbound(format!(
                "outbound `{}` actual earlier carrier differs from accepted XHTTP",
                input.tag
            ))
        })?;
        options = options.with_owned_mask_settings(masks);
        if options.needs_node_dns() {
            options = options.with_node_dns(crate::component::native_dns::prepare_node(
                resources,
                network.clone(),
            )?);
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
