//! Project every existing HY2 outbound into one actual shared authenticated pool.
use super::*;
use crate::component::background::{BackgroundCarrier, BackgroundNetwork};
use crate::component::{RegistrationError, ResourcePreparation};
use ::hysteria2::transport::owned::{PoolFactory, PoolOptions, RoleOptions};
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hasher},
    sync::{Arc, OnceLock},
};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};
pub(crate) struct Factory {
    network: Arc<BackgroundNetwork>,
}
pub(crate) struct NativePoolOptions {
    pool: Arc<PoolOptions>,
    dns: Option<zero_core::component::ComponentId>,
}
impl Factory {
    pub(crate) fn new(network: Arc<BackgroundNetwork>) -> Self {
        Self { network }
    }
}
impl ComponentFactory for Factory {
    type Options = NativePoolOptions;
    fn describe(&self, options: &NativePoolOptions) -> ComponentSpec {
        let mut spec = PoolFactory.describe(&options.pool);
        spec.dependencies.extend(options.dns);
        spec
    }
    fn build(
        &self,
        options: Arc<NativePoolOptions>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        if context.dependencies.len() != usize::from(options.dns.is_some())
            || options
                .dns
                .is_some_and(|dns| context.dependency(dns).is_none())
        {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        context.dependencies = Arc::from([]);
        let upstreams = options
            .pool
            .profiles()
            .iter()
            .map(|profile| {
                BackgroundCarrier {
                    network: self.network.clone(),
                    instance: context.instance(),
                    initial: profile.binding().clone(),
                    endpoint: profile.endpoint().clone(),
                    wire_endpoint: profile.wire_endpoint(),
                    tag: profile.tag().into(),
                }
                .transport()
            })
            .collect();
        PoolFactory.build_with_upstreams(options.pool.clone(), context, upstreams)
    }
}
impl Hysteria2Adapter {
    pub(crate) fn outbound_options(
        &self,
        input: &zero_config::OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        factory: Arc<Factory>,
    ) -> Result<RoleOptions, RegistrationError> {
        let config = resources
            .candidate()
            .ok_or_else(|| options_error("complete Hysteria2 candidate is required"))?;
        let index = config
            .outbounds
            .iter()
            .filter(|outbound| {
                matches!(&outbound.protocol, OutboundProtocolConfig::Hysteria2 { .. })
            })
            .position(|outbound| outbound.tag() == input.tag())
            .ok_or_else(|| options_error("expected Hysteria2 outbound profile"))?;
        let prepared = resources.resources().find(|resource| {
            let spec = resource.recipe().spec();
            spec.roles.is_empty() && matches!(&spec.owner,
                zero_platform_tokio::contract::component::ResourceOwner::Endpoint(name) if name.as_ref() == "hysteria2.authenticated_pool")
        }).map(|resource|resource.component());
        if let Some(owner) = prepared {
            let mut bytes = serde_json::to_vec(input).map_err(options_error)?;
            if let Some(source) = resources.source_dir() {
                bytes.extend(source.as_os_str().as_encoded_bytes());
            }
            return Ok(self.outbound_pool.bind_prepared_profile(
                index,
                owner,
                identity(&bytes, &[]),
            ));
        }
        let profiles = config
            .outbounds
            .iter()
            .filter(|outbound| {
                matches!(&outbound.protocol, OutboundProtocolConfig::Hysteria2 { .. })
            })
            .map(|outbound| {
                let mut bytes = serde_json::to_vec(outbound).map_err(options_error)?;
                if let Some(source) = resources.source_dir() {
                    bytes.extend(source.as_os_str().as_encoded_bytes());
                }
                Ok(pool_profile(
                    outbound.tag(),
                    &outbound.protocol,
                    UpstreamBinding {
                        prefix: vec![],
                        dial: outbound.dial.to_policy(),
                        network_generation: zero_core::component::NetworkGeneration(
                            factory.network.egress.generation(),
                        ),
                    },
                    identity(&bytes, &[]),
                )
                .expect("selected Hysteria2 protocol")
                .with_source_dir(resources.source_dir()))
            })
            .collect::<Result<Vec<_>, RegistrationError>>()?;
        let mut bytes = Vec::new();
        bytes.extend((Arc::as_ptr(&self.outbound_pool) as usize).to_le_bytes());
        let mut files = std::collections::BTreeMap::new();
        let mut materials = Vec::with_capacity(profiles.len());
        for profile in &profiles {
            let material = match profile.ca_cert_path() {
                Some(path) => {
                    use std::collections::btree_map::Entry;
                    let material = match files.entry(path) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            let bytes = zero_runtime::configuration::ConfigurationSource::read_preparation_file(
                                entry.key(), config.runtime.effective_file_read_max_bytes(),
                            ).map_err(|source| RegistrationError::Planning(
                                zero_platform_tokio::contract::ChannelError::new(
                                    zero_platform_tokio::contract::Stage::Prepare,
                                    zero_platform_tokio::contract::Reason::Io,
                                    zero_platform_tokio::contract::Impact::Component,
                                ).with_source(source)
                            ))?;
                            entry.insert(Arc::<[u8]>::from(bytes))
                        }
                    };
                    Some(material.clone())
                }
                None => None,
            };
            materials.push(material);
        }
        for outbound in &config.outbounds {
            if matches!(&outbound.protocol, OutboundProtocolConfig::Hysteria2 { .. }) {
                bytes.extend(serde_json::to_vec(outbound).map_err(options_error)?);
            }
        }
        if let Some(source) = resources.source_dir() {
            bytes.extend(source.as_os_str().as_encoded_bytes());
        }
        let options = Arc::new(
            self.outbound_pool
                .options(profiles, identity(&bytes, &materials))
                .with_prepared_ca(materials)
                .map_err(RegistrationError::Planning)?,
        );
        let dns = options
            .profiles()
            .iter()
            .any(|profile| matches!(profile.endpoint().address, zero_core::Address::Domain(_)))
            .then(|| crate::component::native_dns::prepare_node(resources, factory.network.clone()))
            .transpose()?;
        let owner = resources.share(
            "hysteria2.authenticated_pool",
            factory,
            Arc::new(NativePoolOptions {
                pool: options.clone(),
                dns,
            }),
        )?;
        Ok(options.bind(index, owner))
    }
}
fn identity(bytes: &[u8], materials: &[Option<Arc<[u8]>>]) -> Arc<[u8]> {
    static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
    KEYS.get_or_init(|| [RandomState::new(), RandomState::new()])
        .iter()
        .flat_map(|key| {
            let mut hash = key.build_hasher();
            hash.write(&(bytes.len() as u64).to_le_bytes());
            hash.write(bytes);
            for material in materials {
                match material {
                    Some(material) => {
                        hash.write(&[1]);
                        hash.write(&(material.len() as u64).to_le_bytes());
                        hash.write(material);
                    }
                    None => hash.write(&[0]),
                }
            }
            hash.finish().to_le_bytes()
        })
        .collect::<Vec<_>>()
        .into()
}
fn options_error(error: impl std::fmt::Display) -> RegistrationError {
    RegistrationError::Options(zero_config::ConfigError::InvalidOutbound(error.to_string()))
}
