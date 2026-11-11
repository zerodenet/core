//! One accepted upload/download recipe, with its actual material handles.
mod cold;
use super::super::*;
use crate::transport::{OwnedXhttpHalf, OwnedXhttpKind, VlessOutboundLeaf, VlessTransportRuntime};
use zero_traits::DialPolicy;
use zero_transport::{profile::OwnedSplitHttpProfile, split_http::XhttpClientPool};
pub struct Policy<'a> {
    pub runtime: &'a VlessTransportRuntime,
    pub dial: DialPolicy,
    pub origins: NonZeroUsize,
    pub users: NonZeroUsize,
    pub limits: zero_platform_tokio::controlled_datagram::metadata::MetadataLimits,
    pub source_dir: Option<&'a std::path::Path>,
    pub file_bytes: NonZeroUsize,
    pub buffers: zero_platform_tokio::controlled_carrier::CarrierBufferLimits,
}
pub struct Options {
    pub(super) upload: Half,
    pub(super) download: Option<Half>,
    pub(super) plan: crate::transport::OwnedVlessOutboundTransportPlan,
    pub(super) origins: NonZeroUsize,
    pub(super) identity: Arc<[u8]>,
    pub(super) dependencies: Vec<zero_core::component::ComponentId>,
    recipe: Arc<[u8]>,
    dimensions: [usize; 4],
}
pub(super) enum Half {
    Quic(Arc<super::super::Options>),
    Tcp(Arc<TcpOptions>),
}
pub(super) struct TcpOptions {
    pub protocol: crate::component::Options,
    material_key: Arc<[u8]>,
    pub plan: crate::transport::OwnedVlessOutboundTransportPlan,
    pub pool: XhttpClientPool,
    pub http: OwnedSplitHttpProfile,
    pub version: zero_transport::split_http::TcpVersion,
    raw_masks: Option<zero_transport::finalmask::Settings>,
    pub origins: NonZeroUsize,
    pub buffers: zero_platform_tokio::controlled_carrier::CarrierBufferLimits,
}
impl Options {
    pub fn with_owned_mask_settings(
        mut self,
        settings: zero_transport::finalmask::Settings,
    ) -> Self {
        let mut recipe = self.recipe.to_vec();
        recipe.extend_from_slice(b"actual-host-mask-material.v1");
        recipe.extend_from_slice(&zero_transport::finalmask::Profile::settings_identity(
            &settings,
        ));
        self.recipe = recipe.into();
        match &mut self.upload {
            Half::Quic(options) => {
                // Still the one cold, unshared owning value. Material handles
                // may be shared; no runtime/Scope or physical IO exists yet.
                let options = Arc::get_mut(options).expect("cold actual upload options");
                options.raw_masks = Some(settings);
            }
            Half::Tcp(options) => {
                Arc::get_mut(options)
                    .expect("cold actual upload options")
                    .raw_masks = Some(settings);
            }
        }
        // Old independent download never inherited the upload FinalMask.
        // Still materialize its genuinely empty profile in the actual Host,
        // rather than using the explicit-library prepared-material branch.
        if let Some(download) = &mut self.download {
            match download {
                Half::Quic(options) => {
                    Arc::get_mut(options)
                        .expect("cold actual download options")
                        .raw_masks = Some(Default::default())
                }
                Half::Tcp(options) => {
                    Arc::get_mut(options)
                        .expect("cold actual download options")
                        .raw_masks = Some(Default::default())
                }
            }
        }
        self.refresh_identity();
        self
    }
    pub fn from_accepted_leaf(
        leaf: &VlessOutboundLeaf,
        policy: Policy<'_>,
    ) -> Result<Option<Self>, zero_transport::RuntimeError> {
        let Some(recipe) = leaf.owned_xhttp_recipe() else {
            return Ok(None);
        };
        let upload = Half::prepare(
            recipe.halves.upload,
            recipe.protocol,
            recipe.identity,
            &policy,
            None,
        )?;
        let download = recipe
            .halves
            .download
            .map(|half| {
                Half::prepare(
                    half,
                    recipe.protocol,
                    recipe.identity,
                    &policy,
                    Some(&upload),
                )
            })
            .transpose()?;
        let cold_recipe: Arc<[u8]> = recipe.identity.to_vec().into();
        let mut identity = recipe.identity.to_vec();
        identity.extend_from_slice(b"vless.actual-owned-xhttp-pair.v1");
        identity.extend_from_slice(upload.identity());
        if let Some(download) = &download {
            identity.extend_from_slice(download.identity());
        }
        let endpoint_users = if matches!(&upload, Half::Quic(_))
            || download
                .as_ref()
                .is_some_and(|half| matches!(half, Half::Quic(_)))
        {
            policy.users.get()
        } else {
            0
        };
        let mut prepared = Self {
            upload,
            download,
            plan: recipe.plan,
            origins: policy.origins,
            identity: identity.into(),
            dependencies: Vec::new(),
            recipe: cold_recipe,
            dimensions: [
                policy.origins.get(),
                endpoint_users,
                policy.buffers.max_queue_packets,
                policy.buffers.total_max_bytes,
            ],
        };
        prepared.refresh_identity();
        Ok(Some(prepared))
    }
}
impl Half {
    fn prepare(
        half: OwnedXhttpHalf,
        protocol: ([u8; 16], Option<&'static str>, [u32; 4]),
        identity: [u8; 32],
        policy: &Policy<'_>,
        previous: Option<&Self>,
    ) -> Result<Self, zero_transport::RuntimeError> {
        let OwnedXhttpHalf {
            plan,
            endpoint,
            kind,
            tls,
            reality,
            pool,
            profile,
        } = half;
        if let OwnedXhttpKind::Http3(quic) = kind {
            let options = super::super::Options::from_material_parts(
                crate::transport::OwnedQuicParts {
                    plan,
                    profile: quic,
                    endpoint,
                    protocol,
                    identity,
                    h3: Some((pool, profile)),
                },
                policy.runtime,
                policy.dial.clone(),
                policy.origins,
                policy.users,
                policy.limits,
                policy.source_dir,
                policy.file_bytes,
            )?
            .with_buffer_policy(policy.buffers)?;
            return Ok(Self::Quic(Arc::new(options)));
        }
        let version = match kind {
            OwnedXhttpKind::Http1 => zero_transport::split_http::TcpVersion::Http1,
            OwnedXhttpKind::Http2 => zero_transport::split_http::TcpVersion::Http2 {
                keepalive: profile.options.xmux.h_keep_alive_period,
            },
            OwnedXhttpKind::Http3(_) => unreachable!(),
        };
        let server = match &endpoint.address {
            zero_core::Address::Ipv4(ip) => std::net::Ipv4Addr::from(*ip).to_string(),
            zero_core::Address::Ipv6(ip) => std::net::Ipv6Addr::from(*ip).to_string(),
            zero_core::Address::Domain(name) => name.clone(),
        };
        let prepared_reality = reality
            .as_ref()
            .map(|profile| {
                crate::reality::PreparedRealityClient::from_options(
                    crate::reality::RealityClientOptions {
                        spider_x: &profile.spider_x,
                        hybrid_key_exchange: profile.hybrid_key_exchange,
                        mldsa65_verify: profile.mldsa65_verify.as_deref(),
                        public_key: &profile.public_key,
                        short_id: &profile.short_id,
                        server_name: profile.server_name.as_deref().unwrap_or(&server),
                        cipher_suites: &profile.cipher_suites,
                        client_fingerprint: &profile.client_fingerprint,
                    },
                )
                .map_err(std::io::Error::other)
            })
            .transpose()?;
        // This is only a process-local material recipe key, never a grant or
        // proof of scope/role/ownership. Different TLS halves must not collide
        // in the old one-profile Census just because their Slot is shared.
        use std::hash::{BuildHasher, Hasher};
        static KEYS: std::sync::OnceLock<std::collections::hash_map::RandomState> =
            std::sync::OnceLock::new();
        let mut hash = KEYS.get_or_init(Default::default).build_hasher();
        struct RecipeWriter<'a>(&'a mut dyn Hasher);
        impl std::fmt::Write for RecipeWriter<'_> {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                self.0.write(value.as_bytes());
                Ok(())
            }
        }
        use std::fmt::Write;
        write!(
            &mut RecipeWriter(&mut hash),
            "{:?}{:?}{:?}{:?}",
            endpoint,
            tls,
            reality,
            policy.dial
        )
        .expect("hasher write cannot fail");
        let mut key = identity.to_vec();
        key.extend_from_slice(b"xhttp.actual-tcp-material.v1");
        key.extend_from_slice(&hash.finish().to_be_bytes());
        let material_key: Arc<[u8]> = key.into();
        let mut protocol = crate::component::Options {
            material: None,
            endpoint,
            server,
            uuid: protocol.0,
            flow: protocol.1,
            testseed: protocol.2,
            reality: prepared_reality,
            // The enclosing owned transport plan applies Encryption once.
            encryption: None,
            tls,
            ws: None,
            grpc: None,
            h2: None,
            http_upgrade: None,
            dial: policy.dial.clone(),
            identity: material_key.clone(),
        };
        // An identical actual TLS material is prepared/read once for this
        // accepted pair and strongly shared. Other halves own their own real
        // direct handle; no ambiguous Registry.bound(instance) is used.
        if let Some(Self::Tcp(previous)) = previous.filter(|previous| matches!(previous, Self::Tcp(previous) if previous.material_key == material_key)) {
            protocol.material = previous.protocol.material.clone();
            protocol.identity = previous.protocol.identity.clone();
        } else {
            protocol = protocol.with_owned_material(policy.runtime, policy.source_dir, policy.file_bytes)?;
        }
        Ok(Self::Tcp(Arc::new(TcpOptions {
            protocol,
            material_key,
            plan,
            pool,
            http: profile,
            version,
            raw_masks: None,
            origins: policy.origins,
            buffers: policy.buffers,
        })))
    }
    fn identity(&self) -> &[u8] {
        match self {
            Self::Quic(options) => &options.identity,
            Self::Tcp(options) => &options.protocol.identity,
        }
    }
}

impl TcpOptions {
    pub(super) fn prepare_masks(
        &self,
        book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        memory: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> std::io::Result<zero_transport::finalmask::Profile> {
        match &self.raw_masks {
            Some(raw) => zero_transport::finalmask::Profile::new_owned_from_ref(raw, book, memory),
            None => Ok(self.plan.final_mask_profile().clone()),
        }
    }
}
