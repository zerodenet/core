//! Immutable recipe ownership; cold parsing grants no driver or native I/O.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use zero_core::Address;
impl NativePoolOptions {
    pub(super) fn prepare_recipe(
        pool: &GrpcPool,
        settings: NativePoolSettings,
        carrier_limit: NonZeroUsize,
        stream_limit: NonZeroUsize,
        background: bool,
    ) -> Result<Self, RuntimeError> {
        let invalid = || {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid native gRPC carrier options",
            )
        };
        if settings.port == 0 || stream_limit.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(invalid().into());
        }
        settings.dial.validate().map_err(|_| invalid())?;
        let ip = zero_core::address::parse_ip_literal(&settings.server);
        let address = match ip {
            Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            None if background && !settings.server.is_empty() => {
                Address::Domain(settings.server.clone())
            }
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "fixed native gRPC requires a literal direct endpoint",
                )
                .into())
            }
        };
        let endpoint = Destination::new(address, settings.port);
        if let Some(tls) = &settings.tls {
            if tls.ca_cert_path.is_some() && !background {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "native gRPC CA paths require a File owner",
                )
                .into());
            }
            if matches!(
                tls.options.ech.source().map_err(|_| invalid())?,
                Some(zero_traits::EchConfigSource::Dns { .. })
            ) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "native gRPC ECH DNS requires its service owner",
                )
                .into());
            }
            #[cfg(feature = "tls")]
            if background {
                crate::tls::OwnedPreparedClient::validate_profile(tls)?;
            } else {
                crate::tls::PreparedClient::validate_profile(tls)?;
            }
            #[cfg(not(feature = "tls"))]
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native gRPC TLS is not compiled",
            )
            .into());
        }
        let authority = settings
            .tls
            .as_ref()
            .and_then(|tls| tls.server_name.as_deref())
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                ip.map(|ip| std::net::SocketAddr::new(ip, settings.port).to_string())
                    .unwrap_or_else(|| settings.server.clone())
            });
        crate::grpc::options::request(&settings.grpc, &authority)?;
        let (identity, background_recipe) = if background {
            let mut recipes = pool.0.background_recipes.lock().unwrap();
            recipes.retain(|recipe| recipe.strong_count() != 0);
            let current = recipes.iter().filter_map(Weak::upgrade).find(|recipe| {
                recipe.settings == settings
                    && recipe.carriers == carrier_limit
                    && recipe.streams == stream_limit
            });
            let current = match current {
                Some(current) => current,
                None => {
                    let current = Arc::new(Recipe {
                        settings: settings.clone(),
                        carriers: carrier_limit,
                        streams: stream_limit,
                        identity: next_identity()?,
                        #[cfg(feature = "tls")]
                        materials: Default::default(),
                    });
                    recipes.try_reserve(1).map_err(std::io::Error::other)?;
                    recipes.push(Arc::downgrade(&current));
                    current
                }
            };
            (current.identity.clone(), Some(current))
        } else {
            let mut recipe = pool.0.recipe.lock().unwrap();
            let identity = match recipe.as_ref() {
                Some(recipe)
                    if recipe.settings == settings
                        && recipe.carriers == carrier_limit
                        && recipe.streams == stream_limit =>
                {
                    recipe.identity.clone()
                }
                Some(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "conflicting complete native gRPC recipe for the same actual pool",
                    )
                    .into())
                }
                None => {
                    let identity = next_identity()?;
                    *recipe = Some(Recipe {
                        settings: settings.clone(),
                        carriers: carrier_limit,
                        streams: stream_limit,
                        identity: identity.clone(),
                        #[cfg(feature = "tls")]
                        materials: Default::default(),
                    });
                    identity
                }
            };
            (identity, None)
        };
        Ok(Self {
            pool: Arc::downgrade(&pool.0),
            scoped_preparation: false,
            selected_background: false,
            settings,
            endpoint,
            authority,
            carrier_limit,
            stream_limit,
            _background_recipe: background_recipe,
            base_identity: identity.clone(),
            identity,
            totals: None,
            node_dns: None,
            ca: None,
        })
    }
}
fn next_identity() -> Result<Arc<[u8]>, RuntimeError> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
        old.checked_add(1)
    })
    .map(|value| Arc::from(value.to_le_bytes()))
    .map_err(|_| std::io::Error::other("native gRPC recipe identity exhausted").into())
}
