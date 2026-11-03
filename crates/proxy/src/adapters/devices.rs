//! Finite compiled device assembly, sharing the actual inbound/outbound owners.
//! No dynamic lifecycle SPI or protocol lookup lives in the runtime registry.
#[cfg(feature = "raw-ip-runtime")]
mod context;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) use context::{OutboundDevicePreparationContext, PreparedDevicePacketPath};
#[cfg(feature = "raw-ip-runtime")]
use std::sync::Arc;
#[cfg(feature = "raw-ip-runtime")]
use zero_config::RuntimeConfig;
#[cfg(feature = "raw-ip-runtime")]
use zero_engine::{EndpointAdmission, EngineError};

#[derive(Default)]
pub(crate) struct Devices {
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) direct: Option<Arc<super::direct::DirectAdapter>>,
    #[cfg(feature = "wireguard")]
    pub(crate) wireguard: Option<Arc<super::wireguard::WireguardAdapter>>,
}

#[cfg(feature = "raw-ip-runtime")]
pub(crate) struct PreparedDevices {
    direct: Option<super::direct::packet::Prepared>,
    #[cfg(feature = "wireguard")]
    wireguard: Option<super::wireguard::lifecycle::PreparedWireguardDevices>,
}
impl std::fmt::Debug for Devices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Devices").finish_non_exhaustive()
    }
}
#[cfg(feature = "raw-ip-runtime")]
impl Devices {
    pub(crate) async fn prepare(
        &self,
        config: &RuntimeConfig,
        context: OutboundDevicePreparationContext,
        admission: EndpointAdmission<'_>,
    ) -> Result<PreparedDevices, EngineError> {
        #[cfg(feature = "wireguard")]
        let wireguard = if let Some(owner) = &self.wireguard {
            let outbounds = config
                .outbounds
                .iter()
                .filter(|outbound| admission.device_enabled(&outbound.tag))
                .collect::<Vec<_>>();
            Some(
                owner
                    .prepare_devices(&outbounds, &config.inbounds, context.clone())
                    .await?,
            )
        } else {
            None
        };
        #[cfg(not(feature = "wireguard"))]
        let _ = (config, admission);
        let direct = if let Some(owner) = &self.direct {
            Some(owner.prepare_devices(context).await?)
        } else {
            None
        };
        Ok(PreparedDevices {
            direct,
            #[cfg(feature = "wireguard")]
            wireguard,
        })
    }
    pub(crate) fn health(
        &self,
        config: &RuntimeConfig,
    ) -> Vec<zero_api::OutboundDeviceHealthSnapshot> {
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            return owner.outbound_device_health(&config.outbounds.iter().collect::<Vec<_>>());
        }
        let _ = config;
        Vec::new()
    }
    pub(crate) fn request_shutdown(&self) {
        if let Some(owner) = &self.direct {
            owner.request_device_shutdown();
        }
        #[cfg(feature = "wireguard")]
        if let Some(owner) = &self.wireguard {
            owner.request_device_shutdown();
        }
    }
    pub(crate) async fn close(&self) -> Result<(), EngineError> {
        self.request_shutdown();
        #[cfg(feature = "wireguard")]
        let result = if let Some(owner) = &self.wireguard {
            owner.close_devices().await
        } else {
            Ok(())
        };
        #[cfg(not(feature = "wireguard"))]
        let result = Ok(());
        let direct = if let Some(owner) = &self.direct {
            owner.close_devices().await
        } else {
            Ok(())
        };
        if result.is_err() {
            if let Err(error) = &direct {
                tracing::warn!(%error, "additional original Direct device cleanup failure");
            }
        }
        result.and(direct)
    }
}
#[cfg(feature = "raw-ip-runtime")]
impl PreparedDevices {
    pub(crate) async fn publish(self) {
        #[cfg(feature = "wireguard")]
        if let Some(wireguard) = self.wireguard {
            wireguard.publish().await;
        }
        if let Some(direct) = self.direct {
            direct.publish();
        }
    }
}

// Inbound endpoint state belongs to these same finite concrete owners. This is
// static assembly glue, not a protocol listener capability or execution SPI.
impl Devices {
    pub(crate) fn prepare_inbound(
        &self,
        inbound: zero_config::InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<Option<crate::protocol_registry::PreparedInboundBind>, zero_engine::EngineError>
    {
        #[cfg(feature = "wireguard")]
        if let Some(adapter) = &self.wireguard {
            return adapter.prepare_inbound_bind(inbound, source_dir);
        }
        let _ = (inbound, source_dir);
        Ok(None)
    }
    pub(crate) fn inbound_requires_restart(&self, inbound: &zero_config::InboundConfig) -> bool {
        #[cfg(feature = "wireguard")]
        if let Some(adapter) = &self.wireguard {
            if zero_traits::ProtocolMetadata::descriptor(adapter.as_ref()).protocol
                == inbound.protocol.protocol_name()
            {
                return adapter.inbound_listener_requires_restart(inbound);
            }
        }
        let _ = inbound;
        false
    }
    pub(crate) fn update_inbound(
        &self,
        inbound: zero_config::InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<bool, zero_engine::EngineError> {
        #[cfg(feature = "wireguard")]
        if let Some(adapter) = &self.wireguard {
            if zero_traits::ProtocolMetadata::descriptor(adapter.as_ref()).protocol
                == inbound.protocol.protocol_name()
            {
                return adapter.update_inbound_listener(inbound, source_dir);
            }
        }
        let _ = (inbound, source_dir);
        Ok(false)
    }
}
