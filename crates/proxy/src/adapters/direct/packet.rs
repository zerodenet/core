//! Built-in Direct projection into the neutral host packet executor.
use super::DirectAdapter;
use crate::{
    adapters::devices::OutboundDevicePreparationContext,
    runtime::packet_route::host::HostPacketDevice,
};
use std::sync::Arc;
pub(crate) struct Prepared {
    next: Option<Arc<HostPacketDevice>>,
    fresh: bool,
    published: bool,
}
impl Drop for Prepared {
    fn drop(&mut self) {
        if self.fresh && !self.published {
            if let Some(device) = &self.next {
                device.close();
            }
        }
    }
}
impl Prepared {
    pub(crate) fn publish(mut self) {
        if let Some(next) = &self.next {
            next.activate();
        }
        self.published = true;
    }
}
impl DirectAdapter {
    pub(crate) async fn prepare_devices(
        &self,
        context: OutboundDevicePreparationContext,
    ) -> Result<Prepared, zero_engine::EngineError> {
        let current = self
            .packet
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let reusable = current.filter(|d| {
            context.direct_packet_device.as_ref() == Some(&d.config)
                && d.mtu == usize::from(context.mtu)
                && d.usable()
        });
        let fresh = reusable.is_none() && context.direct_packet_device.is_some();
        let next = if reusable.is_some() {
            reusable
        } else {
            self.close_devices().await?;
            match context.direct_packet_device {
                Some(config) => Some(self.prepare_host_device(config, context.mtu)?),
                None => None,
            }
        };
        // Retain the real original even if this prepared value is discarded.
        *self.packet.lock().unwrap_or_else(|e| e.into_inner()) = next.clone();
        Ok(Prepared {
            next,
            fresh,
            published: false,
        })
    }
    pub(crate) fn request_device_shutdown(&self) {
        if let Some(device) = self
            .packet
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            device.close();
        }
    }
    pub(crate) async fn close_devices(&self) -> Result<(), zero_engine::EngineError> {
        self.request_device_shutdown();
        let device = self
            .packet
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(device) = device {
            if device.config.backend == zero_config::DirectPacketDeviceBackend::Wintun {
                // Existing Windows host-session backend has no original native
                // receipt. Keep its explicit compatibility join, never qualify
                // it as a native device/provider merely by deleting the SPI.
                device.wait_workers_stopped().await?;
            } else {
                device.wait_closed().await?;
            }
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        self.close_descriptor_original()?;
        self.packet.lock().unwrap_or_else(|e| e.into_inner()).take();
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn close_descriptor_original(&self) -> Result<(), zero_engine::EngineError> {
        let original = self
            .packet_original
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(original) = original {
            match original.outcome() {
                zero_tun::TunAcquisitionOutcome::KnownNoNative
                | zero_tun::TunAcquisitionOutcome::Closed(_) => {}
                zero_tun::TunAcquisitionOutcome::Unknown(_) => {
                    original.close().map_err(|source| {
                        std::io::Error::new(source.kind(), SharedAcquisitionError(source))
                    })?;
                }
            }
            self.packet_original
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
        }
        Ok(())
    }

    fn prepare_host_device(
        &self,
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
    ) -> Result<Arc<HostPacketDevice>, zero_engine::EngineError> {
        match config.backend {
            zero_config::DirectPacketDeviceBackend::Descriptor => {
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                {
                    let fd = config.fd.ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Direct packet descriptor is missing",
                        )
                    })?;
                    let original = zero_tun::AdoptedTunOwner::new();
                    // Install this exact native owner before the duplication call,
                    // validation, registration or first driver can fail.
                    *self
                        .packet_original
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = Some(original.clone());
                    let device = original
                        .acquire(fd, &config.interface)
                        .map_err(|error| std::io::Error::new(error.source.kind(), error))?;
                    return Ok(HostPacketDevice::start_adopted(
                        device, original, config, mtu,
                    ));
                }
            }
            zero_config::DirectPacketDeviceBackend::Wintun => {
                #[cfg(target_os = "windows")]
                {
                    let device = zero_tun::ExistingWindowsTun::open(
                        &config.interface,
                        &config.router_addresses,
                        mtu,
                    )?;
                    return Ok(HostPacketDevice::start(device, config, mtu));
                }
            }
        }
        #[allow(unused_variables)]
        let _ = (config, mtu);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Direct PacketSink host binding backend unsupported on this platform",
        )
        .into())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug)]
struct SharedAcquisitionError(Arc<std::io::Error>);
#[cfg(any(target_os = "linux", target_os = "macos"))]
impl std::fmt::Display for SharedAcquisitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
impl std::error::Error for SharedAcquisitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
