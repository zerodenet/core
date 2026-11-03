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
                Some(config) => Some(prepare_host_device(config, context.mtu)?),
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
        self.packet.lock().unwrap_or_else(|e| e.into_inner()).take();
        Ok(())
    }
}

fn prepare_host_device(
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
                let device = zero_tun::adopt(fd, &config.interface)?;
                return Ok(HostPacketDevice::start_owned(device, config, mtu));
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
