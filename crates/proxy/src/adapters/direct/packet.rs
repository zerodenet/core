//! Built-in Direct projection into the neutral host packet executor.
use super::DirectAdapter;
use crate::{
    protocol_registry::{
        ClaimedPacketLeaf, OutboundDeviceLifecycleCapability, OutboundDevicePreparationContext,
        PreparedOutboundDeviceState,
    },
    runtime::packet_route::{host::HostPacketDevice, PreparedPacketRouteOperation},
};
use std::sync::Arc;
pub(super) struct PacketLeaf(pub(super) Arc<HostPacketDevice>);
impl ClaimedPacketLeaf for PacketLeaf {
    fn prepare_packet_route(&self) -> Box<dyn PreparedPacketRouteOperation> {
        Box::new(self.0.clone())
    }
}
struct Prepared {
    owner: Arc<std::sync::Mutex<Option<Arc<HostPacketDevice>>>>,
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
impl PreparedOutboundDeviceState for Prepared {
    fn publish(
        mut self: Box<Self>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let old = std::mem::replace(
            &mut *self.owner.lock().unwrap_or_else(|e| e.into_inner()),
            self.next.take(),
        );
        let retired = old.filter(|old| {
            self.owner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .is_none_or(|new| !Arc::ptr_eq(old, new))
        });
        if let Some(old) = &retired {
            old.close();
        }
        let next = self.owner.lock().unwrap_or_else(|e| e.into_inner()).clone();
        self.published = true;
        Box::pin(async move {
            if let Some(old) = retired {
                old.wait_stopped().await;
            }
            if let Some(next) = next {
                next.activate();
            }
        })
    }
}
#[async_trait::async_trait]
impl OutboundDeviceLifecycleCapability for DirectAdapter {
    async fn prepare_outbound_devices(
        &self,
        _: &[&zero_config::OutboundConfig],
        _: &[zero_config::InboundConfig],
        context: OutboundDevicePreparationContext,
    ) -> Result<Box<dyn PreparedOutboundDeviceState>, zero_engine::EngineError> {
        let current = self
            .packet
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let (next, fresh) = if let Some(config) = context.direct_packet_device {
            if let Some(current) = current
                .filter(|d| d.config == config && d.mtu == usize::from(context.mtu) && d.usable())
            {
                (Some(current), false)
            } else {
                (Some(prepare_host_device(config, context.mtu)?), true)
            }
        } else {
            (None, false)
        };
        Ok(Box::new(Prepared {
            owner: self.packet.clone(),
            next,
            fresh,
            published: false,
        }))
    }
    fn shutdown_outbound_devices(&self) -> crate::protocol_registry::OutboundDeviceCompletion {
        let device = self.packet.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(device) = &device {
            device.close();
        }
        Box::pin(async move {
            if let Some(device) = device {
                device.wait_stopped().await;
            }
        })
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
                return Ok(HostPacketDevice::start(device, config, mtu));
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
