//! Built-in Direct projection into the neutral host packet executor.
pub(in crate::adapters::direct) mod standard;
use super::DirectAdapter;
use crate::runtime::packet_route::host::HostPacketDevice;
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
    pub(crate) fn publish_sync(&mut self) {
        if let Some(next) = &self.next {
            next.activate();
        }
    }
    pub(crate) fn confirm_publication(&mut self) {
        self.published = true;
    }
}
impl DirectAdapter {
    /// Called after the rejected Prepared guard was dropped. Its fresh device
    /// is fenced there, while a borrowed healthy published device stays live.
    pub(crate) async fn close_discarded_device(&self) -> Result<(), zero_engine::EngineError> {
        let healthy = self
            .packet
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|device| device.usable());
        if healthy {
            return Ok(());
        }
        // Also collect a failed native acquisition with no returned I/O view.
        self.close_devices().await
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
            device.wait_closed().await?;
        }
        self.packet.lock().unwrap_or_else(|e| e.into_inner()).take();
        Ok(())
    }
}

#[cfg(test)]
#[path = "packet/tests/publication.rs"]
mod publication;
