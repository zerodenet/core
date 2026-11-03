use super::*;
use zero_transport::mkcp::{ListenerProfile, Settings};

impl VlessInboundListenerRequest {
    pub fn with_mkcp(self, settings: Option<Settings>) -> Result<Self, RuntimeError> {
        self.with_mkcp_masks(settings, &[])
    }
    pub fn with_mkcp_masks(
        mut self,
        settings: Option<Settings>,
        masks: &[zero_transport::finalmask::udp::Mask],
    ) -> Result<Self, RuntimeError> {
        self.mkcp = settings
            .map(|settings| ListenerProfile::new_with_masks(settings, masks))
            .transpose()?;
        Ok(self)
    }
    pub fn uses_mkcp(&self) -> bool {
        self.mkcp.is_some()
    }
}
