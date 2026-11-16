//! Cold preparation cannot publish credentials into the active listener.
use super::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
pub(crate) struct ProfileUpdate {
    profiles: Arc<Mutex<HashMap<String, crate::inbound::VlessInboundProfile>>>,
    tag: String,
    candidate: crate::inbound::VlessInboundProfile,
}
impl ProfileUpdate {
    pub(crate) fn new(
        profiles: Arc<Mutex<HashMap<String, crate::inbound::VlessInboundProfile>>>,
        tag: &str,
        candidate: crate::inbound::VlessInboundProfile,
    ) -> Self {
        Self {
            profiles,
            tag: tag.into(),
            candidate,
        }
    }
    fn commit(self) -> crate::inbound::VlessInboundProfile {
        let mut profiles = self.profiles.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(current) = profiles.get(&self.tag) {
            current.publish_validated_users(&self.candidate);
            current.clone()
        } else {
            profiles.insert(self.tag, self.candidate.clone());
            self.candidate
        }
    }
}
pub struct PreparedVlessInboundListenerRequest {
    request: VlessInboundListenerRequest,
    update: ProfileUpdate,
}
impl VlessInboundListenerRequest {
    pub fn prepare_publication(
        self,
        runtime: &super::super::VlessTransportRuntime,
        tag: &str,
    ) -> PreparedVlessInboundListenerRequest {
        let update = runtime.stage_inbound_profile(tag, self.profile.clone());
        PreparedVlessInboundListenerRequest {
            request: self,
            update,
        }
    }
}
impl VlessInboundListenerRequest {
    pub(crate) fn bind_host_masks(
        mut self,
        resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
    ) -> Result<Self, zero_platform_tokio::contract::ChannelError> {
        use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
        let fail = |source: std::io::Error| {
            ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                .with_source(source)
        };
        if let Some(settings) = self.mask_recipe.take() {
            self.transport.final_mask = if settings.tcp.is_empty() {
                // Existing UDP source decorators keep their own original
                // Metadata/grant preparation. This adds no TCP work or task.
                zero_transport::finalmask::Profile::new(settings).map_err(fail)?
            } else {
                let book = resources.carrier_buffer_budget().ok_or_else(|| {
                    fail(std::io::Error::other(
                        "inbound TCP masks require the actual Host buffer book",
                    ))
                })?;
                zero_transport::finalmask::Profile::new_owned(
                    settings,
                    book,
                    resources.packet_driver_metadata()?,
                )
                .map_err(fail)?
            };
        } else if !self.transport.final_mask.tcp().is_empty() {
            let book = resources.carrier_buffer_budget().ok_or_else(|| {
                fail(std::io::Error::other(
                    "retained inbound TCP masks have no original Host book",
                ))
            })?;
            if !self
                .transport
                .final_mask
                .same_owned_budget(&book, &resources.packet_driver_metadata()?)
            {
                return Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::StalePlan,
                    Impact::Component,
                ));
            }
        }
        Ok(self)
    }
}
impl VlessInboundListenerRequest {
    pub(crate) fn prepare_decryption_maintenance(
        &self,
        cancel: zero_platform_tokio::contract::CancellationToken,
    ) -> Result<
        Option<crate::encryption::HostedMaintenance>,
        zero_platform_tokio::contract::ChannelError,
    > {
        self.transport
            .decryption
            .as_ref()
            .map(|server| server.prepare_hosted(cancel))
            .transpose()
    }
}
impl PreparedVlessInboundListenerRequest {
    pub(crate) fn bind_host_masks(
        mut self,
        resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
    ) -> Result<Self, zero_platform_tokio::contract::ChannelError> {
        self.request = self.request.bind_host_masks(resources)?;
        Ok(self)
    }
    pub(crate) fn session_tasks(&self) -> usize {
        if self.request.transport.split_http_registry.is_some() {
            zero_transport::split_http::SplitHttpRegistry::MAX_SESSIONS
        } else {
            0
        }
    }
    pub(crate) fn requires_decryption_maintenance(&self) -> bool {
        self.request
            .transport
            .decryption
            .as_ref()
            .is_some_and(|server| server.requires_maintenance())
    }
    pub(crate) fn target_endpoints(&self) -> Vec<zero_traits::FallbackEndpoint> {
        let mut endpoints = self.request.transport.target_endpoints();
        if let Some(endpoint) = self
            .request
            .hysteria
            .as_ref()
            .and_then(|profile| profile.target_endpoint())
        {
            if !endpoints.contains(&endpoint) {
                endpoints.push(endpoint);
            }
        }
        endpoints
    }
    pub(crate) fn file_root(&self) -> Option<std::path::PathBuf> {
        self.request
            .hysteria
            .as_ref()
            .and_then(|profile| profile.file_root())
    }
    pub(crate) fn masks(&self) -> Vec<zero_transport::finalmask::udp::Mask> {
        self.request.mask_recipe.as_ref().map_or_else(
            || self.request.transport.final_mask.udp().to_vec(),
            |settings| settings.udp.clone(),
        )
    }
    pub(crate) fn uses_mkcp(&self) -> bool {
        self.request.uses_mkcp()
    }
    pub(crate) fn validate_standard_quic_source(
        &self,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
        if self.request.uses_mkcp() {
            return Err(
                ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component)
                    .with_source(std::io::Error::other(
                        "carrier requires additional original native providers",
                    )),
            );
        }
        Ok(())
    }
    pub(crate) fn validate_standard_source(
        &self,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
        if self.request.uses_hysteria() {
            return Err(
                ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component)
                    .with_source(std::io::Error::other(
                        "native/fallback source ownership is not yet integrated",
                    )),
            );
        }
        if self.request.uses_mkcp() {
            zero_transport::finalmask::owned::validate(&self.masks()).map_err(|source| {
                ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component)
                    .with_source(source)
            })?;
        }
        Ok(())
    }
    /// Called by the installed factory after confirmed StopOld, never during decode.
    pub fn activate(self) -> VlessInboundListenerRequest {
        self.request.with_profile(self.update.commit())
    }
}
