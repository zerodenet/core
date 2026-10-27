//! Project configured endpoint facts onto one actual compiled Datagram generation.
use crate::runtime::udp_flow::registered::UpstreamAssociationTarget;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

/// Host-resolved, secret-free identity for an existing standard Datagram role.
/// Never retain protocol configuration or a strong role/pool in a flow handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DatagramAssociationTarget {
    tag: String,
    server: String,
    port: u16,
    instance: zero_core::component::ComponentInstanceId,
}
impl DatagramAssociationTarget {
    pub(crate) fn resolve(
        execution: &crate::protocol_registry::TcpExecutionServices,
        tag: &str,
        server: &str,
        port: u16,
    ) -> Result<Self, zero_engine::EngineError> {
        let plan = execution.components.as_ref().map_err(|error| {
            zero_engine::EngineError::Io(std::io::Error::other(
                crate::component::instance::retained(error.clone()),
            ))
        })?;
        let slot = plan
            .configured(tag)
            .ok_or_else(|| invalid(Reason::Unsupported, "upstream Datagram role is unavailable"))?;
        let target = Self {
            tag: tag.into(),
            server: server.into(),
            port,
            instance: slot.instance_id(),
        };
        target.current_slot(execution)?;
        Ok(target)
    }
    pub(crate) fn current_slot(
        &self,
        execution: &crate::protocol_registry::TcpExecutionServices,
    ) -> Result<std::sync::Arc<crate::component::instance::ComponentSlot>, zero_engine::EngineError>
    {
        if let Some((reason, endpoint)) =
            zero_engine::EndpointAdmission::from_config(execution.config())
                .outbound_denial(&self.tag)
        {
            return Err(zero_engine::EngineError::InvalidPlan {
                message: format!("{reason}: {endpoint}"),
            });
        }
        let input = execution
            .config()
            .outbounds
            .iter()
            .find(|input| input.tag == self.tag)
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "upstream Datagram configuration was retired",
                )
            })?;
        if input.protocol.endpoint() != Some((&self.server, self.port)) {
            return Err(invalid(
                Reason::StalePlan,
                "upstream Datagram endpoint was retired",
            ));
        }
        let plan = execution.components.as_ref().map_err(|error| {
            zero_engine::EngineError::Io(std::io::Error::other(
                crate::component::instance::retained(error.clone()),
            ))
        })?;
        let slot = plan
            .configured(&self.tag)
            .ok_or_else(|| invalid(Reason::Unsupported, "upstream Datagram role is unavailable"))?;
        if slot.instance_id() != self.instance {
            return Err(invalid(
                Reason::StalePlan,
                "upstream Datagram generation was retired",
            ));
        }
        Ok(slot)
    }
}
impl UpstreamAssociationTarget for DatagramAssociationTarget {
    fn outbound_tag(&self) -> &str {
        &self.tag
    }
    fn log_parts(&self) -> (&str, &str, u16) {
        (&self.tag, &self.server, self.port)
    }
}
fn invalid(reason: Reason, message: &'static str) -> zero_engine::EngineError {
    let error = ChannelError::new(Stage::Prepare, reason, Impact::Session)
        .with_source(std::io::Error::other(message));
    zero_engine::EngineError::Io(std::io::Error::other(error))
}

#[cfg(test)]
#[path = "association/tests.rs"]
mod tests;
