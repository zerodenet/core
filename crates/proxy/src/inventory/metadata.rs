use zero_api::ProtocolCapability;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_engine::EngineError;

use super::ProtocolInventory;

impl ProtocolInventory {
    /// Public endpoint facts only. Metadata consumers must not construct a
    /// protocol claim, parse private material or enter its connection pool.
    pub(crate) fn outbound_endpoint(
        &self,
        snapshot: &zero_engine::EngineRuntimeSnapshot,
        leaf: &zero_engine::ResolvedLeafOutbound<'_>,
    ) -> Result<Option<(String, u16)>, EngineError> {
        let zero_engine::ResolvedLeafOutbound::Proxy { identity } = leaf else {
            return Ok(None);
        };
        let outbound = snapshot
            .config()
            .outbounds
            .get(identity.config_index())
            .ok_or_else(|| EngineError::InvalidPlan {
                message: "resolved endpoint is outside the accepted configuration".into(),
            })?;
        if let Some((reason, endpoint)) =
            zero_engine::EndpointAdmission::from_snapshot(snapshot).outbound_denial(outbound.tag())
        {
            return Err(EngineError::InvalidPlan {
                message: format!("{reason}: {endpoint}"),
            });
        }
        Ok(outbound
            .protocol
            .endpoint()
            .map(|(host, port)| (host.to_owned(), port)))
    }

    pub fn supported_inbounds(&self) -> Vec<&'static str> {
        self.registry.inbound_names()
    }

    pub fn supported_outbounds(&self) -> Vec<&'static str> {
        self.registry.outbound_names()
    }

    pub fn protocol_capabilities(&self) -> Vec<ProtocolCapability> {
        self.registry.capabilities()
    }
    pub(crate) fn control_capability_descriptors(
        &self,
        workspace: &mut zero_runtime::configuration::ControlBudgetPermit,
    ) -> std::io::Result<Vec<zero_traits::ProtocolCapabilityDescriptor>> {
        self.registry.control_capability_descriptors(workspace)
    }

    pub fn validate_config(&self, config: &RuntimeConfig) -> Result<(), EngineError> {
        if let Some(device) = &config.runtime.network.direct_packet_device {
            let supported = cfg!(feature = "raw-ip-runtime")
                && match device.backend {
                    zero_config::DirectPacketDeviceBackend::Descriptor => {
                        cfg!(any(target_os = "linux", target_os = "macos"))
                    }
                    zero_config::DirectPacketDeviceBackend::Wintun => cfg!(target_os = "windows"),
                };
            if !supported {
                let message = "Direct PacketSink requires raw-ip-runtime and a host binding backend supported on this platform";
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    message,
                )));
            }
        }
        let inbounds: Vec<_> = config
            .inbounds
            .iter()
            .cloned()
            .map(|mut inbound| {
                inbound.udp.enabled &= config.runtime.udp.enabled;
                inbound
            })
            .collect();
        self.registry.validate_inbounds(&inbounds)?;
        self.registry.validate_outbounds(&config.outbounds)?;
        Ok(())
    }

    pub(crate) fn validate_dial_environment(
        &self,
        config: &RuntimeConfig,
    ) -> Result<(), EngineError> {
        for outbound in &config.outbounds {
            zero_platform_tokio::validate_dial_policy(&outbound.dial.to_policy()).map_err(
                |error| {
                    EngineError::Io(std::io::Error::new(
                        error.kind(),
                        format!("outbound `{}` dial validation: {error}", outbound.tag),
                    ))
                },
            )?;
        }
        Ok(())
    }

    pub fn supports_inbound_protocol(&self, protocol: &InboundProtocolConfig) -> bool {
        self.registry.supports_inbound(protocol)
    }

    pub fn supports_outbound_protocol(&self, protocol: &OutboundProtocolConfig) -> bool {
        self.registry.supports_outbound(protocol)
    }
}
