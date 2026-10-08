use zero_api::ProtocolCapability;
use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_engine::EngineError;

use super::ProtocolInventory;

impl ProtocolInventory {
    pub fn supported_inbounds(&self) -> Vec<&'static str> {
        self.registry.inbound_names()
    }

    pub fn supported_outbounds(&self) -> Vec<&'static str> {
        self.registry.outbound_names()
    }

    pub fn protocol_capabilities(&self) -> Vec<ProtocolCapability> {
        self.registry.capabilities()
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

    pub fn supports_inbound_protocol(&self, protocol: &InboundProtocolConfig) -> bool {
        self.registry.supports_inbound(protocol)
    }

    pub fn supports_outbound_protocol(&self, protocol: &OutboundProtocolConfig) -> bool {
        self.registry.supports_outbound(protocol)
    }
}
