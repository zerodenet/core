use super::*;
use serde::ser::{SerializeSeq, SerializeStruct};
use zero_config::{InboundConfig, OutboundConfig};
pub(super) struct Config<'v, 's>(pub &'v EngineConfigQuery<'s>);
impl Serialize for Config<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let config = self.0.snapshot.config();
        #[derive(Serialize)]
        struct Mode<'a> {
            kind: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            outbound: Option<&'a str>,
        }
        let mut out = serializer.serialize_struct("ConfigSnapshot", 6)?;
        out.serialize_field("config_revision", &self.0.revision)?;
        out.serialize_field(
            "mode",
            &Mode {
                kind: config.mode.kind(),
                outbound: config.mode.outbound(),
            },
        )?;
        out.serialize_field("rule_count", &config.route.rules.len())?;
        out.serialize_field("listeners", &Listeners(&config.inbounds))?;
        out.serialize_field("outbounds", &Outbounds(&config.outbounds))?;
        out.serialize_field("outbound_groups", &super::policy::Policies(self.0))?;
        out.end()
    }
}
struct Listeners<'a>(&'a [InboundConfig]);
impl Serialize for Listeners<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Listener<'a> {
            tag: &'a str,
            protocol: &'a str,
            listen_address: &'a str,
            listen_port: u16,
            udp_enabled: bool,
        }
        let mut out = serializer.serialize_seq(Some(self.0.len()))?;
        for inbound in self.0 {
            out.serialize_element(&Listener {
                tag: &inbound.tag,
                protocol: inbound.protocol.protocol_name(),
                listen_address: &inbound.listen.address,
                listen_port: inbound.listen.port,
                udp_enabled: inbound.udp.enabled,
            })?;
        }
        out.end()
    }
}
struct Outbounds<'a>(&'a [OutboundConfig]);
impl Serialize for Outbounds<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Outbound<'a> {
            tag: &'a str,
            protocol: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            server: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            port: Option<u16>,
            udp_enabled: bool,
        }
        let mut out = serializer.serialize_seq(Some(self.0.len()))?;
        for outbound in self.0 {
            let endpoint = outbound.protocol.endpoint();
            out.serialize_element(&Outbound {
                tag: &outbound.tag,
                protocol: outbound.protocol.protocol_name(),
                server: endpoint.map(|(host, _)| host),
                port: endpoint.map(|(_, port)| port),
                udp_enabled: outbound.udp.enabled,
            })?;
        }
        out.end()
    }
}
