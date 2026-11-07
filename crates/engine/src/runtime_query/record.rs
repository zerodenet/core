//! The existing canonical FlowRecord shape over original retained field guards.
use super::{address::Host, row::Row};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize, Serializer,
};
use zero_core::Address;
#[derive(Serialize)]
struct Endpoint<'a> {
    tag: &'a str,
    protocol: &'a str,
}
#[derive(Serialize)]
struct Auth<'a> {
    scheme: &'a str,
    principal_key: Option<&'a str>,
    attributes: std::collections::BTreeMap<&'a str, &'a str>,
}
#[derive(Serialize)]
struct Source<'a> {
    ip: Option<Host<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    process_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    process_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    process_path: Option<&'a str>,
}
// An absent intercepted IP is the original empty string, not JSON null.
struct SourceView<'a>(Source<'a>);
impl Serialize for SourceView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let r = &self.0;
        let mut m = s.serialize_map(None)?;
        if let Some(ip) = r.ip {
            m.serialize_entry("ip", &ip)?;
        } else {
            m.serialize_entry("ip", "")?;
        }
        if let Some(v) = r.port {
            m.serialize_entry("port", &v)?;
        }
        if let Some(v) = r.process_id {
            m.serialize_entry("process_id", &v)?;
        }
        if let Some(v) = r.process_name {
            m.serialize_entry("process_name", v)?;
        }
        if let Some(v) = r.process_path {
            m.serialize_entry("process_path", v)?;
        }
        m.end()
    }
}
#[derive(Serialize)]
struct Target<'a> {
    host: Host<'a>,
    port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_ip: Option<Host<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    host_source: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fake_ip_reverse_status: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved_ip: Option<Resolved<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sniffed_host: Option<&'a str>,
}
#[derive(Serialize)]
#[serde(untagged)]
enum Resolved<'a> {
    Address(Host<'a>),
    Remote(&'a str),
}
struct Route<'a>(Row<'a>);
impl Serialize for Route<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if let Some(route) = self.0.route {
            return route.serialize(s);
        }
        let mut m = s.serialize_map(Some(3))?;
        m.serialize_entry("mode", self.0.mode)?;
        m.serialize_entry("action", "pending")?;
        m.serialize_entry("selection_chain", &[""; 0])?;
        m.end()
    }
}
struct Relays<'a>(&'a [(String, String)]);
impl Serialize for Relays<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for (tag, protocol) in self.0 {
            seq.serialize_element(&Endpoint { tag, protocol })?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct Path<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    outbound: Option<Endpoint<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote: Option<&'a crate::FlowRemoteEndpoint>,
    relay_chain: Relays<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    network: Option<&'a crate::FlowNetworkObservation>,
}
#[derive(Serialize)]
struct ResultView<'a> {
    outcome: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    close_reason: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<&'a crate::FlowFailureObservation>,
}
pub(super) struct Record<'a>(pub Row<'a>);
impl Serialize for Record<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let r = self.0;
        let n = r.numbers;
        let mut out = s.serialize_map(None)?;
        out.serialize_entry("flow_id", &Id(r.id))?;
        out.serialize_entry("revision", &n.revision)?;
        out.serialize_entry(
            "state",
            if r.end.is_some() {
                "completed"
            } else {
                "active"
            },
        )?;
        out.serialize_entry(
            "network",
            match r.network {
                zero_core::Network::Tcp => "tcp",
                zero_core::Network::Udp => "udp",
            },
        )?;
        out.serialize_entry(
            "inbound",
            &Endpoint {
                tag: r.inbound.unwrap_or_default(),
                protocol: r.protocol.as_str(),
            },
        )?;
        if let Some(auth) = r.auth {
            out.serialize_entry(
                "auth",
                &Auth {
                    scheme: &auth.scheme,
                    principal_key: auth.principal_key.as_deref(),
                    attributes: Default::default(),
                },
            )?;
        }
        if r.source_ip.is_some()
            || r.process_id.is_some()
            || r.process_name.is_some()
            || r.process_path.is_some()
        {
            out.serialize_entry(
                "source",
                &SourceView(Source {
                    ip: r.source_ip.map(Host),
                    port: r.source_port,
                    process_id: r.process_id,
                    process_name: r.process_name,
                    process_path: r.process_path,
                }),
            )?;
        }
        let resolved = match r.target {
            Address::Domain(_) if r.path.outbound_protocol.as_deref() == Some("direct") => r
                .path
                .remote
                .as_ref()
                .filter(|v| v.host.parse::<std::net::IpAddr>().is_ok())
                .map(|v| Resolved::Remote(&v.host)),
            Address::Domain(_) => None,
            _ => Some(Resolved::Address(Host(r.target))),
        };
        out.serialize_entry(
            "target",
            &Target {
                host: Host(r.target),
                port: r.port,
                original_ip: r.original_target.map(Host),
                host_source: r.host_source.map(|v| v.as_str()),
                fake_ip_reverse_status: r.reverse_status.map(|v| v.as_str()),
                resolved_ip: resolved,
                sniffed_host: r.sni,
            },
        )?;
        out.serialize_entry("route", &Route(r))?;
        out.serialize_entry(
            "path",
            &Path {
                outbound: r.outbound.map(|tag| Endpoint {
                    tag,
                    protocol: r.path.outbound_protocol.as_deref().unwrap_or("unknown"),
                }),
                remote: r.path.remote.as_ref(),
                relay_chain: Relays(&r.path.relay_chain),
                network: r.path.network.as_ref(),
            },
        )?;
        out.serialize_entry(
            "traffic",
            &zero_api::TrafficStats {
                bytes_up: r.bytes_up(),
                bytes_down: r.bytes_down(),
                inbound_rx_bytes: Some(n.rx),
                inbound_tx_bytes: Some(n.tx),
                outbound_rx_bytes: Some(n.remote_rx),
                outbound_tx_bytes: Some(n.remote_tx),
                packets_up: None,
                packets_down: None,
            },
        )?;
        out.serialize_entry(
            "throughput",
            &zero_api::FlowThroughput {
                upload_bps: n.up_bps,
                download_bps: n.down_bps,
                sampled_at_unix_ms: n.sampled_at,
            },
        )?;
        out.serialize_entry(
            "timing",
            &zero_api::FlowRecordTiming {
                started_at_unix_ms: r.started_at,
                last_activity_at_unix_ms: n.last_activity,
                ended_at_unix_ms: r.end.map(|e| e.finished_at),
                duration_ms: r.end.map(|e| e.duration),
            },
        )?;
        if let Some(end) = r.end {
            out.serialize_entry(
                "result",
                &ResultView {
                    outcome: outcome(end.outcome),
                    close_reason: end.close_reason,
                    failure: end.failure,
                },
            )?;
        }
        out.end()
    }
}
struct Id(u64);
impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl Serialize for Id {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}
pub(super) fn outcome(value: crate::SessionOutcome) -> &'static str {
    match value {
        crate::SessionOutcome::DirectRelayed => "direct_relayed",
        crate::SessionOutcome::ChainedRelayed => "chained_relayed",
        crate::SessionOutcome::Blocked => "blocked",
        crate::SessionOutcome::Failed => "failed",
        crate::SessionOutcome::Cancelled => "cancelled",
    }
}
