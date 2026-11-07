//! Borrow the legacy fields and the canonical record from one frozen row.
use super::{address::AddressView, record::Record, row::Row};
use serde::{ser::SerializeMap, Serialize, Serializer};
#[derive(Serialize)]
struct Auth<'a> {
    scheme: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    principal_key: Option<&'a str>,
}
pub(super) struct Legacy<'a> {
    pub row: Row<'a>,
    pub completed: bool,
}
impl Serialize for Legacy<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let r = self.row;
        let n = r.numbers;
        let mut out = s.serialize_map(None)?;
        out.serialize_entry("record", &Record(r))?;
        out.serialize_entry("id", &r.id)?;
        if let Some(value) = r.inbound {
            out.serialize_entry("inbound_tag", value)?;
        }
        if let Some(value) = r.outbound {
            out.serialize_entry("outbound_tag", value)?;
        }
        out.serialize_entry("target", &AddressView::new(r.target))?;
        out.serialize_entry("port", &r.port)?;
        out.serialize_entry("protocol", r.protocol.as_str())?;
        if let Some(a) = r.auth {
            out.serialize_entry(
                "auth",
                &Auth {
                    scheme: &a.scheme,
                    principal_key: a.principal_key.as_deref(),
                },
            )?;
        }
        out.serialize_entry(
            "network",
            match r.network {
                zero_core::Network::Tcp => "tcp",
                zero_core::Network::Udp => "udp",
            },
        )?;
        out.serialize_entry("mode", r.mode)?;
        out.serialize_entry("started_at_unix_ms", &r.started_at)?;
        out.serialize_entry("last_activity_at_unix_ms", &n.last_activity)?;
        out.serialize_entry("bytes_up", &r.bytes_up())?;
        out.serialize_entry("bytes_down", &r.bytes_down())?;
        out.serialize_entry("inbound_rx_bytes", &n.rx)?;
        out.serialize_entry("inbound_tx_bytes", &n.tx)?;
        out.serialize_entry("outbound_rx_bytes", &n.remote_rx)?;
        out.serialize_entry("outbound_tx_bytes", &n.remote_tx)?;
        if self.completed {
            let end = r
                .end
                .expect("completed query carries original completed row");
            out.serialize_entry("finished_at_unix_ms", &end.finished_at)?;
            out.serialize_entry("duration_ms", &end.duration)?;
            out.serialize_entry("outcome", super::record::outcome(end.outcome))?;
            if let Some(reason) = end.close_reason {
                out.serialize_entry("close_reason", reason)?;
            }
        } else {
            out.serialize_entry(
                "throughput_up_bps",
                &if r.end.is_some() { 0 } else { n.up_bps },
            )?;
            out.serialize_entry(
                "throughput_down_bps",
                &if r.end.is_some() { 0 } else { n.down_bps },
            )?;
        }
        if let Some(value) = r.process_id {
            out.serialize_entry("process_id", &value)?;
        }
        if let Some(value) = r.process_name {
            out.serialize_entry("process_name", value)?;
        }
        out.end()
    }
}
