//! The same frozen scalars plus original borrowed fields, never a deep DTO copy.
use crate::{FlowFailureObservation, FlowPathObservation, FlowRouteObservation, SessionOutcome};
use zero_core::{
    Address, FakeIpReverseStatus, Network, ProtocolType, SessionAuth, TargetHostSource,
};
#[derive(Clone, Copy)]
pub(crate) struct Numbers {
    pub revision: u64,
    pub last_activity: u64,
    pub rx: u64,
    pub tx: u64,
    pub remote_rx: u64,
    pub remote_tx: u64,
    pub up_bps: u64,
    pub down_bps: u64,
    pub sampled_at: u64,
}
#[derive(Clone, Copy)]
pub(crate) struct End<'a> {
    pub finished_at: u64,
    pub duration: u64,
    pub outcome: SessionOutcome,
    pub close_reason: Option<&'a str>,
    pub failure: Option<&'a FlowFailureObservation>,
}
#[derive(Clone, Copy)]
pub(crate) struct Row<'a> {
    pub id: u64,
    pub inbound: Option<&'a str>,
    pub outbound: Option<&'a str>,
    pub route: Option<&'a FlowRouteObservation>,
    pub path: &'a FlowPathObservation,
    pub target: &'a Address,
    pub original_target: Option<&'a Address>,
    pub host_source: Option<TargetHostSource>,
    pub reverse_status: Option<FakeIpReverseStatus>,
    pub port: u16,
    pub protocol: ProtocolType,
    pub auth: Option<&'a SessionAuth>,
    pub network: Network,
    pub mode: &'a str,
    pub started_at: u64,
    pub numbers: Numbers,
    pub process_id: Option<u32>,
    pub process_name: Option<&'a str>,
    pub process_path: Option<&'a str>,
    pub sni: Option<&'a str>,
    pub source_ip: Option<&'a Address>,
    pub source_port: Option<u16>,
    pub end: Option<End<'a>>,
}
impl Row<'_> {
    pub(crate) fn bytes_up(&self) -> u64 {
        self.numbers.rx.max(self.numbers.remote_tx)
    }
    pub(crate) fn bytes_down(&self) -> u64 {
        self.numbers.remote_rx.max(self.numbers.tx)
    }
}
