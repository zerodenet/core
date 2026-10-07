/// Why a prepared TCP outbound is being executed.
///
/// The intent is mandatory at the dispatch boundary so control-plane probes
/// cannot accidentally inherit data-plane health side effects.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TcpDispatchIntent {
    /// Business traffic records carrier observations for URLTest selection.
    Traffic,
    /// Policy-owned probes actively test candidates, including those in
    /// cooldown. Recovery is recorded after the probe response, not dialing.
    PolicyProbe,
    /// Manual diagnostics actively test the outbound without consulting or
    /// mutating the shared traffic-health state.
    DiagnosticProbe,
    /// DNS outcomes belong to the resolver's fallback chain and do not alter
    /// business-traffic carrier observations.
    DnsDetour,
}

impl TcpDispatchIntent {
    pub(super) const fn records_outbound_health(self) -> bool {
        matches!(self, Self::Traffic)
    }
}
