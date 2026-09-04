/// Why a prepared TCP outbound is being executed.
///
/// The intent is mandatory at the dispatch boundary so control-plane probes
/// cannot accidentally inherit data-plane health side effects.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TcpDispatchIntent {
    /// User/data-plane traffic participates in the shared outbound circuit
    /// breaker.
    Traffic,
    /// Policy-owned probes continue to respect an active traffic quarantine,
    /// but apply their success or failure only through the explicit policy
    /// result path.
    PolicyProbe,
    /// Manual diagnostics actively test the outbound without consulting or
    /// mutating the shared traffic-health state.
    DiagnosticProbe,
    /// DNS detours must remain able to recover an outbound whose ordinary
    /// traffic is quarantined. Their outcome is transport evidence for the
    /// DNS fallback chain, not data-plane circuit-breaker evidence.
    DnsDetour,
}

impl TcpDispatchIntent {
    pub(super) const fn checks_outbound_health(self) -> bool {
        !matches!(self, Self::DiagnosticProbe | Self::DnsDetour)
    }

    pub(super) const fn records_outbound_health(self) -> bool {
        matches!(self, Self::Traffic)
    }
}
