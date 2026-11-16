//! Explicit preservation of the existing native TLS/WS compatibility semantics.
use super::*;
impl Options {
    pub fn from_native_browser_options_refs<TTls, TWs, TGrpc>(
        input: VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        source_dir: Option<&Path>,
        ca_max_bytes: Option<NonZeroUsize>,
        owned: bool,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        // Retain the full WS/TLS value in the existing identity and executor.
        // The explicit owning adapter selected native compatibility; a failed
        // Browser Source never enters this constructor.
        Self::from_options_refs_inner(input, dial, source_dir, ca_max_bytes, owned, false, true)
    }
}
