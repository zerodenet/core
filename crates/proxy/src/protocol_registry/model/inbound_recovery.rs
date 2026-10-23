//! Protocol-owned behavior over actual passive state; no erased type recovery.
use super::PreparedInboundBind;
use zero_config::InboundConfig;
use zero_engine::EngineError;

pub(crate) trait InboundRecovery: Send + Sync {
    /// Called only after the host confirmed the exact old instance's task,
    /// native I/O and user obligations. The result is cold and grants no I/O.
    fn prepare_restore(
        &self,
        last_good: InboundConfig,
        source_dir: Option<&std::path::Path>,
    ) -> Result<PreparedInboundBind, EngineError>;
    /// A protocol may preserve its original security state for a compatible
    /// replacement only when its actual dependency bindings are safe to use.
    fn prepare_replacement(
        &self,
        _next: InboundConfig,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Option<PreparedInboundBind>, EngineError> {
        Ok(None)
    }
}
