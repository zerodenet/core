//! Two concrete helper epochs, each with ten independent actual close phases.
//! This payload has no Source/Scope/controller edge back to its owning Session.
use super::*;
use std::{fmt, sync::OnceLock};
#[derive(Clone, Copy)]
pub(super) enum Phase {
    Control,
    HelperExit,
    Authorization,
    Event,
    Listener,
    Unclaimed,
    ToolProgress,
    ToolClose,
    Shutdown,
    AuthorizationOutput,
    WakeRead,
    WakeWrite,
}
pub(super) struct Failures {
    slots: [OnceLock<io::Error>; 22],
}
impl Failures {
    pub(super) fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| OnceLock::new()),
        }
    }
    pub(super) fn retain(&self, epoch: usize, phase: Phase, source: io::Error) {
        assert!(epoch < 2, "actual fixed helper epoch census");
        // Each index names the same real close actor, which itself caches its
        // single syscall outcome. A retry cannot introduce a different actor.
        let phase = phase as usize;
        let slot = if phase < 10 {
            epoch * 10 + phase
        } else {
            20 + phase - 10
        };
        let _ = self.slots[slot].set(source);
    }
    pub(super) fn any(&self) -> bool {
        self.slots.iter().any(|source| source.get().is_some())
    }
}
impl fmt::Debug for Failures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for Failures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (phase, source) in self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(phase, slot)| slot.get().map(|source| (phase, source)))
        {
            if !first {
                f.write_str("; ")?;
            }
            first = false;
            write!(f, "helper original phase {phase}: {source}")?;
        }
        Ok(())
    }
}
impl std::error::Error for Failures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.slots
            .iter()
            .find_map(OnceLock::get)
            .map(|source| source as _)
    }
}
