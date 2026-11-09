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
    RouteLockV4,
    RouteLockV6,
    RouteTransient,
    RouteUnlockV4,
    RouteUnlockV6,
    RouteTransientOperation,
    RouteLockAcquireV4,
    RouteLockAcquireV6,
}
pub(super) struct Failures {
    slots: [OnceLock<io::Error>; 32],
    controls: Option<[PacketMetadataArc<super::super::command::control::CommandControl>; 2]>,
}
impl Failures {
    pub(super) fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| OnceLock::new()),
            controls: None,
        }
    }
    pub(super) fn with_controls(
        controls: [PacketMetadataArc<super::super::command::control::CommandControl>; 2],
    ) -> Self {
        Self {
            controls: Some(controls),
            ..Self::new()
        }
    }
    pub(super) fn retain(&self, epoch: usize, phase: Phase, source: io::Error) -> bool {
        // The same source's already accepted delivery is not a new phase.
        // Storing it here would make payload -> Retained(payload) a strong
        // self-cycle. Inspect only this concrete private wrapper and exact
        // payload identity; foreign retained errors are still original causes.
        if source
            .get_ref()
            .and_then(|source| source.downcast_ref::<Retained>())
            .and_then(|source| source.0.as_error().downcast_ref::<Failures>())
            .is_some_and(|source| std::ptr::eq(source, self))
        {
            return false;
        }
        assert!(epoch < 2, "actual fixed helper epoch census");
        // Each index names the same real close actor, which itself caches its
        // single syscall outcome. A retry cannot introduce a different actor.
        let phase = phase as usize;
        let slot = if phase < 10 {
            epoch * 10 + phase
        } else if phase < Phase::RouteTransient as usize {
            20 + phase - 10
        } else if phase == Phase::RouteTransient as usize {
            24 + epoch
        } else if phase < Phase::RouteTransientOperation as usize {
            26 + phase - Phase::RouteUnlockV4 as usize
        } else if phase == Phase::RouteTransientOperation as usize {
            28 + epoch
        } else {
            30 + phase - Phase::RouteLockAcquireV4 as usize
        };
        self.slots[slot].set(source).is_ok()
    }
    pub(super) fn any(&self) -> bool {
        self.slots.iter().any(|source| source.get().is_some())
            || self
                .controls
                .as_ref()
                .is_some_and(|controls| controls.iter().any(|control| control.any_fault()))
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
        if let Some(controls) = &self.controls {
            for (epoch, control) in controls
                .iter()
                .enumerate()
                .filter(|(_, control)| control.any_fault())
            {
                write!(f, "; helper epoch {epoch} {}", &**control)?;
            }
        }
        Ok(())
    }
}
impl std::error::Error for Failures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.slots
            .iter()
            .find_map(OnceLock::get)
            .map(|source| source as &(dyn std::error::Error + 'static))
            .or_else(|| {
                self.controls
                    .as_ref()
                    .and_then(|controls| controls.iter().find_map(|control| control.first_fault()))
                    .map(|source| source as _)
            })
    }
}

/// A std IO boundary preserves the exact paid error allocation and header.
/// The no_std metadata owner stays opaque; no format/copy/new error Arc.
pub(super) fn retained(source: zero_traits::packet_metadata::PacketMetadataError) -> io::Error {
    io::Error::other(Retained(source))
}
#[derive(Debug)]
struct Retained(zero_traits::packet_metadata::PacketMetadataError);
impl fmt::Display for Retained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_error(), f)
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_error())
    }
}

#[cfg(test)]
#[path = "failures/tests.rs"]
mod tests;
