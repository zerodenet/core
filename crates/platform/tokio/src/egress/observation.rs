//! Borrowed metadata of this exact selector; no selection/IO/restore rights.
use super::*;
pub struct EgressReadObservation<'a>(std::sync::RwLockReadGuard<'a, EgressInterfaces>);
impl EgressInterfaceControl {
    pub fn read_observation(&self) -> EgressReadObservation<'_> {
        EgressReadObservation(self.0.read().expect("egress interface lock poisoned"))
    }
}
impl EgressReadObservation<'_> {
    pub fn interface(&self, ipv6: bool) -> Option<&EgressInterface> {
        if ipv6 {
            self.0.ipv6.as_ref()
        } else {
            self.0.ipv4.as_ref()
        }
    }
    pub fn unavailable_reason(&self, ipv6: bool) -> Option<&str> {
        if ipv6 {
            self.0.ipv6_unavailable_reason.as_deref()
        } else {
            self.0.ipv4_unavailable_reason.as_deref()
        }
    }
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    pub fn ipv6_to_ipv4_fallbacks(&self) -> u64 {
        self.0.ipv6_to_ipv4_fallbacks
    }
}

#[cfg(test)]
mod tests;
