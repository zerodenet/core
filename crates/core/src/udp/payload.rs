//! Existing independent-library cloning is an unreserved byte copy. Actual
//! Source-owned dispatches move once and are borrowed by the Kernel pipe; they
//! never call this library conversion or Vec-only into_parts boundary.
use super::*;
impl Clone for InboundUdpDispatch {
    fn clone(&self) -> Self {
        Self {
            peer_identity: self.peer_identity.clone(),
            target: self.target.clone(),
            route_target: self.route_target.clone(),
            sniffed_original_target: self.sniffed_original_target.clone(),
            sniffed_host_source: self.sniffed_host_source,
            skip_fake_ip_restore: self.skip_fake_ip_restore,
            port: self.port,
            payload: self.payload.as_ref().to_vec().into(),
            protocol: self.protocol,
            client_session_id: self.client_session_id,
            transparent_target: self.transparent_target,
            transparent_original_target: self.transparent_original_target.clone(),
            transparent_host_source: self.transparent_host_source,
        }
    }
}

#[cfg(test)]
mod tests;
