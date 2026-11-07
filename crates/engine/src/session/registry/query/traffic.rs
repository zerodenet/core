//! Borrow every original role field once; never recapture between scope rows.
use super::*;
struct Row<'a> {
    original: &'a ActiveSessionEntry,
    outbound: MutexGuard<'a, Option<String>>,
    path: MutexGuard<'a, FlowPathObservation>,
}
pub(crate) struct SessionTrafficRows<'a>(Vec<Row<'a>>);
impl SessionQueryGuard<'_> {
    pub(crate) fn traffic_rows(
        &self,
        workspace: &mut ControlBudgetPermit,
    ) -> std::io::Result<SessionTrafficRows<'_>> {
        let mut rows = workspace::vector::<Row<'_>>(self.0.sessions.len(), workspace)?;
        for entry in self.0.sessions.values() {
            rows.push(Row {
                original: entry,
                outbound: entry
                    .outbound_tag
                    .lock()
                    .expect("outbound tag lock poisoned"),
                path: entry.path.lock().expect("session path lock poisoned"),
            });
        }
        Ok(SessionTrafficRows(rows))
    }
}
impl SessionTrafficRows<'_> {
    pub(crate) fn activity(
        &self,
        scope: &zero_api::TrafficScope,
        roles: &[zero_config::EndpointRoleBindingRef<'_>],
    ) -> (u64, u64) {
        let mut streams = 0;
        let mut datagrams = 0;
        for row in &self.0 {
            let entry = row.original;
            let outbound = &row.outbound;
            let path = &row.path;
            let matches = match scope {
                zero_api::TrafficScope::Global => true,
                zero_api::TrafficScope::Inbound { tag } => {
                    entry.inbound_tag.as_deref() == Some(tag.as_str())
                }
                zero_api::TrafficScope::Outbound { tag } => {
                    outbound.as_deref() == Some(tag.as_str())
                        || path.relay_chain.iter().any(|(actual, _)| actual == tag)
                }
                zero_api::TrafficScope::Endpoint { endpoint_id } => roles
                    .iter()
                    .filter(|role| role.matches_endpoint(endpoint_id))
                    .any(|role| {
                        entry
                            .inbound_tag
                            .as_deref()
                            .is_some_and(|tag| role.matches_role(true, tag))
                            || outbound
                                .as_deref()
                                .is_some_and(|tag| role.matches_role(false, tag))
                            || path
                                .relay_chain
                                .iter()
                                .any(|(tag, _)| role.matches_role(false, tag))
                    }),
                zero_api::TrafficScope::Peer { .. }
                | zero_api::TrafficScope::HostInterface { .. } => false,
            };
            if matches {
                match entry.network {
                    Network::Tcp => streams += 1,
                    Network::Udp => datagrams += 1,
                }
            }
        }
        (streams, datagrams)
    }
}

impl SessionTrafficRows<'_> {
    pub(crate) fn endpoint_activity(
        &self,
        binding: &zero_config::EndpointBindingConfig,
    ) -> (u64, u64) {
        let (mut streams, mut datagrams) = (0, 0);
        for row in &self.0 {
            let matches = row
                .original
                .inbound_tag
                .as_ref()
                .is_some_and(|tag| binding.inbound_tags.contains(tag))
                || row
                    .outbound
                    .as_ref()
                    .is_some_and(|tag| binding.outbound_tags.contains(tag))
                || row
                    .path
                    .relay_chain
                    .iter()
                    .any(|(tag, _)| binding.outbound_tags.contains(tag));
            if matches {
                match row.original.network {
                    Network::Tcp => streams += 1,
                    Network::Udp => datagrams += 1,
                }
            }
        }
        (streams, datagrams)
    }
}
