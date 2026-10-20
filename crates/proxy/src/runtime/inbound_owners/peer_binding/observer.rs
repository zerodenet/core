//! Read-only original join state, with no back-reference to the peer/resource graph.
use super::*;

/// Same original State/notification cell. Holding this does not hold Binding,
/// group, Registry, runtime, resources, cancellation authority or a Scope owner.
/// Only original Scope dispatch can construct the JoinedPeer it observes.
#[derive(Clone)]
pub(crate) struct HostPeerJoinObserver {
    parent: ComponentInstanceId,
    join: Arc<JoinCell>,
}
impl std::fmt::Debug for HostPeerJoinObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.join.state.lock().expect("peer binding poisoned");
        f.debug_struct("HostPeerJoinObserver")
            .field("parent", &self.parent)
            .field("task", &state.task)
            .field("joined", &state.joined.is_some())
            .finish_non_exhaustive()
    }
}
impl HostPeerBinding {
    pub(crate) fn join_observer(&self) -> HostPeerJoinObserver {
        HostPeerJoinObserver {
            parent: self.parent,
            join: self.join.clone(),
        }
    }
}
impl HostPeerJoinObserver {
    pub(crate) fn belongs_to(&self, binding: &Arc<HostPeerBinding>) -> bool {
        Arc::ptr_eq(&self.join, &binding.join)
    }
    pub(crate) fn observed_join(&self) -> Option<Arc<JoinedPeer>> {
        self.join
            .state
            .lock()
            .expect("peer binding poisoned")
            .joined
            .clone()
    }
    /// Same actual spawn_tracked witness, carrying only weak Scope provenance.
    /// Reading it never consumes an exit or acknowledges a response/native I/O.
    pub(crate) fn join_witness(
        &self,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoinWitness> {
        self.join
            .state
            .lock()
            .expect("peer binding poisoned")
            .witness
            .clone()
    }
    pub(crate) async fn wait_joined(
        &self,
        deadline: Instant,
    ) -> Result<Arc<JoinedPeer>, ChannelError> {
        tokio::time::timeout_at(deadline, async {
            loop {
                let changed = self.join.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let state = self.join.state.lock().expect("peer binding poisoned");
                    if let Some(joined) = &state.joined {
                        return Ok(joined.clone());
                    }
                    if let Some(error) = &state.rejected {
                        return Err(retained(error.clone()));
                    }
                }
                changed.await;
            }
        })
        .await
        .map_err(|source| {
            let mut error = ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                .with_source(source);
            error.component = Some(self.parent.component);
            error
        })?
    }
}

#[cfg(test)]
mod tests;
