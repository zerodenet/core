//! Apply backpressure at request-group rotation while a completed finite POST
//! still owns its response. Streaming GET/POST bodies are never a drain gate.
use super::*;
impl XhttpClientPool {
    pub(super) async fn wait_rotating_responses(
        &self,
        selected: &GroupRef,
        owner: Option<&OwnerKey>,
    ) {
        loop {
            let returning = {
                let state = self.0.state.lock().unwrap();
                let Some(index) = state
                    .groups
                    .iter()
                    .position(|group| std::ptr::eq(&**group, &**selected))
                else {
                    return;
                };
                // Only predecessors can hold capacity for this rotation. A
                // later group may already be waiting on this very response.
                state.groups[..index]
                    .iter()
                    .find(|group| {
                        group.matches_owner(owner)
                            && !group.request_reusable()
                            && group.has_only_finite_responses()
                            && !group.connections.has_logical_work()
                    })
                    .cloned()
            };
            let Some(group) = returning else { return };
            let changed = group.connections.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if group.has_only_finite_responses() && !group.connections.has_logical_work() {
                changed.await;
            }
            // Body return permits cache retirement only. The actual Root still
            // supplies original native closure and Scope ACK before N refunds.
            self.reap();
        }
    }
}
