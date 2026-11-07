//! Retire only clients that really hold the original instances being replaced.
use super::*;
use zero_core::component::ComponentInstanceId;
impl DriverOwner {
    pub(crate) fn retained_instances(&self) -> io::Result<Vec<ComponentInstanceId>> {
        let clients = self
            .state
            .lock()
            .expect("DoH driver owner poisoned")
            .native
            .clone();
        let mut instances = std::collections::BTreeSet::new();
        for client in clients {
            for instance in client.retained_instances()? {
                instances.insert(instance);
            }
        }
        Ok(instances.into_iter().collect())
    }
    pub(crate) fn source_tasks(&self) -> io::Result<Vec<crate::OwnedDnsQueryTasks>> {
        let provider = self
            .provider
            .lock()
            .expect("DoH original provider poisoned")
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| io::Error::other("actual retained DoH provider unavailable"))?;
        let (node, query) = provider
            .source_roots()
            .ok_or_else(|| io::Error::other("actual DoH source pair unavailable"))?;
        let tasks = provider.tasks();
        if !node.owns_tasks(&tasks) && !query.owns_tasks(&tasks) {
            return Err(io::Error::other(
                "actual canonical DoH provider no longer belongs to its source pair",
            ));
        }
        let mut result = Vec::new();
        for source in [node, query] {
            let Ok(current) = source.tasks() else {
                continue;
            };
            if !result
                .iter()
                .any(|old: &crate::OwnedDnsQueryTasks| old.same_owner(&current))
            {
                result.push(current);
            }
        }
        Ok(result)
    }
    pub(crate) async fn retire_instances(
        &self,
        replaced: &[ComponentInstanceId],
        end: Instant,
    ) -> io::Result<()> {
        let _opening = tokio::time::timeout_at(end, self.connect_lock.lock())
            .await
            .map_err(|source| crate::retirement_observation::deadline(source, ""))?;
        let candidates = self
            .state
            .lock()
            .expect("DoH driver owner poisoned")
            .native
            .clone();
        let mut selected = Vec::new();
        for client in candidates {
            let instances = client.retained_instances()?;
            if instances.iter().any(|actual| replaced.contains(actual)) {
                if client.leases.load(std::sync::atomic::Ordering::Acquire) != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "actual DoH query still holds its logical lease",
                    ));
                }
                selected.push(client);
            }
        }
        let retired = {
            let mut clients = tokio::time::timeout_at(end, self.clients.lock())
                .await
                .map_err(|source| crate::retirement_observation::deadline(source, ""))?;
            let mut retired = Vec::new();
            let mut index = 0;
            while index < clients.len() {
                if selected
                    .iter()
                    .any(|actual| clients[index].has_state(actual))
                {
                    retired.push(clients.remove(index));
                } else {
                    index += 1;
                }
            }
            retired
        };
        for client in &selected {
            client.retire();
        }
        // Drop senders and deliver their native/Scope wakes outside cache/state locks.
        drop(retired);
        self.drain_retired(end).await?;
        for client in selected {
            let native = client.native.get().expect("same original retained");
            native.settle(end).await?;
            let actual = native
                .task
                .get()
                .ok_or_else(|| io::Error::other("original DoH registration unavailable"))?;
            let joined = tokio::time::timeout_at(end, actual.wait_registration_released())
                .await
                .map_err(|source| crate::retirement_observation::deadline(source, ""))?;
            if !actual.matches_joined(&joined) || !native.retired() {
                return Err(io::Error::other(
                    "actual DoH original or registration ACK remains retained",
                ));
            }
            if let Err(source) = joined.result() {
                if !self.retained_failures_received() {
                    return Err(io::Error::other(source));
                }
            }
        }
        Ok(())
    }
}
