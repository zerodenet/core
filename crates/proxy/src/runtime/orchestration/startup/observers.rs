//! Retire the previous generation before any new listener is prepared or bound.
use super::super::{lifecycle::handle_listener_result, state::OrchestrationState};
use crate::runtime::Proxy;
use std::time::Duration;
use zero_engine::EngineError;

impl OrchestrationState {
    pub(in crate::runtime::orchestration) async fn retire_previous_listeners(
        &mut self,
        proxy: &Proxy,
    ) -> Result<(), EngineError> {
        if !self.listeners.has_previous_generation() {
            return Ok(());
        }
        // This is the existing cleanup grace, never a protocol Ready timeout.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let factory = self.inbound_runtime_factory.clone();
        let listeners = &mut self.listeners;
        let drain = tokio::time::timeout_at(deadline, async {
            let mut ignored_expected_exits = 0;
            while let Some(original) = listeners.join_next().await {
                // These outcomes belong to the interrupted generation, not the
                // new expected-exit counter. Preserve its exact source/disposition.
                if let Err(error) =
                    handle_listener_result(Some(original), true, &mut ignored_expected_exits)
                {
                    listeners.retain_failure(error);
                }
            }
        });
        // Native/user owners may need to fence I/O before a remaining task can
        // finish. Drive cleanup alongside the existing observer, not after
        // waiting for that task's join. The same Scope mutex still serializes
        // every actual TaskExit consumption.
        let close = proxy
            .inbound_owners
            .shutdown_all_reported(deadline, |tag, terminal| {
                factory.publish_inbound_terminal(tag, terminal);
                Ok(())
            });
        let (drained, owners) = tokio::join!(drain, close);
        if let Err(source) = drained {
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, source).into());
        }
        if let Some(original) = self.listeners.take_failure() {
            return Err(original);
        }
        owners?;
        proxy
            .inbound_owners
            .confirm_observers_retired(&mut self.listeners)?;
        Ok(())
    }
}
