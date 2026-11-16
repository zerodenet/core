//! Original setup/endpoint exit reception, separate from native and Host ACK.
use super::*;

impl Entry {
    pub(in crate::transport::owned::pool) async fn receive_setup(
        &self,
        end: Instant,
    ) -> Result<(), ChannelError> {
        tokio::time::timeout_at(end, self.receive_setup_original())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?
    }
    pub(in crate::transport::owned::pool) async fn receive_setup_original(
        &self,
    ) -> Result<(), ChannelError> {
        let caller = { self.state.lock().await.caller.clone() };
        let Some(caller) = caller else { return Ok(()) };
        let joined = caller.wait().await;
        {
            let mut state = self.state.lock().await;
            if state
                .setup_exit
                .as_ref()
                .is_none_or(|previous| !caller.matches_joined(previous))
            {
                state.setup_exit = Some(joined.clone());
                if let Err(source) = joined.result() {
                    if state.setup_failure.is_none() {
                        // Panic/registration wrapper faults are real task
                        // faults, unlike an already retained operation result.
                        state.retired_application.push(joined.clone());
                        state.setup_failure = Some(SetupFailure {
                            source,
                            next_candidate: false,
                        });
                    }
                }
            }
            state.initializing = false;
        }
        if joined.result().is_err() {
            caller
                .acknowledge_hosted_failure(&joined)
                .map_err(|source| error(Stage::Close, Reason::Internal).with_source(source))?;
        }
        Ok(())
    }
    pub(in crate::transport::owned::pool) async fn wait_setup_ack(
        &self,
        end: Instant,
    ) -> Result<(), ChannelError> {
        let caller = { self.state.lock().await.caller.clone() };
        if let Some(caller) = caller {
            let joined = tokio::time::timeout_at(end, caller.wait_registration_released())
                .await
                .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
            if !caller.matches_joined(&joined) || !joined.registration_released() {
                return Err(error(Stage::Close, Reason::Internal));
            }
        }
        Ok(())
    }
}

impl Entry {
    pub(in crate::transport::owned::pool) async fn wait_endpoint_ack(
        &self,
        end: Instant,
    ) -> Result<(), ChannelError> {
        let receipt = { self.state.lock().await.retiring_registrations.clone() };
        if let Some(receipt) = receipt {
            for original in receipt
                .registrations()
                .iter()
                .chain(receipt.related_registrations())
            {
                let joined = tokio::time::timeout_at(end, original.wait_registration_released())
                    .await
                    .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
                if !original.matches_joined(&joined) || !joined.registration_released() {
                    return Err(error(Stage::Close, Reason::Internal));
                }
            }
            let old = {
                let mut state = self.state.lock().await;
                if state
                    .retiring_registrations
                    .as_ref()
                    .is_some_and(|actual| actual.same_original(&receipt))
                {
                    state.retiring_registrations.take()
                } else {
                    None
                }
            };
            drop(old);
        }
        Ok(())
    }
}
