//! Same Source receiver before any native accept or child registration.
use super::*;
impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn client_failure_receiver(
        &self,
    ) -> Option<&zero_platform_tokio::AcceptedTaskFailures> {
        self.client_task_failures.get()
    }
    pub(in crate::runtime::inbound_owners) fn prepare_client_failures(
        &self,
        originals: &zero_runtime::HostOriginalBudgets,
    ) -> Result<zero_platform_tokio::AcceptedTaskFailures, ChannelError> {
        let memory = originals.packet.driver_metadata_view();
        if self.client_task_failures.get().is_none() {
            let actual = zero_platform_tokio::AcceptedTaskFailures::prepare(
                self.identity,
                &self.registrar,
                &memory,
            )?;
            // A concurrent cold installer may win. Neither candidate owns IO,
            // tasks or acceptance, so the unused paid allocation can return.
            if let Err(unused) = self.client_task_failures.set(actual) {
                drop(unused);
            }
        }
        let actual = self.client_failure_receiver().expect("installed receiver");
        if !actual.matches(self.identity, &self.registrar.scope_identity(), &memory) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        Ok(actual.clone())
    }
}

#[cfg(test)]
impl InboundOwner {
    pub(crate) fn diagnose_datagram_failures_for_test(&self) {
        use std::error::Error;
        eprintln!(
            "actual Source result {:?}: {:?}",
            self.identity,
            self.result()
        );
        if let Some(receiver) = self.client_failure_receiver() {
            for failure in receiver.iter() {
                eprintln!(
                    "actual Source accepted failure: task={:?} class={:?} result={:?}",
                    failure.id(),
                    failure.class(),
                    failure.result()
                );
                if let Err(error) = failure.result() {
                    let mut source: Option<&(dyn Error + 'static)> = Some(error);
                    for index in 0..32 {
                        let Some(actual) = source else {
                            break;
                        };
                        eprintln!("actual accepted failure source[{index}]: {actual:?}; {actual}");
                        source = actual.source();
                    }
                }
            }
        }
        #[cfg(feature = "managed-datagram-runtime")]
        {
            let groups = self.peers.lock().expect("peer owners poisoned").clone();
            for peers in groups {
                peers.diagnose_actual_retirement();
            }
        }
    }
}
