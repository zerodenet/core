//! Pre-delivery protocol originals belong to this actual Query row.
use super::*;
mod failure;
pub(in crate::component::native_dns::provider) use failure::ProtocolUse;
use zero_platform_tokio::{
    contract::{component::PacketDriverMetadataView, ChannelError, Reason},
    controlled_carrier::ProtocolOriginal,
};
impl NativeUse {
    pub(in crate::component::native_dns::provider) fn retain_protocol_original(
        &self,
        original: ProtocolOriginal,
        memory: &PacketDriverMetadataView,
        ceiling: usize,
    ) -> Result<(), ChannelError> {
        // Remove only positively settled actual Root records. Move each old
        // token out before dropping it: it may release a paid header/waker.
        loop {
            let retired = {
                let mut entries = self
                    .protocol_originals
                    .lock()
                    .expect("DNS pending protocols poisoned");
                let index = entries.iter().position(|entry| entry.original.is_settled());
                index.map(|index| entries.remove(index))
            };
            match retired {
                Some(original) => drop(original),
                None => break,
            }
        }
        if self
            .protocol_originals
            .lock()
            .expect("DNS pending protocols poisoned")
            .iter()
            .any(|same| same.original.same_original(&original))
        {
            return Ok(());
        }
        let mut prepared = Some(ProtocolUse::prepare(original, memory)?);
        // This original Host's notification guard precedes the table lock,
        // including allocation failure and old/new capacity credit overlap.
        let notifications = memory.notifications();
        let mut rejected = None;
        let outcome = {
            let mut entries = self
                .protocol_originals
                .lock()
                .expect("DNS pending protocols poisoned");
            if entries.iter().any(|same| {
                same.original
                    .same_original(&prepared.as_ref().unwrap().original)
            }) {
                Ok(())
            } else {
                entries
                    .try_reserve_one_with_metadata(memory, ceiling)
                    .map_err(|source| {
                        crate::component::instance::invalid(
                            Reason::Capacity,
                            "DNS pending protocol metadata capacity exhausted",
                        )
                        .with_source(source)
                    })
                    .and_then(|()| {
                        entries
                            .push_reserved(prepared.take().unwrap())
                            .map_err(|value| {
                                rejected = Some(value);
                                crate::component::instance::invalid(
                                    Reason::Capacity,
                                    "DNS pending protocol table has no paid slot",
                                )
                            })
                    })
            }
        };
        drop(notifications);
        drop(prepared);
        drop(rejected);
        outcome
    }
    pub(in crate::component::native_dns::provider) fn protocol_originals_returned(&self) -> bool {
        let mut index = 0;
        loop {
            // A cached receiver can retain drivers and a LoanOwner. Clone only
            // this paid token under the Query mutex; inspect/drop its receiver
            // outside the mutex, just as in the asynchronous return path.
            let original = self
                .protocol_originals
                .lock()
                .expect("DNS pending protocols poisoned")
                .get(index)
                .map(|entry| entry.original.clone());
            let Some(original) = original else {
                return true;
            };
            if !original.is_returned() {
                return false;
            }
            index += 1;
        }
    }
    pub(in crate::component::native_dns::provider) async fn return_protocol_originals(
        &self,
        end: tokio::time::Instant,
    ) -> std::io::Result<()> {
        let mut index = 0;
        loop {
            // One already registered value at a time; never hold this mutex
            // across Root polling/await or allocate a second history snapshot.
            let original = self
                .protocol_originals
                .lock()
                .expect("DNS pending protocols poisoned")
                .get(index)
                .map(|entry| entry.original.clone());
            let Some(original) = original else {
                return Ok(());
            };
            // This runs only after the actual Query LoanOwner and adoption
            // returned their IO/views above. The sealed request is pre-delivery
            // only: an established cached receiver keeps its physical Root.
            original.request_retirement();
            match tokio::time::timeout_at(end, original.wait_returned_original()).await {
                Err(source) => return Err(retirement::pending_deadline(source)),
                Ok(Err(source)) => {
                    // The sealed wait's only error is loss of its original
                    // Root before return or settlement. Cache that one causal event,
                    // not a new diagnostic allocation on every retry.
                    let (retained, repeated) = {
                        let entries = self
                            .protocol_originals
                            .lock()
                            .expect("DNS pending protocols poisoned");
                        let entry = entries.get(index).expect("same actual Query protocol row");
                        assert!(entry.original.same_original(&original));
                        entry.receive_failure(source)
                    };
                    drop(repeated);
                    return Err(self.receiver.accept_retained_error(retained)?);
                }
                Ok(Ok(())) if !original.is_returned() => {
                    return Err(retirement::pending_incomplete(
                        "DNS protocol original lacks Root settlement or its exact cached receiver",
                    ))
                }
                Ok(Ok(())) => {}
            }
            index += 1;
        }
    }
}
