//! One actual SDK request and its private Session Arc in the existing Memory.
use super::*;
impl Memory {
    pub(super) fn prepare_ring(
        &self,
        component: zero_core::component::ComponentId,
    ) -> Result<(), ChannelError> {
        if self.tasks.is_none() {
            return Ok(());
        } // explicit non-hosted library responsibilities unchanged
        let ring = self
            .roots
            .packet
            .reserve_requested_ring()
            .map_err(|source| {
                error(
                    component,
                    Reason::Capacity,
                    "SDK requested ring cannot fit before acquisition",
                )
                .with_source(source)
            })?;
        {
            let mut held = self
                .ring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if held.is_some() {
                drop(held);
                ring.release();
                return Err(error(
                    component,
                    Reason::Revoked,
                    "same ring already reserved",
                ));
            }
            *held = Some(ring); // retain before any later fallible/native work
        }
        let layout = zero_tun::OwnedWintunOwner::session_metadata_layout().map_err(|source| {
            error(
                component,
                Reason::Capacity,
                "actual SDK Session layout overflow",
            )
            .with_source(source)
        })?;
        let metadata = self
            .roots
            .packet
            .reserve_driver_metadata(layout)
            .map_err(|source| {
                error(
                    component,
                    Reason::Capacity,
                    "SDK Session metadata cannot fit before acquisition",
                )
                .with_source(source)
            })?;
        *self
            .session_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(metadata);
        Ok(())
    }
    pub(in super::super) async fn settle_ring(
        &self,
        original: &original::Original,
        deadline: Instant,
    ) -> bool {
        if self
            .ring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
        {
            return true;
        }
        let reader = self
            .reader
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(reader) = reader {
            if tokio::time::timeout_at(deadline, reader.wait_registration_released())
                .await
                .is_err()
            {
                return false;
            }
            if !reader
                .try_joined()
                .is_some_and(|joined| joined.registration_released())
            {
                return false;
            }
        }
        // Adapter/Event outcomes remain independent. There is no public raw
        // Session Arc/Weak in this owned path; the same original's actual
        // Arc::try_unwrap (or never allocated session) is the allocation fact.
        if !original.ring_request_finished() || !original.session_allocation_returned() {
            return false;
        }
        // No await under this synchronous guard, and no callback under owner
        // locks. Both real debits return before external notifications flush.
        let _notifications = self.roots.packet.defer_notifications();
        let ring = self
            .ring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let metadata = self
            .session_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(metadata);
        if let Some(ring) = ring {
            ring.release();
        }
        true
    }
}

#[cfg(test)]
impl Memory {
    pub(in super::super) fn test_sdk_reader(&self) -> Option<TaskJoinWitness> {
        self.reader
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(in super::super) fn test_session_metadata_retained(&self) -> bool {
        self.session_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
}
