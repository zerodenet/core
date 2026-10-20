use super::*;
use zero_platform_tokio::controlled_datagram::{
    metadata::GrantedMetadataDatagram, AcquisitionOutcome,
};
impl HostResources {
    pub(in crate::component::instance::resources::native) fn bind_metadata(
        &self,
        request: MetadataDatagramRequest,
    ) -> Result<GrantedMetadataListener, ChannelError> {
        let (_, scope) = self
            .metadata
            .identity
            .as_ref()
            .ok_or_else(|| self.denied())?;
        // Scope observations can release the last temporary strong owner. Never
        // make this call under resource/native/cleanup locks.
        if !scope.registration_open() {
            return Err(error(
                self.component,
                Reason::Revoked,
                "metadata host scope is not started/open",
            ));
        }
        self.listener_phase(
            &self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )?;
        let entry = self
            .metadata
            .entries
            .get(&request.ordinal)
            .ok_or_else(|| self.denied())?;
        if entry.source != request.source || entry.limits != request.limits {
            return Err(self.denied());
        }
        let mut native = entry.lock().ok_or_else(|| {
            error(
                self.component,
                Reason::Capacity,
                "metadata original owner is busy",
            )
        })?;
        self.listener_phase(
            &self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )?;
        if native.closing {
            return Err(error(
                self.component,
                Reason::Revoked,
                "metadata owner is closing",
            ));
        }
        native.observe_originals(self.component);
        if native.main_busy || native.probes_busy {
            return Err(error(
                self.component,
                Reason::Capacity,
                "metadata native evidence is busy",
            ));
        }
        if let Some(unknown) = native
            .main
            .unknown
            .first()
            .or_else(|| native.probes.unknown.first())
        {
            let mut failure = error(
                self.component,
                Reason::Io,
                "metadata original disposition remains unknown",
            );
            failure.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
            return Err(failure.with_source(evidence::NativeSource(unknown.source.clone())));
        }
        if native.remaining == 0 {
            return Err(error(
                self.component,
                Reason::Capacity,
                "metadata bind attempt budget exhausted",
            ));
        }
        native.remaining -= 1;
        native.attempted = true;
        // The owner was installed during cold construction. Panics, failed
        // registration and later rejection cannot drop its actual originals.
        let granted = native.owner.bind(entry.source);
        let GrantedMetadataDatagram { rx, tx, control } = match granted {
            Ok(grant) => grant,
            Err(source) => {
                let mut failure = error(self.component, Reason::Io, "metadata acquisition failed");
                failure.delivery = match source.outcome {
                    AcquisitionOutcome::Unknown(_) => {
                        zero_platform_tokio::contract::DeliveryState::Unknown
                    }
                    _ => zero_platform_tokio::contract::DeliveryState::NotSubmitted,
                };
                failure = failure.with_source(source);
                let source = native
                    .first_fault
                    .get_or_insert_with(|| Arc::new(failure))
                    .clone();
                native.observe_originals(self.component);
                return Err(crate::component::instance::retained(source));
            }
        };
        native.controls.push(control);
        let observation = match native
            .controls
            .last_mut()
            .expect("installed same original control")
            .take_observation()
        {
            Ok(observation) => observation,
            Err(source) => {
                let failure = Arc::new(
                    error(
                        self.component,
                        Reason::Io,
                        "metadata observation transfer failed",
                    )
                    .with_source(source),
                );
                let source = native.first_fault.get_or_insert(failure).clone();
                native.observe_originals(self.component);
                return Err(crate::component::instance::retained(source));
            }
        };
        let phase = self.listener_phase(
            &self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        if let Err(failure) = phase {
            native.closing = true;
            native.close_originals(self.component);
            return Err(failure);
        }
        native.observe_originals(self.component);
        Ok(GrantedMetadataListener {
            scope: scope.clone(),
            rx,
            tx,
            observation,
        })
    }
}
