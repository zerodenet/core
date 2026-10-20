use super::*;
impl HostResources {
    fn carrier_phase(&self, state: &State) -> Result<(), ChannelError> {
        if self.cancel.is_cancelled() || state.closed {
            return Err(error(
                self.component,
                Reason::Revoked,
                "carrier registration was revoked",
            ));
        }
        let deadline = state.start_deadline.ok_or_else(|| {
            error(
                self.component,
                Reason::InvalidConfig,
                "carrier registration requires lifecycle start",
            )
        })?;
        if Instant::now() >= deadline {
            return Err(error(
                self.component,
                Reason::Timeout,
                "carrier registration deadline elapsed",
            ));
        }
        Ok(())
    }
    pub(in crate::component::instance::resources::native) fn register_carrier(
        &self,
        ordinal: usize,
        actual: CarrierOwnerRegistration,
        tasks: &TaskRegistrar,
    ) -> Result<CarrierOwnerBinding, ChannelError> {
        let (instance, scope) = self
            .carriers
            .identity
            .as_ref()
            .ok_or_else(|| self.denied())?;
        let declaration = self
            .carriers
            .declarations
            .get(&ordinal)
            .ok_or_else(|| self.denied())?;
        let ResourceDeclaration::ControlledOutboundStream { max_sockets, .. } = declaration else {
            return Err(self.denied());
        };
        if actual.instance() != *instance
            || actual.limit() != *max_sockets
            || !scope.matches(tasks)
            || !scope.registration_open()
        {
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "carrier instance, cap or actual scope differs",
            ));
        }
        {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.carrier_phase(&state)?;
            let mut records = self
                .carriers
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if records
                .iter()
                .any(|(other, entry)| *other != ordinal && entry.actual.same_owner(&actual))
            {
                return Err(error(
                    self.component,
                    Reason::InvalidConfig,
                    "one carrier owner cannot cover multiple declarations",
                ));
            }
            if let Some(entry) = records.get(&ordinal) {
                if !entry.actual.same_owner(&actual) {
                    return Err(error(
                        self.component,
                        Reason::InvalidConfig,
                        "carrier declaration already has its sole owner",
                    ));
                }
                if let Some(binding) = &entry.bound {
                    return Ok(binding.clone());
                }
            } else {
                records.insert(
                    ordinal,
                    Registration {
                        actual: actual.clone(),
                        bound: None,
                        first_fault: None,
                        first_native_fault: None,
                    },
                );
            }
        }
        // Pending identity is persistent before a potentially reentrant scope
        // getter. Failure/cancellation never removes that original registration.
        let host_cancel = self
            .carriers
            .host_cancel
            .as_ref()
            .expect("declared carrier host fence");
        let result = actual.bind_declared(declaration, tasks, host_cancel);
        let binding_created = result.is_ok();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = result.and_then(|binding| {
            self.carrier_phase(&state)?;
            if !binding.matches_registration(&actual)
                || !binding.matches_declaration(declaration)
                || !binding.matches_host_cancel(host_cancel)
            {
                return Err(error(
                    self.component,
                    Reason::InvalidConfig,
                    "carrier binding does not match its original declaration",
                ));
            }
            Ok(binding)
        });
        let mut records = self
            .carriers
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = records
            .get_mut(&ordinal)
            .expect("persistent original carrier registration");
        let binding = match result {
            Ok(binding) => {
                entry.bound = Some(binding.clone());
                binding
            }
            Err(failure) => {
                // Drop a losing original error only after releasing every host
                // lock; source destructors may themselves reenter cleanup.
                let incoming = Arc::new(failure);
                let first = entry
                    .first_fault
                    .get_or_insert_with(|| incoming.clone())
                    .clone();
                drop(records);
                drop(state);
                if binding_created {
                    self.cancel.cancel();
                }
                return Err(crate::component::instance::retained(first));
            }
        };
        drop(records);
        drop(state);
        Ok(binding)
    }
}
