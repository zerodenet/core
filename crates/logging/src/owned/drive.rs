use super::*;
impl ScopedTracing {
    /// Called by main while its same pinned application future remains live.
    /// Join/ACK, including a late hosted ACK, gates every replacement actor.
    pub fn drive(&mut self, tasks: &TaskRegistrar) -> io::Result<()> {
        if !self.shared.scope.same_scope(&tasks.scope_identity()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "foreign logging Scope",
            ));
        }
        for (entry, lane) in self.shared.entries.iter().zip(&mut self.lanes) {
            if lane.creating {
                continue;
            }
            if let Some(witness) = &lane.witness {
                let Some(joined) = witness.try_joined() else {
                    continue;
                };
                if joined.result().is_err() && !lane.failure_delivered {
                    continue;
                }
                if !joined.registration_released() {
                    continue;
                }
            }
            if lane.witness.is_some() {
                // Registration ACK permits replacement, but does not destroy
                // retained headers. Drop the actual old handles before refund.
                drop(lane.witness.take());
                drop(lane.slot.take());
            }
            let need = !entry.started.load(std::sync::atomic::Ordering::Acquire)
                || !entry.queue_empty()
                || entry.needs_reopen()
                || (self.stopping && !entry.native_closed() && self.shared.book.preparing() == 0);
            if !need || entry.has_pending_io() || entry.is_unknown() {
                continue;
            }
            // A real worker must have all of its possible fixed source cells
            // prepaid. Capacity failure does not create an empty drain job.
            if !(0..4).all(|index| entry.ensure_slot(index)) {
                continue;
            }
            if lane.slot.is_none() {
                match fault::WorkerSlot::reserve(&self.shared.book) {
                    Ok(slot) => lane.slot = Some(slot),
                    Err(_) => continue,
                }
            }
            let launch = match tasks.reserve_launch(TaskClass::Critical) {
                Ok(launch) => launch,
                Err(_) => continue,
            };
            // Explicit creation responsibility, not a joined/native receipt.
            lane.creating = true;
            entry.gate.reset();
            let actual = entry.clone();
            match launch.spawn_blocking_tracked(move || worker::run(actual)) {
                Ok((_, witness)) => {
                    let old = lane.witness.replace(witness);
                    lane.failure_delivered = false;
                    lane.creating = false;
                    // The real witness is strongly held before first IO.
                    entry
                        .started
                        .store(true, std::sync::atomic::Ordering::Release);
                    entry.gate.open();
                    drop(old);
                }
                Err(source) => {
                    lane.creating = false;
                    return Err(io::Error::other(source));
                }
            }
        }
        Ok(())
    }
    pub fn take_worker_failure(&mut self) -> io::Result<Option<WorkerFailure>> {
        for (index, lane) in self.lanes.iter_mut().enumerate() {
            if let Some(source) = &lane.delivery {
                return Ok(Some(source.clone()));
            }
            if lane.failure_delivered {
                continue;
            }
            let Some(witness) = &lane.witness else {
                continue;
            };
            let Some(joined) = witness.try_joined() else {
                continue;
            };
            if joined.result().is_ok() {
                continue;
            }
            let frame = {
                self.shared.entries[index]
                    .working
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
            };
            self.shared.entries[index].require_reopen();
            let source = lane
                .slot
                .take()
                .expect("real worker error prepaid")
                .receive(witness, frame)?
                .expect("actual failure");
            lane.delivery = Some(source.clone());
            witness
                .acknowledge_hosted_failure(source.joined())
                .map_err(io::Error::other)?;
            return Ok(Some(source));
        }
        Ok(None)
    }
    pub fn accept_worker_failure(&mut self, source: &WorkerFailure) -> bool {
        let Some(lane) = self.lanes.iter_mut().find(|lane| {
            lane.delivery
                .as_ref()
                .is_some_and(|actual| actual.same(source))
        }) else {
            return false;
        };
        lane.failure_delivered = true;
        let old = lane.delivery.take();
        drop(old);
        true
    }
    pub fn drained(&self) -> bool {
        if self.shared.book.preparing() != 0
            || self.lanes.iter().any(|lane| {
                lane.creating
                    || lane.witness.as_ref().is_some_and(|w| {
                        w.try_joined()
                            .is_none_or(|joined| !joined.registration_released())
                    })
            })
        {
            return false;
        }
        self.shared.entries.iter().all(|entry| {
            entry
                .queue
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
                && entry
                    .working
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_none()
        })
    }
}
