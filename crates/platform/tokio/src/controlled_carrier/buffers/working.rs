//! One original Entry's prepaid scratch and two working credits, not a frame
//! pool. No Vec, codec, allocator callback, native controller or Scope is held.
use super::*;
#[derive(Clone, Copy, Debug)]
pub struct CarrierBufferWorkingShape {
    pub receive_scratch_bytes: usize,
    pub transmit_working_bytes: usize,
    pub receive_working_bytes: usize,
}
#[derive(Debug)]
struct Slot {
    state: Mutex<WorkingState>,
    changed: tokio::sync::Notify,
    book: CarrierBufferBudget,
}
#[derive(Debug)]
struct WorkingState {
    available: Option<CarrierBufferCredit>,
    top_up: Option<CarrierBufferCredit>,
    bound: usize,
}
#[derive(Clone, Debug)]
pub struct CarrierBufferWorkingLane(Arc<Slot>);
#[derive(Debug)]
pub struct CarrierBufferWorkingSet {
    scratch: Option<CarrierBufferCredit>,
    transmit: CarrierBufferWorkingLane,
    receive: CarrierBufferWorkingLane,
}
/// Drop only after the actual frame/copy backing disappears. A working credit
/// returns to its same original lane; an extra queued frame refunds its root.
/// The return target is pure accounting and has no Entry/IO/Scope back edge.
#[derive(Debug)]
pub struct CarrierBufferFrameCredit {
    credit: Option<CarrierBufferCredit>,
    return_to: Option<Arc<Slot>>,
}
impl CarrierBufferBudget {
    /// A concrete control writer has its own prepaid lane so accepted control
    /// work can finish while business frames occupy their transmit promises.
    pub fn try_working_lane(&self, bound: usize) -> io::Result<CarrierBufferWorkingLane> {
        if bound == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let credit = self.try_reserve(bound)?;
        Ok(CarrierBufferWorkingLane(Arc::new(Slot {
            state: Mutex::new(WorkingState {
                available: Some(credit),
                top_up: None,
                bound,
            }),
            changed: tokio::sync::Notify::new(),
            book: self.clone(),
        })))
    }
    /// Sizes come from the actual producer's plain bounds, including its codec
    /// and temporary-copy peak. Native GSO is never guessed or truncated here.
    /// Production must prepay before starting that original's business IO.
    /// Its two fixed bookkeeping slots belong to that same Entry's existing
    /// original-count admission; this byte book measures declared backing
    /// peaks, not opaque Arc/Notify implementation allocations or process RSS.
    pub fn try_working_set(
        &self,
        shape: CarrierBufferWorkingShape,
    ) -> io::Result<CarrierBufferWorkingSet> {
        if shape.receive_scratch_bytes == 0
            || shape.transmit_working_bytes == 0
            || shape.receive_working_bytes == 0
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let peak = shape
            .receive_scratch_bytes
            .checked_add(shape.transmit_working_bytes)
            .and_then(|bytes| bytes.checked_add(shape.receive_working_bytes))
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut credit = self.try_reserve(peak)?;
        let scratch = credit.split_off(shape.receive_scratch_bytes)?;
        let transmit = credit.split_off(shape.transmit_working_bytes)?;
        let lane = |credit: CarrierBufferCredit| {
            CarrierBufferWorkingLane(Arc::new(Slot {
                state: Mutex::new(WorkingState {
                    bound: credit.bytes(),
                    available: Some(credit),
                    top_up: None,
                }),
                changed: tokio::sync::Notify::new(),
                book: self.clone(),
            }))
        };
        Ok(CarrierBufferWorkingSet {
            scratch: Some(scratch),
            transmit: lane(transmit),
            receive: lane(credit),
        })
    }
}
impl CarrierBufferWorkingSet {
    pub fn take_receive_scratch(&mut self) -> Option<CarrierBufferCredit> {
        self.scratch.take()
    }
    pub fn transmit(&self) -> CarrierBufferWorkingLane {
        self.transmit.clone()
    }
    pub fn receive(&self) -> CarrierBufferWorkingLane {
        self.receive.clone()
    }
}
impl CarrierBufferWorkingLane {
    pub fn bound(&self) -> usize {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bound
    }
    /// Extend the same prepaid lane when the original native grant reveals its
    /// real GSO bound, before that hop starts business IO. An outstanding frame
    /// keeps its exact ticket; the added backing joins it only on actual return.
    pub fn try_grow_bound(&self, bound: usize) -> io::Result<()> {
        let previous = self.bound();
        if bound <= previous {
            return Ok(());
        }
        let mut extra = match self.0.book.try_reserve(bound - previous) {
            Ok(extra) => extra,
            Err(_) if self.bound() >= bound => return Ok(()),
            Err(source) => return Err(source),
        };
        let spent = {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let needed = bound.saturating_sub(state.bound);
            if needed == 0 {
                None
            } else {
                let additional = extra.split_off(needed)?;
                let spent = if let Some(available) = state.available.as_mut() {
                    Some(absorb(available, additional))
                } else if let Some(top_up) = state.top_up.as_mut() {
                    Some(absorb(top_up, additional))
                } else {
                    state.top_up = Some(additional);
                    None
                };
                state.bound = bound;
                spent
            }
        };
        // Even unused reservations can wake a reentrant original writer.
        drop(spent);
        drop(extra);
        self.0.changed.notify_waiters();
        Ok(())
    }
    fn take_working(&self, bytes: usize) -> Option<CarrierBufferFrameCredit> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if bytes > state.bound {
            return None;
        }
        let credit = state.available.take()?;
        Some(CarrierBufferFrameCredit {
            credit: Some(credit),
            return_to: Some(self.0.clone()),
        })
    }
    pub fn try_reserve(&self, bytes: usize) -> io::Result<CarrierBufferFrameCredit> {
        if let Some(credit) = self.take_working(bytes) {
            return Ok(credit);
        }
        self.0
            .book
            .try_reserve(bytes)
            .map(CarrierBufferFrameCredit::extra)
    }
    /// The original poller retains one such future and its actual Ready credit.
    /// Working-credit return does not reduce root.used, so waiting solely on
    /// root capacity would deadlock when all prepaid scratch remains live.
    pub fn reserve(
        &self,
        bytes: usize,
    ) -> impl std::future::Future<Output = io::Result<CarrierBufferFrameCredit>> + Send + 'static
    {
        let actual = self.clone();
        async move {
            loop {
                let returned = actual.0.changed.notified();
                tokio::pin!(returned);
                returned.as_mut().enable();
                if let Some(credit) = actual.take_working(bytes) {
                    return Ok(credit);
                }
                tokio::select! {
                    biased;
                    _ = &mut returned => {},
                    credit = actual.0.book.reserve(bytes) => return credit.map(CarrierBufferFrameCredit::extra),
                }
            }
        }
    }
}
impl CarrierBufferFrameCredit {
    fn extra(credit: CarrierBufferCredit) -> Self {
        Self {
            credit: Some(credit),
            return_to: None,
        }
    }
    pub fn bytes(&self) -> usize {
        self.credit
            .as_ref()
            .expect("live original frame credit")
            .bytes()
    }
    pub fn is_working(&self) -> bool {
        self.return_to.is_some()
    }
}
impl Drop for CarrierBufferFrameCredit {
    fn drop(&mut self) {
        let Some(mut credit) = self.credit.take() else {
            return;
        };
        match &self.return_to {
            Some(slot) => {
                let spent = {
                    let mut state = slot
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let spent = state.top_up.take().map(|extra| absorb(&mut credit, extra));
                    let previous = state.available.replace(credit);
                    assert!(
                        previous.is_none(),
                        "the same unique working credit returned once"
                    );
                    spent
                };
                drop(spent);
                slot.changed.notify_waiters();
            }
            None => drop(credit),
        }
    }
}

// Both tickets are issued by this exact lane's original book. Leave the empty
// moved ticket to its caller so its destructor never runs under the lane lock.
fn absorb(target: &mut CarrierBufferCredit, mut extra: CarrierBufferCredit) -> CarrierBufferCredit {
    assert!(target.book.same_budget(&extra.book));
    target.bytes = target
        .bytes
        .checked_add(extra.bytes)
        .expect("prepaid original byte total");
    extra.bytes = 0;
    extra
}

#[cfg(test)]
mod tests;
