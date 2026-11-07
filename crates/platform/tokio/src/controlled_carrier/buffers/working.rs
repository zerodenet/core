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
    available: Mutex<Option<CarrierBufferCredit>>,
    changed: tokio::sync::Notify,
    bound: usize,
    book: CarrierBufferBudget,
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
                bound: credit.bytes(),
                available: Mutex::new(Some(credit)),
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
        self.0.bound
    }
    fn take_working(&self, bytes: usize) -> Option<CarrierBufferFrameCredit> {
        if bytes > self.0.bound {
            return None;
        }
        let credit = self
            .0
            .available
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()?;
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
            if bytes > actual.0.bound {
                return actual
                    .0
                    .book
                    .reserve(bytes)
                    .await
                    .map(CarrierBufferFrameCredit::extra);
            }
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
        let Some(credit) = self.credit.take() else {
            return;
        };
        match &self.return_to {
            Some(slot) => {
                let previous = slot
                    .available
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .replace(credit);
                assert!(
                    previous.is_none(),
                    "the same unique working credit returned once"
                );
                slot.changed.notify_waiters();
            }
            None => drop(credit),
        }
    }
}

#[cfg(test)]
mod tests;
