//! One prepaid replay selection of the actual immutable history originals.
use super::*;
use std::io;
use zero_runtime::configuration::ControlBudgetPermit;
pub struct RetainedReplay {
    events: Vec<Arc<RawApiEvent>>,
    _credit: Option<ControlBudgetPermit>,
    pub actual_from: u64,
    pub has_gap: bool,
}
impl RetainedReplay {
    pub fn empty() -> Self {
        Self {
            events: Vec::new(),
            _credit: None,
            actual_from: 0,
            has_gap: false,
        }
    }
    pub fn len(&self) -> usize {
        self.events.len()
    }
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
    pub fn event(&self, index: usize) -> &RawApiEvent {
        &self.events[index]
    }
}
struct Count(usize);
impl io::Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("original replay length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl EngineEventLog {
    pub(crate) fn retained_replay(
        &self,
        since: u64,
        limit: usize,
        filter: &EventFilter,
        mut credit: ControlBudgetPermit,
    ) -> io::Result<RetainedReplay> {
        // Parameter custody outlives this guard, including on allocation/error.
        let retained = self.inner.lock().expect("engine event log lock poisoned");
        let requested = since.saturating_add(1);
        let retained_from = retained
            .front()
            .and_then(|event| event.sequence)
            .unwrap_or_else(|| self.next_sequence.load(Ordering::Relaxed).max(requested));
        let selected = || {
            retained
                .iter()
                .filter(|event| {
                    event.sequence.is_some_and(|seq| seq > since) && matches_filter(event, filter)
                })
                .take(limit)
        };
        let mut count = 0usize;
        let mut bytes = Count(0);
        for event in selected() {
            count += 1;
            serde_json::to_writer(&mut bytes, &**event).map_err(io::Error::other)?;
        }
        let bytes = count
            .checked_mul(std::mem::size_of::<Arc<RawApiEvent>>())
            .and_then(|workspace| workspace.checked_add(bytes.0))
            .ok_or_else(|| io::Error::other("original replay storage overflow"))?;
        // Reserve before the first Arc/Vec clone. Originals remain under their
        // real history owner; this credit covers their retained response use.
        credit.grow(bytes)?;
        let mut events = Vec::new();
        events.try_reserve_exact(count).map_err(io::Error::other)?;
        events.extend(selected().cloned());
        let has_gap = retained_from > requested;
        let actual_from = events
            .first()
            .and_then(|event| event.sequence)
            .unwrap_or(if has_gap { retained_from } else { requested });
        Ok(RetainedReplay {
            events,
            _credit: Some(credit),
            actual_from,
            has_gap,
        })
    }
}

#[cfg(test)]
mod tests;
