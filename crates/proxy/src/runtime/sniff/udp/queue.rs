//! One original row array for Pending AND Ready packets, not two paid copies.
use super::rows::Rows;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
struct Queued<T> {
    sequence: u64,
    pending: Option<u64>,
    ready: Option<u64>,
    original: Option<T>,
}
pub(crate) struct Packets<T> {
    values: Rows<Queued<T>>,
    maximum: usize,
    next_ready: u64,
    // Both actual receive loops drain Ready before reading another original.
    // A refused growth holds this one input inline and backpressures the
    // next read; it never discards an older Pending classification.
    immediate: Option<Queued<T>>,
}
impl<T> Packets<T> {
    pub(crate) fn new(memory: Option<&PacketDriverMetadataView>, maximum: usize) -> Self {
        Self {
            values: Rows::new(memory),
            maximum,
            next_ready: 0,
            immediate: None,
        }
    }
    pub(crate) fn push(&mut self, sequence: u64, pending: Option<u64>, original: T) -> bool {
        let ready = if pending.is_none() {
            let next = self.next_ready;
            self.next_ready = self.next_ready.saturating_add(1);
            Some(next)
        } else {
            None
        };
        let row = Queued {
            sequence,
            pending,
            ready,
            original: Some(original),
        };
        match self.values.try_push(row, self.maximum) {
            Ok(()) => true,
            Err((_, original)) => {
                assert!(
                    self.immediate.is_none(),
                    "receive loop drains the prior inline original first"
                );
                self.immediate = Some(original);
                false
            }
        }
    }
    pub(crate) fn row_pressure(&self) -> bool {
        self.immediate.is_some()
    }
    fn promote_inline(&mut self) -> bool {
        let Some(original) = self.immediate.take() else {
            return true;
        };
        match self.values.try_push(original, self.maximum) {
            Ok(()) => true,
            Err((_, original)) => {
                self.immediate = Some(original);
                false
            }
        }
    }
    /// Arm the same Root before trying promotion. Preserve all Pending rows,
    /// packet numbers and reassembly while the original caller deadline runs.
    pub(crate) async fn wait_capacity(&mut self, deadline: tokio::time::Instant) {
        let Some(memory) = self.values.memory().cloned() else {
            return;
        };
        let changed = memory.changed_since(memory.snapshot());
        tokio::pin!(changed);
        let ready = std::future::poll_fn(|cx| {
            use std::future::Future;
            std::task::Poll::Ready(changed.as_mut().poll(cx).is_ready())
        })
        .await;
        if self.promote_inline() || ready {
            return;
        }
        tokio::select! { _=&mut changed=>{}, _=tokio::time::sleep_until(deadline)=>{} }
    }
    pub(crate) fn current_mut(&mut self, sequence: u64) -> Option<&mut T> {
        if let Some(index) = self
            .values
            .iter()
            .position(|row| row.sequence == sequence && row.pending.is_none())
        {
            return self.values[index].original.as_mut();
        }
        self.immediate
            .as_mut()
            .filter(|row| row.sequence == sequence && row.pending.is_none())
            .and_then(|row| row.original.as_mut())
    }
    pub(crate) fn ready_mut(&mut self) -> Option<&mut T> {
        let index = self
            .values
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.ready.map(|ready| (ready, index)))
            .min();
        if let Some((_, index)) = index {
            return self.values[index].original.as_mut();
        }
        self.immediate
            .as_mut()
            .filter(|row| row.ready.is_some())
            .and_then(|row| row.original.as_mut())
    }
    pub(crate) fn pop(&mut self, sequence: u64) -> Option<T> {
        if let Some(index) = self
            .values
            .iter()
            .position(|row| row.sequence == sequence && row.pending.is_none())
        {
            return self.values.remove(index).original;
        }
        if self
            .immediate
            .as_ref()
            .is_some_and(|row| row.sequence == sequence && row.pending.is_none())
        {
            return self.immediate.take().and_then(|row| row.original);
        }
        None
    }
    pub(crate) fn pop_ready(&mut self) -> Option<T> {
        let index = self
            .values
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.ready.map(|ready| (ready, index)))
            .min();
        if let Some((_, index)) = index {
            return self.values.remove(index).original;
        }
        if self
            .immediate
            .as_ref()
            .is_some_and(|row| row.ready.is_some())
        {
            self.immediate.take().and_then(|row| row.original)
        } else {
            None
        }
    }
    pub(crate) fn release(&mut self, id: u64, mut apply: impl FnMut(T) -> T) {
        for row in self.values.iter_mut().filter(|row| row.pending == Some(id)) {
            let original = row.original.take().expect("one actual queued original");
            row.original = Some(apply(original));
            row.pending = None;
            row.ready = Some(self.next_ready);
            self.next_ready = self.next_ready.saturating_add(1);
        }
        if let Some(row) = self
            .immediate
            .as_mut()
            .filter(|row| row.pending == Some(id))
        {
            let original = row.original.take().expect("same inline pending original");
            row.original = Some(apply(original));
            row.pending = None;
            row.ready = Some(self.next_ready);
            self.next_ready = self.next_ready.saturating_add(1);
        }
    }
    pub(crate) fn release_all(&mut self) {
        for row in self.values.iter_mut().filter(|row| row.pending.is_some()) {
            row.pending = None;
            row.ready = Some(self.next_ready);
            self.next_ready = self.next_ready.saturating_add(1);
        }
        if let Some(row) = self.immediate.as_mut().filter(|row| row.pending.is_some()) {
            row.pending = None;
            row.ready = Some(self.next_ready);
            self.next_ready = self.next_ready.saturating_add(1);
        }
    }
}

#[cfg(test)]
#[path = "queue/tests.rs"]
mod tests;
