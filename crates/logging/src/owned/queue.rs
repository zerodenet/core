//! Each output has its own queue. Only its global book is shared.
use super::*;
use std::collections::VecDeque;
pub(super) struct Frame {
    pub(super) bytes: Box<[u8]>,
    _payload: book::Credit,
}
impl Frame {
    pub(super) fn prepare(book: &Arc<book::Book>, output: usize, bytes: &[u8]) -> io::Result<Self> {
        let credit = book.payload(output, bytes.len())?;
        let mut data = Box::<[u8]>::new_uninit_slice(bytes.len());
        for (target, byte) in data.iter_mut().zip(bytes) {
            target.write(*byte);
        }
        // SAFETY: each exact slot is initialized, with no allocation or copy
        // after its same payload admission. No Vec spare capacity is retained.
        let data = unsafe { data.assume_init() };
        Ok(Self {
            bytes: data,
            _payload: credit,
        })
    }
}
pub(super) struct Ring {
    data: VecDeque<Frame>,
    _storage: Option<book::Storage>,
}
impl Ring {
    pub(super) fn empty() -> Self {
        Self {
            data: VecDeque::new(),
            _storage: None,
        }
    }
    fn prepare(book: &Arc<book::Book>, minimum: usize) -> io::Result<Self> {
        let slots = minimum
            .checked_next_power_of_two()
            .ok_or_else(capacity)?
            .max(4);
        let bytes = slots
            .checked_mul(std::mem::size_of::<Frame>())
            .ok_or_else(capacity)?;
        let storage = book.storage(bytes)?;
        let mut data = VecDeque::new();
        data.try_reserve_exact(slots).map_err(io::Error::other)?;
        if data.capacity() > slots {
            return Err(io::Error::other("logging ring exceeded prepaid layout"));
        }
        Ok(Self {
            data,
            _storage: Some(storage),
        })
    }
    pub(super) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub(super) fn pop(&mut self) -> Option<Frame> {
        self.data.pop_front()
    }
}
impl file::Entry {
    pub(super) fn enqueue(&self, mut frame: Option<Frame>) -> io::Result<()> {
        loop {
            let minimum = {
                let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
                if queue.data.len() < queue.data.capacity() {
                    queue.data.push_back(frame.take().unwrap());
                    queue.data.back_mut().unwrap()._payload.queued();
                    drop(queue);
                    self.book.wake.notify();
                    return Ok(());
                }
                add(queue.data.len(), 1)?
            };
            let mut candidate = Ring::prepare(&self.book, minimum)?;
            let old = {
                let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
                if queue.data.len() < candidate.data.capacity() {
                    candidate.data.append(&mut queue.data);
                    Some(std::mem::replace(&mut *queue, candidate))
                } else {
                    None
                }
            };
            drop(old);
        }
    }
}
