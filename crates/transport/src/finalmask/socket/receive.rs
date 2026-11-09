//! One original GRO backing; decode a single member without a second queue.
use super::*;
use futures_util::task::AtomicWaker;
use std::sync::atomic::{AtomicBool, Ordering};
pub(super) struct Receiver {
    value: Mutex<Option<Cursor>>,
    waiter: AtomicWaker,
    contended: AtomicBool,
}
struct Cursor {
    scratch: super::super::memory::Scratch,
    info: RecvMeta,
    offset: usize,
    available: bool,
}
struct Lease<'a> {
    receiver: &'a Receiver,
    value: Option<Cursor>,
}
impl Receiver {
    pub(super) fn new(scratch: super::super::memory::Scratch) -> Self {
        Self {
            value: Mutex::new(Some(Cursor {
                scratch,
                info: RecvMeta::default(),
                offset: 0,
                available: false,
            })),
            waiter: AtomicWaker::new(),
            contended: AtomicBool::new(false),
        }
    }
    pub(super) fn poll(
        &self,
        owner: &Socket,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.waiter.register(cx.waker());
        let value = self.value.lock().unwrap().take();
        let Some(value) = value else {
            self.contended.store(true, Ordering::Release);
            // Recheck after publishing the wait, without calling any foreign
            // poll/waker while the actual cursor mutex is held.
            let available = self.value.lock().unwrap().is_some();
            if available {
                cx.waker().wake_by_ref();
            }
            return Poll::Pending;
        };
        let mut lease = Lease {
            receiver: self,
            value: Some(value),
        };
        let cursor = lease.value.as_mut().unwrap();
        for _ in 0..32 {
            if !cursor.available {
                let mut input = [IoSliceMut::new(&mut cursor.scratch.bytes)];
                let mut metadata = [RecvMeta::default()];
                let count =
                    std::task::ready!(owner.inner.poll_recv(cx, &mut input, &mut metadata))?;
                if count == 0 {
                    return Poll::Ready(Ok(0));
                }
                cursor.info = metadata[0];
                cursor.offset = 0;
                if cursor.info.len > cursor.scratch.bytes.len()
                    || (cursor.info.stride == 0 && cursor.info.len != 0)
                {
                    continue;
                }
                cursor.available = true;
            }
            let end = cursor
                .offset
                .saturating_add(cursor.info.stride)
                .min(cursor.info.len);
            let encoded = &cursor.scratch.bytes[cursor.offset..end];
            cursor.offset = end;
            cursor.available = end < cursor.info.len;
            let peak = { owner.codec.lock().unwrap().working_peak(encoded.len())? };
            let _credit = owner.memory.receive(peak)?;
            let decoded = { owner.codec.lock().unwrap().decode(encoded) };
            let Ok(decoded) = decoded else {
                continue;
            };
            if decoded.len() > bufs[0].len() {
                continue;
            }
            bufs[0][..decoded.len()].copy_from_slice(&decoded);
            meta[0] = RecvMeta {
                len: decoded.len(),
                stride: decoded.len(),
                ..cursor.info
            };
            return Poll::Ready(Ok(1));
        }
        drop(lease);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        *self.receiver.value.lock().unwrap() = self.value.take();
        if self.receiver.contended.swap(false, Ordering::AcqRel) {
            self.receiver.waiter.wake();
        }
    }
}
