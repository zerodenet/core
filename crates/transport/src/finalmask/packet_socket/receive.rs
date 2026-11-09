//! Preserve one GRO original across callers without materialising batch copies.
use super::*;
use futures_util::task::AtomicWaker;
use quinn::udp::RecvMeta;
use std::{
    io::IoSliceMut,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};
pub(super) struct Receiver {
    state: Mutex<Option<Cursor>>,
    waiter: AtomicWaker,
    contended: AtomicBool,
}
struct Cursor {
    scratch: crate::finalmask::memory::Scratch,
    meta: RecvMeta,
    at: usize,
    available: bool,
}
struct Lease<'a> {
    owner: &'a Receiver,
    cursor: Option<Cursor>,
}
impl Receiver {
    pub(super) fn new(scratch: crate::finalmask::memory::Scratch) -> Self {
        Self {
            state: Mutex::new(Some(Cursor {
                scratch,
                meta: RecvMeta::default(),
                at: 0,
                available: false,
            })),
            waiter: AtomicWaker::new(),
            contended: AtomicBool::new(false),
        }
    }
    pub(super) fn poll(
        &self,
        socket: &dyn AsyncUdpSocket,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddress)>> {
        self.waiter.register(cx.waker());
        let cursor = self.state.lock().unwrap().take();
        let Some(cursor) = cursor else {
            self.contended.store(true, Ordering::Release);
            let available = self.state.lock().unwrap().is_some();
            if available {
                cx.waker().wake_by_ref();
            }
            return Poll::Pending;
        };
        let mut lease = Lease {
            owner: self,
            cursor: Some(cursor),
        };
        let cursor = lease.cursor.as_mut().unwrap();
        for _ in 0..32 {
            if !cursor.available {
                let mut meta = [RecvMeta::default()];
                let count = std::task::ready!(socket.poll_recv(
                    cx,
                    &mut [IoSliceMut::new(&mut cursor.scratch.bytes)],
                    &mut meta
                ))?;
                if count == 0 {
                    continue;
                }
                cursor.meta = meta[0];
                cursor.at = 0;
                if cursor.meta.len > cursor.scratch.bytes.len()
                    || (cursor.meta.len != 0 && cursor.meta.stride == 0)
                    || (cursor.meta.len != 0
                        && cursor.meta.len.div_ceil(cursor.meta.stride)
                            > socket.max_receive_segments().max(1))
                {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "original packet GRO exceeds its declared capability",
                    )));
                }
                cursor.available = true;
            }
            let end = cursor
                .at
                .saturating_add(cursor.meta.stride)
                .min(cursor.meta.len);
            let frame = &cursor.scratch.bytes[cursor.at..end];
            cursor.at = end;
            cursor.available = end < cursor.meta.len;
            if frame.len() > output.len() {
                continue;
            }
            output[..frame.len()].copy_from_slice(frame);
            return Poll::Ready(Ok((
                frame.len(),
                socket_addr_to_socket_address(cursor.meta.addr),
            )));
        }
        drop(lease);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        *self.owner.state.lock().unwrap() = self.cursor.take();
        if self.owner.contended.swap(false, Ordering::AcqRel) {
            self.owner.waiter.wake();
        }
    }
}
