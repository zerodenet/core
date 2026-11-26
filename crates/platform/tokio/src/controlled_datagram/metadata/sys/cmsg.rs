use std::{
    ffi::{c_int, c_uchar},
    mem, ptr,
};

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(super) use unix::Aligned;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::Aligned;

/// Helper to encode control messages (`cmsg`) into an aligned ancillary buffer.
pub(super) struct Encoder<'a, M: MsgHdr> {
    hdr: &'a mut M,
    cmsg: Option<&'a mut M::ControlMessage>,
    len: usize,
}

impl<'a, M: MsgHdr> Encoder<'a, M> {
    /// # Safety
    /// - `hdr` must point to a suitably aligned writable control buffer.
    /// - `Encoder` must be dropped before `hdr` is passed to a syscall.
    pub(super) unsafe fn new(hdr: &'a mut M) -> Self {
        Self {
            cmsg: unsafe { hdr.cmsg_first_hdr().as_mut() },
            hdr,
            len: 0,
        }
    }

    pub(super) fn push<T: Copy>(&mut self, level: c_int, ty: c_int, value: T) {
        assert!(mem::align_of::<T>() <= mem::align_of::<M::ControlMessage>());
        let space = M::ControlMessage::cmsg_space(mem::size_of_val(&value));
        assert!(
            self.hdr.control_len() >= self.len + space,
            "control message buffer too small. Required: {}, Available: {}",
            self.len + space,
            self.hdr.control_len()
        );
        let cmsg = self.cmsg.take().expect("no control buffer space remaining");
        cmsg.set(
            level,
            ty,
            M::ControlMessage::cmsg_len(mem::size_of_val(&value)),
        );
        unsafe {
            ptr::write(cmsg.cmsg_data() as *const T as *mut T, value);
        }
        self.len += space;
        self.cmsg = unsafe { self.hdr.cmsg_nxt_hdr(cmsg).as_mut() };
    }

    pub(super) fn finish(self) {}
}

impl<M: MsgHdr> Drop for Encoder<'_, M> {
    fn drop(&mut self) {
        self.hdr.set_control_len(self.len);
    }
}

/// # Safety
/// `cmsg` must refer to a native `cmsg` containing a payload of type `T`.
pub(super) unsafe fn decode<T: Copy, C: CMsgHdr>(cmsg: &impl CMsgHdr) -> T {
    assert!(mem::align_of::<T>() <= mem::align_of::<C>());
    debug_assert_eq!(cmsg.cmsg_byte_len(), C::cmsg_len(mem::size_of::<T>()));
    unsafe { ptr::read(cmsg.cmsg_data() as *const T) }
}

pub(super) struct Iter<'a, M: MsgHdr> {
    hdr: &'a M,
    cmsg: Option<&'a M::ControlMessage>,
}

impl<'a, M: MsgHdr> Iter<'a, M> {
    /// # Safety
    /// `hdr` must hold a valid initialized native `cmsg` buffer outliving `'a`.
    pub(super) unsafe fn new(hdr: &'a M) -> Self {
        Self {
            hdr,
            cmsg: unsafe { hdr.cmsg_first_hdr().as_ref() },
        }
    }
}

impl<'a, M: MsgHdr> Iterator for Iter<'a, M> {
    type Item = &'a M::ControlMessage;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.cmsg.take()?;
        self.cmsg = unsafe { self.hdr.cmsg_nxt_hdr(current).as_ref() };
        Some(current)
    }
}

pub(super) trait MsgHdr {
    type ControlMessage: CMsgHdr;

    fn cmsg_first_hdr(&self) -> *mut Self::ControlMessage;
    fn cmsg_nxt_hdr(&self, cmsg: &Self::ControlMessage) -> *mut Self::ControlMessage;
    fn set_control_len(&mut self, len: usize);
    fn control_len(&self) -> usize;
}

pub(super) trait CMsgHdr {
    fn cmsg_len(length: usize) -> usize;
    fn cmsg_space(length: usize) -> usize;
    fn cmsg_data(&self) -> *mut c_uchar;
    fn set(&mut self, level: c_int, ty: c_int, len: usize);
    #[cfg_attr(not(debug_assertions), allow(dead_code))]
    fn cmsg_byte_len(&self) -> usize;
}
