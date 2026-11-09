//! Every actually delivered descriptor is retained before checking its frame.
use super::*;
impl Transfer {
    pub(in crate::macos_privilege) fn receive_fd(&self, socket: RawFd) -> io::Result<RawFd> {
        self.enter_receive();
        let mut marker = [0_u8];
        let mut iovec = libc::iovec {
            iov_base: marker.as_mut_ptr().cast(),
            iov_len: 1,
        };
        let space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as u32) } as usize;
        let mut aligned = [0_usize; 4];
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = &mut iovec;
        message.msg_iovlen = 1;
        message.msg_control = aligned.as_mut_ptr().cast();
        message.msg_controllen = space as _;
        let count = loop {
            let count = unsafe { libc::recvmsg(socket, &mut message, 0) };
            if count >= 0 {
                break count;
            }
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if source.kind() != io::ErrorKind::WouldBlock {
                self.capture_received(&[])?;
            }
            return Err(source);
        };
        let mut descriptors = [-1; 2];
        let mut delivered = 0;
        let header = unsafe { libc::CMSG_FIRSTHDR(&message) };
        if !header.is_null()
            && unsafe {
                (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS
            }
        {
            let prefix = unsafe { libc::CMSG_LEN(0) } as usize;
            delivered = (unsafe { (*header).cmsg_len } as usize)
                .saturating_sub(prefix)
                .min(space.saturating_sub(prefix))
                / std::mem::size_of::<RawFd>();
            delivered = delivered.min(descriptors.len());
            for (index, slot) in descriptors[..delivered].iter_mut().enumerate() {
                *slot = unsafe {
                    std::ptr::read_unaligned(libc::CMSG_DATA(header).cast::<RawFd>().add(index))
                };
            }
        }
        self.capture_received(&descriptors[..delivered])?;
        if count != 1
            || marker[0] != 1
            || delivered != 1
            || descriptors[0] < 0
            || message.msg_flags & libc::MSG_CTRUNC != 0
        {
            return Err(io::Error::other(
                "ancillary frame does not contain its one exact original",
            ));
        }
        Ok(descriptors[0])
    }
}
