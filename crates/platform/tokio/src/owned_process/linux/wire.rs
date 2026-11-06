//! A bounded private seqpacket conversation with the one native job supervisor.
//! No protocol arguments, environment values or credentials cross this channel.
use std::io;
pub(super) const BEGIN: i32 = 1;
pub(super) const STOP: i32 = 2;
pub(super) const ROOT_HANDLE: i32 = 3;
pub(super) const ROOT_INSTALLED: i32 = 4;
pub(super) const STARTED: i32 = 5;
pub(super) const ROOT_EXIT: i32 = 6;
pub(super) const FAULT: i32 = 7;
pub(super) const TREE_ENDED: i32 = 8;
const MAGIC: i32 = 0x5a505243;
#[derive(Clone, Copy)]
pub(super) struct Frame(pub(super) [i32; 8]);
impl Frame {
    pub(super) fn new(token: u64, kind: i32, value: i32, pid: i32) -> Self {
        Self([
            MAGIC,
            1,
            token as i32,
            (token >> 32) as i32,
            kind,
            value,
            pid,
            0,
        ])
    }
    pub(super) fn matches(&self, token: u64) -> bool {
        self.0[0] == MAGIC
            && self.0[1] == 1
            && self.0[2] == token as i32
            && self.0[3] == (token >> 32) as i32
            && self.0[7] == 0
    }
    pub(super) fn kind(&self) -> i32 {
        self.0[4]
    }
    pub(super) fn value(&self) -> i32 {
        self.0[5]
    }
    pub(super) fn pid(&self) -> i32 {
        self.0[6]
    }
}
pub(super) struct Message {
    pub(super) frame: Frame,
    pub(super) descriptors: [i32; 32],
    pub(super) count: usize,
    pub(super) malformed: bool,
    pub(super) eof: bool,
}
// SAFETY: callers retain each original fd while these synchronous syscalls run.
// Both post-fork and parent paths use initialized POD and bounded stack storage.
pub(super) unsafe fn send(fd: i32, frame: Frame, descriptor: Option<i32>) -> i32 {
    unsafe {
        let mut data = frame.0;
        let mut iov = libc::iovec {
            iov_base: data.as_mut_ptr().cast(),
            iov_len: std::mem::size_of_val(&data),
        };
        let mut control = [0usize; 8];
        let mut message: libc::msghdr = std::mem::zeroed();
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        if let Some(descriptor) = descriptor {
            message.msg_control = control.as_mut_ptr().cast();
            message.msg_controllen = libc::CMSG_SPACE(std::mem::size_of::<i32>() as u32) as usize;
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<i32>() as u32) as usize;
            std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<i32>(), descriptor);
        }
        loop {
            let result = libc::sendmsg(fd, &message, libc::MSG_NOSIGNAL);
            if result == std::mem::size_of_val(&data) as isize {
                return 0;
            }
            let error = *libc::__errno_location();
            if result < 0 && error == libc::EINTR {
                continue;
            }
            return if result < 0 { error } else { libc::EIO };
        }
    }
}
pub(super) unsafe fn receive(fd: i32) -> Result<Message, i32> {
    unsafe {
        let mut result = Message {
            frame: Frame([0; 8]),
            descriptors: [-1; 32],
            count: 0,
            malformed: false,
            eof: false,
        };
        let mut iov = libc::iovec {
            iov_base: result.frame.0.as_mut_ptr().cast(),
            iov_len: std::mem::size_of::<Frame>(),
        };
        let mut control = [0usize; 16];
        let mut message: libc::msghdr = std::mem::zeroed();
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        message.msg_controllen = std::mem::size_of_val(&control);
        let size = loop {
            let size = libc::recvmsg(fd, &mut message, libc::MSG_CMSG_CLOEXEC);
            if size < 0 && *libc::__errno_location() == libc::EINTR {
                continue;
            }
            break size;
        };
        if size < 0 {
            return Err(*libc::__errno_location());
        }
        result.eof = size == 0;
        result.malformed = size != std::mem::size_of::<Frame>() as isize
            || message.msg_flags & (libc::MSG_TRUNC | libc::MSG_CTRUNC) != 0;
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let minimum = libc::CMSG_LEN(0) as usize;
                if (*header).cmsg_len < minimum {
                    result.malformed = true;
                    break;
                }
                let count = ((*header).cmsg_len - minimum) / std::mem::size_of::<i32>();
                let mut index = 0;
                while index < count {
                    let descriptor =
                        std::ptr::read_unaligned(libc::CMSG_DATA(header).cast::<i32>().add(index));
                    // The complete ancillary buffer can hold at most 28
                    // descriptors. All are retained before semantic validation;
                    // malformed messages cannot discard unknown originals.
                    result.descriptors[result.count] = descriptor;
                    result.count += 1;
                    index += 1;
                }
            } else {
                result.malformed = true;
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
        Ok(result)
    }
}
pub(super) fn io_error(errno: i32) -> io::Error {
    io::Error::from_raw_os_error(errno)
}
