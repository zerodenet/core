//! Install a created original before its first ioctl/connect/registration.
use super::*;

impl Utun {
    pub(crate) fn validate_created_name(name: Option<&str>) -> io::Result<()> {
        requested_unit(name).map(|_| ())
    }
    pub(crate) fn open_created(name: Option<&str>) -> io::Result<OwnedFd> {
        Self::validate_created_name(name)?;
        let fd = if unsafe { libc::geteuid() } == 0 {
            let fd = unsafe { libc::socket(AF_SYSTEM, libc::SOCK_DGRAM, SYSPROTO_CONTROL) };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            fd
        } else {
            // A failed helper transfer is Unknown at the persistent parent
            // owner, since the helper may already have acquired its original.
            crate::macos_privilege::request_utun(name)?
        };
        // SAFETY: either acquisition returned this one transferred original.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
    pub(crate) fn initialize_created(fd: RawFd, name: Option<&str>) -> io::Result<String> {
        if unsafe { libc::geteuid() } == 0 {
            let mut info: libc::ctl_info = unsafe { std::mem::zeroed() };
            for (dst, src) in info.ctl_name.iter_mut().zip(UTUN_CONTROL_NAME.bytes()) {
                *dst = src as libc::c_char;
            }
            if unsafe { libc::ioctl(fd, CTLIOCGINFO, &mut info as *mut _) } < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut address: libc::sockaddr_ctl = unsafe { std::mem::zeroed() };
            address.sc_len = std::mem::size_of::<libc::sockaddr_ctl>() as u8;
            address.sc_family = AF_SYSTEM as u8;
            address.ss_sysaddr = AF_SYS_CONTROL;
            address.sc_id = info.ctl_id;
            address.sc_unit = requested_unit(name)?;
            if unsafe {
                libc::connect(
                    fd,
                    &address as *const _ as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_ctl>() as libc::socklen_t,
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let interface = interface_name(fd)?;
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(interface)
    }
}
