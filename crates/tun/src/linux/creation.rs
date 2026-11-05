//! Native opening and subsequent initialization are separate owner boundaries.
use super::*;
impl LinuxTun {
    /// The acquisition owner installs this raw original before any ioctl.
    pub(crate) fn open_created(name: Option<&str>) -> io::Result<std::os::fd::OwnedFd> {
        let name = name.unwrap_or("tun%d");
        let bytes = CString::new(name).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "interface name contains nul byte",
            )
        })?;
        if bytes.as_bytes_with_nul().len() > libc::IFNAMSIZ {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "interface name exceeds kernel limit",
            ));
        }
        OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/net/tun")
            .map(Into::into)
    }
    pub(crate) fn initialize_created(
        fd: std::os::fd::RawFd,
        name: Option<&str>,
    ) -> io::Result<String> {
        let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
        let bytes = CString::new(name.unwrap_or("tun%d")).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "interface name contains nul byte",
            )
        })?;
        if bytes.as_bytes_with_nul().len() > libc::IFNAMSIZ {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "interface name exceeds kernel limit",
            ));
        }
        for (dst, src) in ifr.ifr_name.iter_mut().zip(bytes.as_bytes_with_nul()) {
            *dst = *src as libc::c_char;
        }
        ifr.ifr_ifru.ifru_flags = (IFF_TUN | IFF_NO_PI) as i16;
        if unsafe { libc::ioctl(fd, libc::TUNSETIFF, &mut ifr) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(ifr
            .ifr_name
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8 as char)
            .collect())
    }
}
