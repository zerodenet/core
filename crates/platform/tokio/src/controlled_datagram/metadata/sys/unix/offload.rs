use std::io;

use socket2::SockRef;

use super::{cmsg, ProbeKind};

#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(target_os = "android")
))]
const UDP_SEGMENT: libc::c_int = libc::UDP_SEGMENT;
#[cfg(target_os = "android")]
const UDP_SEGMENT: libc::c_int = 103;

#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(target_os = "android")
))]
pub(super) const UDP_GRO: libc::c_int = libc::UDP_GRO;
#[cfg(target_os = "android")]
pub(super) const UDP_GRO: libc::c_int = 104;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) fn set_segment_size(encoder: &mut cmsg::Encoder<'_, libc::msghdr>, segment_size: u16) {
    encoder.push(libc::SOL_UDP, UDP_SEGMENT, segment_size);
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub(super) fn set_segment_size(_encoder: &mut cmsg::Encoder<'_, libc::msghdr>, _segment_size: u16) {
}

pub(super) fn capability_without_probe(kind: ProbeKind) -> Option<usize> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        match kind {
            ProbeKind::Gso if !linux_gso::requires_probe() => Some(1),
            _ => None,
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = kind;
        Some(1)
    }
}

pub(super) fn probe_owned_socket(kind: ProbeKind, socket: SockRef<'_>) -> io::Result<usize> {
    if let Some(value) = capability_without_probe(kind) {
        return Ok(value);
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        match kind {
            ProbeKind::Gso => {
                if !linux_gso::requires_probe() {
                    Ok(1)
                } else {
                    super::set_socket_option(&*socket, libc::SOL_UDP, UDP_SEGMENT, 1500)
                        .map(|()| 64)
                }
            }
            ProbeKind::Gro => {
                super::set_socket_option(&*socket, libc::SOL_UDP, UDP_GRO, super::OPTION_ON)
                    .map(|()| 64)
            }
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = socket;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "platform requires no UDP probe",
        ))
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux_gso {
    use std::{ffi::CStr, io, mem, str::FromStr, sync::OnceLock};

    const SUPPORTED_SINCE: (u8, u8) = (4, 18);
    static SUPPORTED: OnceLock<bool> = OnceLock::new();

    pub(super) fn requires_probe() -> bool {
        *SUPPORTED.get_or_init(supported_by_current_kernel)
    }

    fn supported_by_current_kernel() -> bool {
        let Ok(release) = kernel_version_string() else {
            return false;
        };
        parse_kernel_version(&release).is_some_and(|version| version >= SUPPORTED_SINCE)
    }

    fn kernel_version_string() -> io::Result<String> {
        let mut n: libc::utsname = unsafe { mem::zeroed() };
        if unsafe { libc::uname(&mut n) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe {
            CStr::from_ptr(n.release.as_ptr())
                .to_string_lossy()
                .into_owned()
        })
    }

    fn parse_kernel_version(release: &str) -> Option<(u8, u8)> {
        let base = release.split_once('-').map_or(release, |(head, _)| head);
        let mut parts = base.split('.');
        let major = u8::from_str(parts.next()?).ok()?;
        let minor = u8::from_str(parts.next()?).ok()?;
        Some((major, minor))
    }
}
