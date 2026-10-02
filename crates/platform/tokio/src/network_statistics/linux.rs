use super::{procfs, MAX_BYTES};
use std::{
    ffi::CString,
    io::{self, Read},
};
pub(super) fn read() -> io::Result<Vec<super::InterfaceStatistics>> {
    let mut text = String::new();
    std::fs::File::open("/proc/net/dev")?
        .take((MAX_BYTES + 1) as u64)
        .read_to_string(&mut text)?;
    procfs::parse(&text, |name| {
        let name = CString::new(name).map_err(|_| super::invalid("invalid interface name"))?;
        // SAFETY: valid nul-terminated name, no retained pointers.
        let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        if index == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(index)
        }
    })
}
