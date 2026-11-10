use super::*;
impl MacosHelperOwner {
    pub(in crate::macos_privilege::session) fn acquire_route_lock(
        &self,
        path: &Path,
        ipv6: bool,
    ) -> io::Result<MacosRouteLock> {
        let mut original = None;
        self.acquire_route_lock_retained(path, ipv6, &mut original)?;
        Ok(original.expect("same acquired original route lock"))
    }
    pub(super) fn acquire_route_lock_retained(
        &self,
        path: &Path,
        ipv6: bool,
        original: &mut Option<MacosRouteLock>,
    ) -> io::Result<()> {
        if original.is_some() {
            return Err(io::Error::other(
                "route file output already owns an original",
            ));
        }
        if self.0.stopped.load(Ordering::Acquire) {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        let _notifications = self.0.memory.defer_notifications();
        let path = transient::native_path(path)?;
        let mut files = self
            .0
            .files
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        let family = usize::from(ipv6);
        let slot = &mut files.locks[family];
        if slot.held
            || slot
                .original
                .as_ref()
                .is_some_and(|original| !original.closed())
        {
            return Err(io::Error::other(
                "same family file original still owes its close",
            ));
        }
        let serial = slot
            .serial
            .checked_add(1)
            .ok_or_else(|| io::Error::other("route file original identity exhausted"))?;
        // Publish the exact family/serial access before the native call. A
        // returned open error is positive no-acquisition for this serial;
        // flock/close errors retain this same Session original thereafter.
        slot.serial = serial;
        slot.original = None;
        slot.unlock = None;
        *original = Some(MacosRouteLock {
            owner: PacketMetadataArc::downgrade(&self.0),
            family,
            serial,
        });
        let fd = unsafe {
            libc::open(
                path.value().as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_CLOEXEC,
                0o600,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        slot.original = Some(Original::new(fd, serial));
        slot.serial = serial; // before flock
        slot.unlock = None;
        let borrowed = unsafe { ManuallyDrop::new(File::from_raw_fd(fd)) };
        if let Err(source) = fs2::FileExt::try_lock_exclusive(&*borrowed) {
            if let Err(close) = slot.original.as_mut().unwrap().close_once() {
                self.0.failures.retain(
                    0,
                    if ipv6 {
                        Phase::RouteLockAcquireV6
                    } else {
                        Phase::RouteLockAcquireV4
                    },
                    source,
                );
                self.0.retain_file_failure(family, close);
                return Err(failures::retained(self.0.failures.clone().into_error()));
            }
            return Err(source);
        }
        slot.held = true;
        Ok(())
    }
}
