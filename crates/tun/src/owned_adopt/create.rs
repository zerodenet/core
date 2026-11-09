//! Acquire raw originals before fallible system-TUN initialization.
use super::*;

impl AdoptedTunOwner {
    /// The same original slot also owns Unix system-TUN creation. No
    /// configuration failure can hide its acquired descriptor behind Drop.
    pub fn create(
        self: &Arc<Self>,
        name: Option<&str>,
    ) -> Result<AdoptedTunIo, TunAcquisitionError> {
        #[cfg(target_os = "macos")]
        if self.macos_helper.get().is_some() && unsafe { libc::geteuid() } != 0 {
            return self.create_with_retained_helper(name);
        }
        #[cfg(target_os = "linux")]
        let remote = false;
        #[cfg(target_os = "macos")]
        let remote =
            unsafe { libc::geteuid() } != 0 && Backend::validate_created_name(name).is_ok();
        self.create_with(
            name,
            remote,
            Backend::open_created,
            Backend::initialize_created,
        )
    }
    pub(super) fn create_with(
        self: &Arc<Self>,
        name: Option<&str>,
        remote: bool,
        open: impl FnOnce(Option<&str>) -> io::Result<OwnedFd>,
        initialize: impl FnOnce(i32, Option<&str>) -> io::Result<String>,
    ) -> Result<AdoptedTunIo, TunAcquisitionError> {
        #[cfg(target_os = "macos")]
        let _notifications = self.macos_memory.get().map(|memory| memory.notifications());
        let mut state = self.lock();
        if !matches!(*state, State::Cold) {
            return Err(TunAcquisitionError {
                outcome: state.outcome(),
                source: Arc::new(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "TUN original already attempted",
                )),
            });
        }
        *state = State::Acquiring;
        let fd = match open(name) {
            Ok(fd) => fd,
            Err(source) => {
                let source = Arc::new(source);
                self.remember_failure(source.clone());
                *state = if remote {
                    // The helper may have acquired an original before a failed
                    // transfer. No local descriptor is not proof of no native.
                    State::Unknown {
                        identity: None,
                        source: source.clone(),
                    }
                } else {
                    State::NoNative(source.clone())
                };
                return Err(TunAcquisitionError {
                    outcome: state.outcome(),
                    source,
                });
            }
        };
        let identity = OwnedTunIdentity::from_fd(fd.as_raw_fd());
        *state = State::Raw {
            fd,
            identity: identity.clone(),
        };
        let checked = match &*state {
            State::Raw { fd, .. } => initialize(fd.as_raw_fd(), name),
            _ => unreachable!(),
        };
        let interface = match checked {
            Ok(interface) => interface,
            Err(source) => {
                let source = Arc::new(source);
                self.remember_failure(source.clone());
                let _ = close_state(&mut state, name.unwrap_or(""));
                return Err(TunAcquisitionError {
                    outcome: state.outcome(),
                    source,
                });
            }
        };
        let State::Raw { fd, .. } = std::mem::replace(
            &mut *state,
            State::Unknown {
                identity: Some(identity.clone()),
                source: Arc::new(io::Error::other(
                    "system TUN registration has no confirmed native outcome",
                )),
            },
        ) else {
            unreachable!()
        };
        match self.register_backend(fd, interface.clone(), identity.clone()) {
            Ok(device) => {
                *state = State::Live {
                    device,
                    identity: identity.clone(),
                };
                self.created
                    .store(true, std::sync::atomic::Ordering::Release);
                Ok(AdoptedTunIo {
                    owner: self.clone(),
                    identity,
                    name: interface,
                })
            }
            Err((fd, error)) => {
                let source = Arc::new(error);
                self.remember_failure(source.clone());
                *state = State::Raw { fd, identity };
                let _ = close_state(&mut state, &interface);
                Err(TunAcquisitionError {
                    outcome: state.outcome(),
                    source,
                })
            }
        }
    }
}
