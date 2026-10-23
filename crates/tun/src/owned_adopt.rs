//! One persistent original host-device duplicate, including failed acquisition.
use crate::{OwnedTunDevice, OwnedTunIdentity, TunCloseReceipt};
use std::{
    io,
    os::fd::{AsRawFd, OwnedFd},
    sync::{Arc, Mutex},
};
#[cfg(target_os = "linux")]
type Backend = crate::LinuxTun;
#[cfg(target_os = "macos")]
type Backend = crate::Utun;
mod io_view;
pub use io_view::AdoptedTunIo;

#[derive(Clone, Debug)]
pub enum TunAcquisitionOutcome {
    /// The native duplication call was never entered or returned no descriptor.
    KnownNoNative,
    /// Explicit successful close of this original duplicate.
    Closed(Arc<TunCloseReceipt>),
    /// No close confirmation, never permission to reuse a descriptor integer.
    Unknown(Option<OwnedTunIdentity>),
}
#[derive(Debug)]
pub struct TunAcquisitionError {
    pub outcome: TunAcquisitionOutcome,
    pub source: Arc<io::Error>,
}
impl std::fmt::Display for TunAcquisitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for TunAcquisitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
/// Retain this owner in the actual candidate before calling acquire(). There
/// is one reservation, not a retry pool. Unknown originals remain charged.
pub struct AdoptedTunOwner {
    state: Mutex<State>,
    acquisition_failure: Mutex<Option<Arc<io::Error>>>,
}
enum State {
    Cold,
    Acquiring,
    Raw {
        fd: OwnedFd,
        identity: OwnedTunIdentity,
    },
    Live {
        device: Backend,
        identity: OwnedTunIdentity,
    },
    Closed(Arc<TunCloseReceipt>),
    NoNative(Arc<io::Error>),
    Unknown {
        identity: Option<OwnedTunIdentity>,
        source: Arc<io::Error>,
    },
}
impl AdoptedTunOwner {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::Cold),
            acquisition_failure: Mutex::new(None),
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn acquire(
        self: &Arc<Self>,
        fd: i32,
        expected: &str,
    ) -> Result<AdoptedTunIo, TunAcquisitionError> {
        let mut state = self.lock();
        if !matches!(*state, State::Cold) {
            let source = match &*state {
                State::Unknown { source, .. } | State::NoNative(source) => source.clone(),
                _ => Arc::new(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "host device owner already attempted acquisition",
                )),
            };
            return Err(TunAcquisitionError {
                outcome: state.outcome(),
                source,
            });
        }
        *state = State::Acquiring;
        let fd = match crate::adopt::duplicate(fd) {
            Ok(fd) => fd,
            Err(error) => {
                let source = Arc::new(error);
                self.remember_failure(source.clone());
                *state = State::NoNative(source.clone());
                return Err(TunAcquisitionError {
                    outcome: TunAcquisitionOutcome::KnownNoNative,
                    source,
                });
            }
        };
        let identity = OwnedTunIdentity::from_fd(fd.as_raw_fd());
        *state = State::Raw {
            fd,
            identity: identity.clone(),
        };
        // The actual owner already holds the original before ioctl/fcntl or
        // the consuming async-registration boundary can fail.
        let checked = match &*state {
            State::Raw { fd, .. } => Backend::validate_adopt(fd.as_raw_fd(), expected),
            _ => unreachable!("same synchronous original owner"),
        };
        let name = match checked {
            Ok(name) => name,
            Err(error) => {
                let source = Arc::new(error);
                self.remember_failure(source.clone());
                let _ = close_state(&mut state, expected);
                return Err(TunAcquisitionError {
                    outcome: state.outcome(),
                    source,
                });
            }
        };
        let unknown = Arc::new(io::Error::other(
            "host TUN async registration has no confirmed native outcome",
        ));
        let State::Raw { fd, .. } = std::mem::replace(
            &mut *state,
            State::Unknown {
                identity: Some(identity.clone()),
                source: unknown,
            },
        ) else {
            unreachable!("original installed before registration")
        };
        match Backend::register_adopted(fd, name.clone(), identity.clone()) {
            Ok(device) => {
                *state = State::Live {
                    device,
                    identity: identity.clone(),
                };
                Ok(AdoptedTunIo {
                    owner: self.clone(),
                    identity,
                    name,
                })
            }
            Err(error) => {
                let source = Arc::new(error);
                self.remember_failure(source.clone());
                *state = State::Unknown {
                    identity: Some(identity.clone()),
                    source: source.clone(),
                };
                Err(TunAcquisitionError {
                    outcome: TunAcquisitionOutcome::Unknown(Some(identity)),
                    source,
                })
            }
        }
    }
    fn remember_failure(&self, source: Arc<io::Error>) {
        let mut first = self
            .acquisition_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if first.is_none() {
            *first = Some(source);
        }
    }
    /// Original failed acquisition, separate from a subsequent native-close error.
    pub fn acquisition_failure(&self) -> Option<Arc<io::Error>> {
        self.acquisition_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    /// Read-only actual outcome; it never consumes a handle or retries Unknown.
    pub fn outcome(&self) -> TunAcquisitionOutcome {
        self.lock().outcome()
    }
    pub fn close(&self) -> Result<Arc<TunCloseReceipt>, Arc<io::Error>> {
        close_state(&mut self.lock(), "")
    }
}
impl State {
    fn outcome(&self) -> TunAcquisitionOutcome {
        match self {
            Self::Cold | Self::NoNative(_) => TunAcquisitionOutcome::KnownNoNative,
            Self::Closed(receipt) => TunAcquisitionOutcome::Closed(receipt.clone()),
            Self::Raw { identity, .. } | Self::Live { identity, .. } => {
                TunAcquisitionOutcome::Unknown(Some(identity.clone()))
            }
            Self::Unknown { identity, .. } => TunAcquisitionOutcome::Unknown(identity.clone()),
            Self::Acquiring => TunAcquisitionOutcome::Unknown(None),
        }
    }
}
fn close_state(state: &mut State, interface: &str) -> Result<Arc<TunCloseReceipt>, Arc<io::Error>> {
    match state {
        State::Closed(receipt) => return Ok(receipt.clone()),
        State::NoNative(source) => return Err(source.clone()),
        State::Unknown { source, .. } => return Err(source.clone()),
        State::Cold => {
            return Err(Arc::new(io::Error::new(
                io::ErrorKind::NotConnected,
                "host device has not acquired an original",
            )))
        }
        _ => {}
    }
    let identity = match state {
        State::Raw { identity, .. } | State::Live { identity, .. } => Some(identity.clone()),
        _ => None,
    };
    let original = std::mem::replace(
        state,
        State::Unknown {
            identity: identity.clone(),
            source: Arc::new(io::Error::other(
                "host TUN consuming close has no confirmed native outcome",
            )),
        },
    );
    let result = match original {
        State::Raw { fd, identity } => crate::owned::close_fd(fd, interface.into(), identity),
        State::Live { device, .. } => device.close_owned(),
        _ => Err(io::Error::other("host TUN acquisition remains unconfirmed")),
    };
    match result {
        Ok(receipt) => {
            let receipt = Arc::new(receipt);
            *state = State::Closed(receipt.clone());
            Ok(receipt)
        }
        Err(error) => {
            let source = Arc::new(error);
            *state = State::Unknown {
                identity,
                source: source.clone(),
            };
            Err(source)
        }
    }
}

#[cfg(test)]
mod tests;
