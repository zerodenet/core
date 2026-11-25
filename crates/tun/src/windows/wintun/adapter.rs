//! Owned Wintun adapter handle and explicit `CloseAdapterReceipt` evidence.

use super::{
    raw::WintunAdapterHandle,
    session::{Session, SessionIdentity, SessionNotAcquired, SessionStartError},
    Error, Wintun, MAX_RING_CAPACITY, MIN_RING_CAPACITY,
};
use std::{
    fmt, io, ptr,
    sync::{Arc, OnceLock},
};
use windows_sys::{core::GUID, Win32::System::Threading::CreateEventW};

#[derive(Clone, Debug)]
pub struct AdapterIdentity(Arc<IdentityInner>);

#[derive(Debug)]
struct IdentityInner {
    native: usize,
}

impl AdapterIdentity {
    fn new(native: usize) -> Self {
        Self(Arc::new(IdentityInner { native }))
    }

    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Issued only after `WintunCloseAdapter` returns for this exact owned handle.
#[derive(Clone, Debug)]
pub struct CloseAdapterReceipt {
    identity: AdapterIdentity,
}

impl CloseAdapterReceipt {
    pub fn identity(&self) -> &AdapterIdentity {
        &self.identity
    }
}

/// Post-open discovery failure that closed the acquired adapter and retains its receipt.
#[derive(Debug)]
pub struct AdapterOpenError {
    closed: CloseAdapterReceipt,
    source: Error,
}

impl AdapterOpenError {
    pub fn closed(&self) -> &CloseAdapterReceipt {
        &self.closed
    }
}

impl fmt::Display for AdapterOpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "adapter discovery failed after closing its original: {}",
            self.source
        )
    }
}

impl std::error::Error for AdapterOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub struct Adapter {
    adapter: WintunAdapterHandle,
    wintun: Wintun,
    identity: AdapterIdentity,
}

// SAFETY: Wintun adapter handles are synchronized internally by the driver.
unsafe impl Send for Adapter {}
unsafe impl Sync for Adapter {}

impl Adapter {
    pub fn create(
        wintun: &Wintun,
        name: &str,
        tunnel_type: &str,
        guid: Option<u128>,
    ) -> Result<Arc<Self>, Error> {
        let name_utf16: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        let tunnel_type_utf16: Vec<u16> = tunnel_type
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let guid_storage = guid.map(guid_from_u128);
        let guid_ptr = guid_storage.as_ref().map_or(ptr::null(), ptr::from_ref);
        let handle = unsafe {
            (wintun.create_adapter)(name_utf16.as_ptr(), tunnel_type_utf16.as_ptr(), guid_ptr)
        };
        if handle.is_null() {
            Err(Error::Io(io::Error::last_os_error()))
        } else {
            Ok(Arc::new(Self {
                adapter: handle,
                identity: AdapterIdentity::new(handle as usize),
                wintun: wintun.clone(),
            }))
        }
    }

    pub fn open(wintun: &Wintun, name: &str) -> Result<Arc<Self>, Error> {
        let name_utf16: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        let handle = unsafe { (wintun.open_adapter)(name_utf16.as_ptr()) };
        if handle.is_null() {
            Err(Error::Io(io::Error::last_os_error()))
        } else {
            Ok(Arc::new(Self {
                adapter: handle,
                identity: AdapterIdentity::new(handle as usize),
                wintun: wintun.clone(),
            }))
        }
    }

    pub fn start_session(self: &Arc<Self>, capacity: u32) -> Result<Session, Error> {
        if !(MIN_RING_CAPACITY..=MAX_RING_CAPACITY).contains(&capacity) {
            return Err(Error::CapacityOutOfRange {
                min: MIN_RING_CAPACITY,
                max: MAX_RING_CAPACITY,
                value: capacity,
            });
        }
        if !capacity.is_power_of_two() {
            return Err(Error::CapacityNotPowerOfTwo(capacity));
        }
        let handle = unsafe { (self.wintun.start_session)(self.adapter, capacity) };
        if handle.is_null() {
            return Err(Error::Io(io::Error::other(SessionNotAcquired::from_null())));
        }
        // Install the original native session before fallible shutdown-event setup.
        let mut provisional = Session {
            session: handle,
            identity: SessionIdentity::new(handle as usize),
            wintun: self.wintun.clone(),
            read_event: OnceLock::new(),
            shutdown_event: ptr::null_mut(),
            _adapter: Arc::clone(self),
        };
        let event = unsafe { CreateEventW(ptr::null(), 0, 0, ptr::null()) };
        if event.is_null() {
            let source = io::Error::last_os_error();
            let ended = provisional.end_original();
            Err(Error::Io(io::Error::other(SessionStartError::new(
                ended, source,
            ))))
        } else {
            provisional.shutdown_event = event;
            Ok(provisional)
        }
    }

    pub fn owned_identity(&self) -> AdapterIdentity {
        self.identity.clone()
    }

    pub fn close_owned(mut self) -> CloseAdapterReceipt {
        assert_eq!(self.identity.0.native, self.adapter as usize);
        let original = std::mem::replace(&mut self.adapter, ptr::null_mut());
        unsafe { (self.wintun.close_adapter)(original) };
        CloseAdapterReceipt {
            identity: self.identity.clone(),
        }
    }
}

impl Drop for Adapter {
    fn drop(&mut self) {
        if !self.adapter.is_null() {
            unsafe { (self.wintun.close_adapter)(self.adapter) };
            self.adapter = ptr::null_mut();
        }
    }
}

fn guid_from_u128(v: u128) -> GUID {
    GUID {
        data1: (v >> 96) as u32,
        data2: (v >> 80) as u16,
        data3: (v >> 64) as u16,
        data4: (v as u64).to_be_bytes(),
    }
}
