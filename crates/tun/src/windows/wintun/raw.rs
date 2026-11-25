//! Dynamic `LoadLibraryW` / `GetProcAddress` symbol table for `wintun.dll` using `extern "system"`.

use std::{
    ffi::{c_int, OsStr},
    io,
    os::windows::ffi::OsStrExt,
    sync::atomic::{AtomicBool, Ordering},
};
use windows_sys::{
    core::GUID,
    Win32::{
        Foundation::{FreeLibrary, FARPROC, HANDLE, HMODULE},
        System::LibraryLoader::{GetProcAddress, LoadLibraryW},
    },
};

#[repr(C)]
pub struct WintunAdapterOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub struct TunSessionOpaque {
    _private: [u8; 0],
}

pub type WintunAdapterHandle = *mut WintunAdapterOpaque;
pub type WintunSessionHandle = *mut TunSessionOpaque;

const WINTUN_LOG_INFO: c_int = 0;
const WINTUN_LOG_WARN: c_int = 1;
const WINTUN_LOG_ERR: c_int = 2;

pub type WintunLoggerCallback =
    Option<unsafe extern "system" fn(level: c_int, timestamp: u64, message: *const u16)>;

type CreateAdapterFn = unsafe extern "system" fn(
    name: *const u16,
    tunnel_type: *const u16,
    requested_guid: *const GUID,
) -> WintunAdapterHandle;
type CloseAdapterFn = unsafe extern "system" fn(adapter: WintunAdapterHandle);
type OpenAdapterFn = unsafe extern "system" fn(name: *const u16) -> WintunAdapterHandle;
type SetLoggerFn = unsafe extern "system" fn(new_logger: WintunLoggerCallback);
type StartSessionFn =
    unsafe extern "system" fn(adapter: WintunAdapterHandle, capacity: u32) -> WintunSessionHandle;
type EndSessionFn = unsafe extern "system" fn(session: WintunSessionHandle);
type GetReadWaitEventFn = unsafe extern "system" fn(session: WintunSessionHandle) -> HANDLE;
type ReceivePacketFn =
    unsafe extern "system" fn(session: WintunSessionHandle, packet_size: *mut u32) -> *mut u8;
type ReleaseReceivePacketFn =
    unsafe extern "system" fn(session: WintunSessionHandle, packet: *const u8);
type AllocateSendPacketFn =
    unsafe extern "system" fn(session: WintunSessionHandle, packet_size: u32) -> *mut u8;
type SendPacketFn = unsafe extern "system" fn(session: WintunSessionHandle, packet: *const u8);

pub struct WintunLibrary {
    module: HMODULE,
    pub(super) create_adapter: CreateAdapterFn,
    pub(super) close_adapter: CloseAdapterFn,
    pub(super) open_adapter: OpenAdapterFn,
    pub(super) set_logger: SetLoggerFn,
    pub(super) start_session: StartSessionFn,
    pub(super) end_session: EndSessionFn,
    pub(super) get_read_wait_event: GetReadWaitEventFn,
    pub(super) receive_packet: ReceivePacketFn,
    pub(super) release_receive_packet: ReleaseReceivePacketFn,
    pub(super) allocate_send_packet: AllocateSendPacketFn,
    pub(super) send_packet: SendPacketFn,
}

// SAFETY: Loaded Win32 module handle and immutable C function pointers are thread-safe.
unsafe impl Send for WintunLibrary {}
unsafe impl Sync for WintunLibrary {}

struct OwnedModule(HMODULE);

impl Drop for OwnedModule {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                FreeLibrary(self.0);
            }
        }
    }
}

impl WintunLibrary {
    pub(super) unsafe fn open(path: &OsStr) -> io::Result<Self> {
        let wide: Vec<u16> = path.encode_wide().chain(std::iter::once(0)).collect();
        let module = unsafe { LoadLibraryW(wide.as_ptr()) };
        if module.is_null() {
            return Err(io::Error::last_os_error());
        }
        let guard = OwnedModule(module);
        let create_adapter: CreateAdapterFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunCreateAdapter\0")?) };
        let close_adapter: CloseAdapterFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunCloseAdapter\0")?) };
        let open_adapter: OpenAdapterFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunOpenAdapter\0")?) };
        let set_logger: SetLoggerFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunSetLogger\0")?) };
        let start_session: StartSessionFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunStartSession\0")?) };
        let end_session: EndSessionFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunEndSession\0")?) };
        let get_read_wait_event: GetReadWaitEventFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunGetReadWaitEvent\0")?) };
        let receive_packet: ReceivePacketFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunReceivePacket\0")?) };
        let release_receive_packet: ReleaseReceivePacketFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunReleaseReceivePacket\0")?) };
        let allocate_send_packet: AllocateSendPacketFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunAllocateSendPacket\0")?) };
        let send_packet: SendPacketFn =
            unsafe { std::mem::transmute(resolve(module, b"WintunSendPacket\0")?) };
        let module = guard.0;
        std::mem::forget(guard);
        let lib = Self {
            module,
            create_adapter,
            close_adapter,
            open_adapter,
            set_logger,
            start_session,
            end_session,
            get_read_wait_event,
            receive_packet,
            release_receive_packet,
            allocate_send_packet,
            send_packet,
        };
        install_default_logger(&lib);
        Ok(lib)
    }
}

impl Drop for WintunLibrary {
    fn drop(&mut self) {
        if !self.module.is_null() {
            unsafe {
                FreeLibrary(self.module);
            }
            self.module = std::ptr::null_mut();
        }
    }
}

unsafe fn resolve(
    module: HMODULE,
    symbol: &[u8],
) -> io::Result<unsafe extern "system" fn() -> isize> {
    let proc: FARPROC = unsafe { GetProcAddress(module, symbol.as_ptr()) };
    proc.ok_or_else(io::Error::last_os_error)
}

static LOGGER_INSTALLED: AtomicBool = AtomicBool::new(false);

fn install_default_logger(lib: &WintunLibrary) {
    if LOGGER_INSTALLED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        .is_ok()
    {
        unsafe { (lib.set_logger)(Some(default_logger)) };
    }
}

unsafe extern "system" fn default_logger(level: c_int, _timestamp: u64, message: *const u16) {
    if message.is_null() {
        return;
    }
    let mut len = 0usize;
    while unsafe { *message.add(len) } != 0 {
        len += 1;
    }
    let text = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(message, len) });
    match level {
        WINTUN_LOG_INFO => tracing::debug!(target: "zero_tun::wintun", "{text}"),
        WINTUN_LOG_WARN => tracing::warn!(target: "zero_tun::wintun", "{text}"),
        WINTUN_LOG_ERR => tracing::error!(target: "zero_tun::wintun", "{text}"),
        _ => tracing::trace!(target: "zero_tun::wintun", level, "{text}"),
    }
}
