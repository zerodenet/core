use std::ffi::c_void;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::watch;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::NetworkManagement::IpHelper::{
    CancelMibChangeNotify2, NotifyIpInterfaceChange, NotifyRouteChange2, MIB_IPFORWARD_ROW2,
    MIB_IPINTERFACE_ROW, MIB_NOTIFICATION_TYPE,
};
use windows_sys::Win32::Networking::WinSock::AF_UNSPEC;

#[derive(Debug)]
struct CallbackContext {
    generation: AtomicU64,
    sender: watch::Sender<u64>,
}

#[derive(Debug)]
pub(super) struct RouteChangeMonitor {
    context: Arc<CallbackContext>,
    receiver: watch::Receiver<u64>,
    route_handle: usize,
    interface_handle: usize,
}

impl RouteChangeMonitor {
    pub(super) fn new() -> io::Result<Self> {
        let (sender, receiver) = watch::channel(0);
        let context = Arc::new(CallbackContext {
            generation: AtomicU64::new(0),
            sender,
        });
        let callback_context = Arc::as_ptr(&context).cast::<c_void>();
        let mut route_handle: HANDLE = std::ptr::null_mut();
        let result = unsafe {
            NotifyRouteChange2(
                AF_UNSPEC,
                Some(route_changed),
                callback_context,
                false,
                &mut route_handle,
            )
        };
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }

        let mut interface_handle: HANDLE = std::ptr::null_mut();
        let result = unsafe {
            NotifyIpInterfaceChange(
                AF_UNSPEC,
                Some(interface_changed),
                callback_context,
                false,
                &mut interface_handle,
            )
        };
        if result != 0 {
            unsafe {
                CancelMibChangeNotify2(route_handle);
            }
            return Err(io::Error::from_raw_os_error(result as i32));
        }

        Ok(Self {
            context,
            receiver,
            route_handle: route_handle as usize,
            interface_handle: interface_handle as usize,
        })
    }

    pub(super) async fn changed(&mut self) -> io::Result<()> {
        self.receiver.changed().await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Windows route notification channel closed",
            )
        })
    }

    pub(super) fn coalesce(&mut self) -> io::Result<()> {
        self.receiver.borrow_and_update();
        Ok(())
    }
}

impl Drop for RouteChangeMonitor {
    fn drop(&mut self) {
        unsafe {
            CancelMibChangeNotify2(self.route_handle as HANDLE);
            CancelMibChangeNotify2(self.interface_handle as HANDLE);
        }
        // Keep the callback context alive until both registrations have been
        // cancelled. The field is intentionally read here to make that
        // lifetime relationship explicit.
        let _ = Arc::strong_count(&self.context);
    }
}

unsafe extern "system" fn route_changed(
    context: *const c_void,
    _row: *const MIB_IPFORWARD_ROW2,
    _notification: MIB_NOTIFICATION_TYPE,
) {
    signal(context);
}

unsafe extern "system" fn interface_changed(
    context: *const c_void,
    _row: *const MIB_IPINTERFACE_ROW,
    _notification: MIB_NOTIFICATION_TYPE,
) {
    signal(context);
}

unsafe fn signal(context: *const c_void) {
    let Some(context) = (context as *const CallbackContext).as_ref() else {
        return;
    };
    let generation = context.generation.fetch_add(1, Ordering::Relaxed) + 1;
    context.sender.send_replace(generation);
}
