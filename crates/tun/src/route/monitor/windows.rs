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
    route_handle: Option<usize>,
    interface_handle: Option<usize>,
    attempted: bool,
    closed: bool,
}

impl RouteChangeMonitor {
    pub(super) fn prepare() -> Self {
        let (sender, receiver) = watch::channel(0);
        Self {
            context: Arc::new(CallbackContext {
                generation: AtomicU64::new(0),
                sender,
            }),
            receiver,
            route_handle: None,
            interface_handle: None,
            attempted: false,
            closed: false,
        }
    }
    pub(super) fn acquire(&mut self) -> io::Result<()> {
        if self.attempted || self.closed {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "route monitor registration was already attempted",
            ));
        }
        self.attempted = true;
        let callback_context = Arc::as_ptr(&self.context).cast::<c_void>();
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
        if !route_handle.is_null() {
            self.route_handle = Some(route_handle as usize);
        }
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
        if !interface_handle.is_null() {
            self.interface_handle = Some(interface_handle as usize);
        }
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        Ok(())
    }
    pub(super) fn close(&mut self) -> io::Result<()> {
        let mut failure = None;
        for original in [&mut self.interface_handle, &mut self.route_handle] {
            if let Some(handle) = *original {
                let status = unsafe { CancelMibChangeNotify2(handle as HANDLE) };
                if status == 0 {
                    original.take();
                } else {
                    failure.get_or_insert_with(|| io::Error::from_raw_os_error(status as i32));
                }
            }
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        self.closed = true;
        Ok(())
    }
    pub(super) fn is_closed(&self) -> bool {
        self.closed
    }

    pub(super) async fn changed(&mut self) -> io::Result<()> {
        if self.closed || self.route_handle.is_none() || self.interface_handle.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "route monitor registration is incomplete or closed",
            ));
        }
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
        if self.close().is_err() {
            // Compatibility callers may drop a failed close. An active native
            // callback must never point at freed context. Managed owners retain
            // this entire original and never use Drop as close evidence.
            let _ = Arc::into_raw(self.context.clone());
        }
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
