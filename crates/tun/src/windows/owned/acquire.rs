//! Install every successful original before subsequent fallible initialization.
use super::*;
use std::net::IpAddr;
impl OwnedWintunOwner {
    pub fn open_existing(
        self: &Arc<Self>,
        name: &str,
        addresses: &[IpAddr],
        mtu: u16,
    ) -> io::Result<OwnedWintunIo> {
        self.acquire(Some(name), false, addresses, mtu)
    }
    pub fn create(self: &Arc<Self>, name: Option<&str>) -> io::Result<OwnedWintunIo> {
        self.acquire(name, true, &[], 0)
    }
    fn acquire(
        self: &Arc<Self>,
        name: Option<&str>,
        configure: bool,
        addresses: &[IpAddr],
        mtu: u16,
    ) -> io::Result<OwnedWintunIo> {
        let mut state = self.state();
        if state.attempted || self.closing.load(std::sync::atomic::Ordering::Acquire) {
            return Err(state
                .acquisition_failure
                .clone()
                .map(shared)
                .unwrap_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "Wintun original already attempted",
                    )
                }));
        }
        state.attempted = true;
        let name = name.unwrap_or("ZeroTun");
        state.name = name.to_owned();
        let acquired = (|| {
            super::super::require_elevated_process()?;
            if name.is_empty()
                || name.contains('\0')
                || name.encode_utf16().count() > 128
                || (!configure && addresses.is_empty())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid owned Wintun interface request",
                ));
            }
            let driver = super::super::load_wintun()?;
            let adapter = match wintun::Adapter::open(&driver, name) {
                Ok(adapter) => adapter,
                Err(source) => {
                    // SDK recovery retains the actual original receipt if
                    // GUID discovery failed after OpenAdapter succeeded.
                    let source = io::Error::from(source);
                    if let Some(closed) = source
                        .get_ref()
                        .and_then(|s| s.downcast_ref::<wintun::AdapterOpenError>())
                    {
                        state.adapter_closed = Some(closed.closed().clone());
                    }
                    if !configure {
                        return Err(source);
                    }
                    state.acquisition_failure = Some(Arc::new(source));
                    wintun::Adapter::create(&driver, name, "ZeroTun", None)
                        .map_err(io::Error::from)?
                }
            };
            state.adapter = Some(adapter);
            if !configure {
                super::super::existing::validate_host_configuration(name, addresses, mtu)?;
            }
            let adapter = state.adapter.as_ref().expect("original adapter installed");
            let session = adapter
                .start_session(wintun::MAX_RING_CAPACITY)
                .map_err(io::Error::from)?;
            state.identity = Some(OwnedTunIdentity::from_wintun(
                session.owned_identity(),
                adapter.owned_identity(),
            ));
            state.session = Some(Arc::new(session));
            // A dormant host link becomes usable only after StartSession.
            // The same parent already retains both originals during this wait.
            if !configure {
                let luid = super::super::interface_luid(name)?;
                for &address in addresses {
                    super::super::wait_for_address_ready_with(
                        luid,
                        address,
                        super::super::existing::activating_dad_state_is_ready,
                    )?;
                }
            }
            state.view_live = true;
            Ok(OwnedWintunIo::new(
                self.clone(),
                name.to_owned(),
                configure,
                state.identity.clone().unwrap(),
            ))
        })();
        match acquired {
            Ok(view) => Ok(view),
            Err(source) => {
                let source = Arc::new(source);
                state.acquisition_failure = Some(source.clone());
                Err(shared(source))
            }
        }
    }
}
