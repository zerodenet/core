//! Concrete helper custody is installed in the same original before any RPC.
use super::*;
use crate::macos_privilege::session::MacosHelperOwner;
use crate::packet_memory::PacketMemoryView;
struct Creating<'a>(&'a AdoptedTunOwner);
impl Drop for Creating<'_> {
    fn drop(&mut self) {
        self.0
            .creating
            .store(false, std::sync::atomic::Ordering::Release);
    }
}
impl AdoptedTunOwner {
    pub fn prepare_macos_helper(
        &self,
        memory: &PacketMemoryView,
        output_limit: usize,
        dedicated_worker: bool,
    ) -> io::Result<crate::MacosHelperAccess> {
        let state = self.lock();
        if !matches!(*state, State::Cold) || self.macos_helper.get().is_some() {
            return Err(io::Error::other(
                "same concrete utun helper already prepared or attempted",
            ));
        }
        // Pure allocations are all paid before helper/listener/Child syscalls.
        let helper = MacosHelperOwner::prepare(memory, output_limit, dedicated_worker)?;
        let access = helper.access();
        self.macos_helper
            .set(helper)
            .map_err(|_| io::Error::other("same helper owner already installed"))?;
        Ok(access)
    }
    pub fn macos_helper(&self) -> Option<crate::MacosHelperAccess> {
        self.macos_helper.get().map(MacosHelperOwner::access)
    }
    pub fn stop_macos_helper(&self) {
        if let Some(helper) = self.macos_helper.get() {
            helper.stop();
        }
    }
    pub fn wait_macos_helper(&self) -> io::Result<()> {
        self.macos_helper
            .get()
            .ok_or_else(|| io::Error::other("actual helper owner missing"))?
            .wait_initial()
    }
    /// Runtime validates its exact original TaskJoinWitness + released actual
    /// registration/failure acceptance before calling the concrete owner.
    pub fn observe_macos_helper_driver_returned(&self) {
        if let Some(helper) = self.macos_helper.get() {
            helper.observe_initial_driver_returned();
        }
    }
    pub fn macos_helper_closed(&self) -> bool {
        self.macos_helper
            .get()
            .is_none_or(|helper| helper.require_closed().is_ok())
    }
    pub(crate) fn retain_macos_transferred(
        &self,
        helper: &MacosHelperOwner,
        fd: std::os::fd::RawFd,
    ) -> io::Result<()> {
        if self
            .macos_helper
            .get()
            .is_none_or(|actual| !actual.same_owner(helper))
        {
            return Err(io::Error::other(
                "transferred original belongs to another actual helper",
            ));
        }
        let mut state = self.lock();
        if !matches!(*state, State::Acquiring)
            || !self.creating.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(io::Error::other(
                "same parent original is not receiving its creation",
            ));
        }
        use std::os::fd::FromRawFd;
        let original = unsafe { OwnedFd::from_raw_fd(fd) };
        let identity = OwnedTunIdentity::from_fd(original.as_raw_fd());
        *state = State::Raw {
            fd: original,
            identity,
        };
        // The actual persistent Raw owner, not an FD integer, now has custody.
        Ok(())
    }
    pub(super) fn create_with_retained_helper(
        self: &Arc<Self>,
        name: Option<&str>,
    ) -> Result<AdoptedTunIo, TunAcquisitionError> {
        let helper = self
            .macos_helper
            .get()
            .expect("selected actual prepared helper");
        {
            let mut state = self.lock();
            if !matches!(*state, State::Cold) {
                return Err(TunAcquisitionError {
                    outcome: state.outcome(),
                    source: Arc::new(io::Error::other("same utun original already attempted")),
                });
            }
            self.creating
                .store(true, std::sync::atomic::Ordering::Release);
            *state = State::Acquiring;
        }
        let _creating = Creating(self);
        let _context = helper
            .enter()
            .map_err(|source| self.fail_created(source, name))?;
        if let Err(source) = helper.acquire_into(self, name) {
            return Err(self.fail_created(source, name));
        }
        let raw = {
            let state = self.lock();
            let State::Raw { fd, .. } = &*state else {
                return Err(TunAcquisitionError {
                    outcome: state.outcome(),
                    source: Arc::new(io::Error::other(
                        "actual transferred original missing after ACK",
                    )),
                });
            };
            fd.as_raw_fd()
        };
        // No state/outcome lock spans helper IO or native initialization. The
        // real creation guard prevents another actor closing this original.
        let interface = Backend::initialize_created(raw, name)
            .map_err(|source| self.fail_created(source, name))?;
        let (fd, identity) = {
            let mut state = self.lock();
            let original = std::mem::replace(&mut *state, State::Acquiring);
            let State::Raw { fd, identity } = original else {
                unreachable!("same creation guard owns original")
            };
            (fd, identity)
        };
        match self.register_backend(fd, interface.clone(), identity.clone()) {
            Ok(device) => {
                *self.lock() = State::Live {
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
            Err((fd, source)) => {
                *self.lock() = State::Raw { fd, identity };
                Err(self.fail_created(source, name))
            }
        }
    }
    fn fail_created(&self, source: io::Error, name: Option<&str>) -> TunAcquisitionError {
        let source = Arc::new(source);
        self.remember_failure(source.clone());
        let mut state = self.lock();
        if matches!(*state, State::Raw { .. }) {
            let _ = close_state(&mut state, name.unwrap_or(""));
        } else if matches!(*state, State::Acquiring) {
            *state = if self
                .macos_helper
                .get()
                .is_some_and(MacosHelperOwner::utun_transfer_settled)
            {
                State::NoNative(source.clone())
            } else {
                State::Unknown {
                    identity: None,
                    source: source.clone(),
                }
            };
        }
        TunAcquisitionError {
            outcome: state.outcome(),
            source,
        }
    }
}
