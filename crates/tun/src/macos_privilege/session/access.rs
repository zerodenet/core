//! Only a weak observation crosses the actual provider/Source handoff.
use super::*;
use std::{cell::RefCell, process::Output};
#[derive(Clone)]
struct Entered {
    owner: MacosHelperOwner,
    cleanup: bool,
}
thread_local! {static CURRENT:RefCell<Option<Entered>>=const{RefCell::new(None)};}
pub struct MacosHelperContext {
    previous: Option<Entered>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl MacosHelperAccess {
    pub fn lease_routes(&self) -> io::Result<MacosHelperLease> {
        MacosHelperOwner(
            self.0
                .upgrade()
                .ok_or_else(|| io::Error::other("actual helper owner expired"))?,
        )
        .lease_routes()
    }
    pub fn enter(&self) -> io::Result<MacosHelperContext> {
        MacosHelperOwner(
            self.0
                .upgrade()
                .ok_or_else(|| io::Error::other("actual helper owner expired"))?,
        )
        .enter()
    }
    pub fn require_closed(&self) -> io::Result<()> {
        // Weak expiry is never native closure or an absent-source fallback.
        MacosHelperOwner(self.0.upgrade().ok_or_else(|| {
            io::Error::other("actual helper owner expired before close observation")
        })?)
        .require_closed()
    }
    fn prepare_cleanup(&self) -> io::Result<MacosHelperContext> {
        let owner = MacosHelperOwner(
            self.0
                .upgrade()
                .ok_or_else(|| io::Error::other("actual helper owner expired"))?,
        );
        owner.0.prepare_cleanup()?;
        owner.enter_mode(true)
    }
}
impl MacosHelperOwner {
    pub fn enter(&self) -> io::Result<MacosHelperContext> {
        self.enter_mode(false)
    }
    pub(super) fn enter_mode(&self, cleanup: bool) -> io::Result<MacosHelperContext> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot
                .as_ref()
                .is_some_and(|current| !current.owner.same_owner(self))
            {
                return Err(io::Error::other(
                    "helper context belongs to another actual native owner",
                ));
            }
            let cleanup = cleanup || slot.as_ref().is_some_and(|current| current.cleanup);
            if self.0.stopped.load(Ordering::Acquire) && !cleanup {
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            let previous = slot.replace(Entered {
                owner: self.clone(),
                cleanup,
            });
            Ok(MacosHelperContext {
                previous,
                _thread: std::marker::PhantomData,
            })
        })
    }
    pub(in crate::macos_privilege) fn output(
        &self,
        program: HelperProgram,
        arguments: &[String],
        input: Option<&str>,
    ) -> io::Result<Output> {
        if !self.0.io_allowed() {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        let _notifications = self.0.memory.defer_notifications();
        self.0.prepare_actor_wake()?;
        let index = self.0.current.load(Ordering::Acquire);
        let mut epoch = self.0.epochs[index]
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if unsafe { libc::geteuid() } == 0 {
            return tool::run(
                &mut epoch.local_tool,
                program,
                arguments,
                input,
                None,
                self.0.output_limit,
            );
        }
        epoch.connect()?;
        if epoch.rpc.pending() {
            return Err(io::Error::other(
                "prior command continuation still owns this helper",
            ));
        }
        let next = epoch
            .command
            .checked_add(1)
            .ok_or_else(|| io::Error::other("helper command sequence exhausted"))?;
        let nonce = epoch
            .id
            .checked_add(next)
            .ok_or_else(|| io::Error::other("helper command nonce overflow"))?;
        let request = HelperRequest::OwnedCommand {
            program,
            arguments: arguments.to_vec(),
            input: input.map(str::to_owned),
            nonce,
            output_limit: self.0.output_limit,
        };
        epoch.ensure_request_capacity(&request, &self.0.memory)?;
        epoch.command = next;
        epoch.tools.begin(nonce)?;
        epoch.rpc.begin(&request, Some(nonce))?;
        if let Err(source) = epoch.drive_rpc() {
            self.0.retain(index, Phase::ToolProgress, source);
            return Err(failures::retained(self.0.failures.clone().into_error()));
        }
        let response = epoch.rpc.take_response()?;
        Ok(Output {
            status: super::super::exit_status(response.success, response.code),
            stdout: response.stdout,
            stderr: response.stderr,
        })
    }
}
impl Drop for MacosHelperContext {
    fn drop(&mut self) {
        CURRENT.with(|slot| {
            let retired = slot.replace(self.previous.take());
            // No RefCell/owner lock is held while the paid header can refund/wake.
            drop(retired);
        });
    }
}
impl MacosHelperLease {
    /// The original route duty owns its own cleanup epoch. With no dedicated
    /// initial helper task (borrowed/Descriptor or UID0), this includes epoch
    /// zero: no other actor can retire its tools/control/transfer originals.
    pub fn finish_cleanup(&self) -> io::Result<()> {
        let index = self.owner.0.current.load(Ordering::Acquire);
        self.owner.0.close_returned_route_files();
        if index == 0 && self.owner.0.dedicated_worker {
            // Only the actually installed initial helper actor owns this one.
            return Ok(());
        }
        loop {
            let result = self.owner.finish_registered_epoch(index);
            if self.owner.require_closed().is_ok() {
                return result;
            }
            if let Err(source) = result {
                self.owner.0.retain(index, Phase::Shutdown, source);
            }
            self.owner.wait_original_progress(index);
        }
    }
    pub fn require_closed(&self) -> io::Result<()> {
        self.owner.require_closed()
    }
    pub fn enter(&self) -> io::Result<MacosHelperContext> {
        self.owner.enter()
    }
    pub fn prepare_cleanup(&self) -> io::Result<MacosHelperContext> {
        self.owner.access().prepare_cleanup()
    }
}
pub(in crate::macos_privilege) fn current() -> Option<MacosHelperOwner> {
    CURRENT.with(|slot| slot.borrow().as_ref().map(|current| current.owner.clone()))
}

impl Session {
    pub(super) fn io_allowed(&self) -> bool {
        if !self.stopped.load(Ordering::Acquire) {
            return true;
        }
        CURRENT.with(|slot| {
            slot.borrow().as_ref().is_some_and(|current| {
                std::ptr::eq(current.owner.0.as_ref(), self) && current.cleanup
            })
        })
    }
}
