//! Only a weak observation crosses the actual provider/Source handoff.
use super::*;
use std::{cell::RefCell, process::Output};
thread_local! {static CURRENT:RefCell<Option<MacosHelperOwner>>=const{RefCell::new(None)};}
pub struct MacosHelperContext {
    previous: Option<MacosHelperOwner>,
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
    pub fn prepare_cleanup(&self) -> io::Result<MacosHelperContext> {
        let owner = MacosHelperOwner(
            self.0
                .upgrade()
                .ok_or_else(|| io::Error::other("actual helper owner expired"))?,
        );
        owner.0.prepare_cleanup()?;
        owner.enter()
    }
}
impl MacosHelperOwner {
    pub fn enter(&self) -> io::Result<MacosHelperContext> {
        CURRENT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot
                .as_ref()
                .is_some_and(|current| !current.same_owner(self))
            {
                return Err(io::Error::other(
                    "helper context belongs to another actual native owner",
                ));
            }
            let previous = slot.replace(self.clone());
            Ok(MacosHelperContext { previous })
        })
    }
    pub(in crate::macos_privilege) fn output(
        &self,
        program: HelperProgram,
        arguments: &[String],
        input: Option<&str>,
    ) -> io::Result<Output> {
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
        epoch.command = epoch
            .command
            .checked_add(1)
            .ok_or_else(|| io::Error::other("helper command sequence exhausted"))?;
        let nonce = epoch
            .id
            .checked_add(epoch.command)
            .ok_or_else(|| io::Error::other("helper command nonce overflow"))?;
        epoch.tools.begin(nonce)?;
        epoch.rpc.begin(
            &HelperRequest::OwnedCommand {
                program,
                arguments: arguments.to_vec(),
                input: input.map(str::to_owned),
                nonce,
                output_limit: self.0.output_limit,
            },
            Some(nonce),
        )?;
        if let Err(source) = epoch.drive_rpc() {
            self.0.retain(index, Phase::ToolProgress, source);
            return Err(io::Error::other(self.0.failures.clone().into_error()));
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
    pub fn enter(&self) -> io::Result<MacosHelperContext> {
        self.owner.enter()
    }
    pub fn prepare_cleanup(&self) -> io::Result<MacosHelperContext> {
        self.owner.access().prepare_cleanup()
    }
}
pub(in crate::macos_privilege) fn current() -> Option<MacosHelperOwner> {
    CURRENT.with(|slot| slot.borrow().clone())
}
