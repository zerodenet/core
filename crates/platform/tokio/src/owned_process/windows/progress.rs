use super::*;
use windows_sys::Win32::{
    Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::{
        JobObjects::{
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
            QueryInformationJobObject, TerminateJobObject,
        },
        Threading::{GetExitCodeProcess, TerminateProcess, WaitForSingleObject},
    },
};

#[derive(Clone, Copy)]
enum Target {
    Started,
    Root,
    Tree,
}
impl OwnedProcess {
    pub async fn wait_started(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        tokio::time::timeout_at(deadline, self.wait(Target::Started))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "original process startup deadline elapsed",
                )
            })?
    }
    pub async fn wait_root_exit(&self) -> io::Result<ExitStatus> {
        let result = self.wait(Target::Root).await;
        if let Some(endpoint) = self.endpoint() {
            endpoint.request_stop();
        }
        result?;
        self.root_status()
            .ok_or_else(|| io::Error::other("original root has no native wait status"))
    }
    pub async fn wait_tree_ended(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        tokio::time::timeout_at(deadline, self.wait(Target::Tree))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "original process tree cleanup deadline elapsed",
                )
            })?
    }
    async fn wait(&self, target: Target) -> io::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.lock();
                state.progress();
                let ready = match target {
                    Target::Tree => {
                        state.tree
                            || (state.originals[PROCESS].raw.is_none()
                                && state.originals[JOB].raw.is_none())
                    }
                    Target::Root => state.root_status.is_some(),
                    Target::Started => state.started,
                };
                if ready {
                    return Ok(());
                }
                if matches!(target, Target::Started | Target::Root) {
                    if let Some(error) = state.first_error.clone() {
                        return Err(io::Error::other(SharedFailure(error)));
                    }
                    if state.stop && matches!(target, Target::Started) {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "process stopped before startup",
                        ));
                    }
                }
            }
            // Same actor, bounded nonblocking native queries. No extra wait
            // worker, thread, Scope or runtime is created for a process job.
            tokio::select! {
                _ = &mut changed => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            }
        }
    }
}
impl State {
    pub(super) fn progress(&mut self) {
        if self.tree {
            return;
        }
        if let Some(process) = self.handle(PROCESS).filter(|_| self.root_status.is_none()) {
            match unsafe { WaitForSingleObject(process, 0) } {
                WAIT_OBJECT_0 => {
                    let mut code = 0;
                    if unsafe { GetExitCodeProcess(process, &mut code) } == 0 {
                        self.fault(io::Error::last_os_error());
                    } else {
                        self.root_status = Some(code);
                        self.stop = true;
                    }
                }
                WAIT_TIMEOUT => {}
                _ => {
                    self.fault(io::Error::last_os_error());
                    self.stop = true;
                }
            }
        }
        let mut active = None;
        if let Some(job) = self.handle(JOB) {
            let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
                unsafe { std::mem::zeroed() };
            if unsafe {
                QueryInformationJobObject(
                    job,
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    std::mem::size_of_val(&accounting) as u32,
                    std::ptr::null_mut(),
                )
            } == 0
            {
                self.fault(io::Error::last_os_error());
                self.stop = true;
            } else {
                active = Some(accounting.ActiveProcesses);
            }
        }
        let no_root = self.handle(PROCESS).is_none();
        if (no_root || self.root_status.is_some())
            && (active == Some(0) || (self.handle(JOB).is_none() && !self.assigned))
        {
            self.tree = true;
            return;
        }
        if self.stop && !self.stop_sent {
            let result = if self.assigned {
                self.handle(JOB)
                    .map(|job| unsafe { TerminateJobObject(job, 1) })
            } else {
                // Assignment failed while the root was still suspended. It
                // could not execute/fork; still consume its actual native wait.
                self.handle(PROCESS)
                    .map(|process| unsafe { TerminateProcess(process, 1) })
            };
            match result {
                Some(0) => self.fault(io::Error::last_os_error()),
                Some(_) => self.stop_sent = true,
                None => {}
            }
        }
    }
}
