//! The helper owns the actual Child and each output/input pipe before IO.
use super::*;
use std::{
    os::unix::process::CommandExt,
    process::{Child, Command, ExitStatus, Output},
};
mod failures;
mod handshake;
mod pipes;

pub(in crate::macos_privilege) struct ChildOriginal {
    child: Option<Child>,
    pipes: [Option<Original>; 3],
    child_pipes: [Option<Original>; 3],
    preparing: [Option<Original>; 2],
    relocated: [Option<Original>; 2],
    waited: Option<ExitStatus>,
}
impl ChildOriginal {
    pub(in crate::macos_privilege) fn new() -> Self {
        Self {
            child: None,
            pipes: [None, None, None],
            child_pipes: [None, None, None],
            preparing: [None, None],
            relocated: [None, None],
            waited: None,
        }
    }
    pub(in crate::macos_privilege) fn close_pipes(&mut self) -> io::Result<()> {
        let mut failure = None;
        for original in self
            .pipes
            .iter_mut()
            .chain(&mut self.child_pipes)
            .chain(&mut self.preparing)
            .chain(&mut self.relocated)
            .flatten()
        {
            if let Err(source) = original.close_once() {
                failure.get_or_insert(source);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    pub(in crate::macos_privilege) fn finish_local(
        &mut self,
        output_limit: usize,
    ) -> io::Result<()> {
        if self.child.is_none() {
            return self.close_pipes();
        }
        if self.reusable() {
            return Ok(());
        }
        // Same Child/pipes, no new command or guessed process-tree receipt.
        // This fixed local query has no input to reconstruct after interruption.
        let output = self.collect(&[], output_limit);
        let closed = self.close_pipes();
        let waited = self.wait();
        failures::finish(Ok(()), output, closed, waited).map(drop)
    }
    pub(in crate::macos_privilege) fn reaped(&self) -> bool {
        self.waited.is_some()
    }
    pub(in crate::macos_privilege) fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.waited {
            return Ok(status);
        }
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("fixed tool Child not returned by spawn"))?;
        let status = child.wait()?;
        self.waited = Some(status);
        Ok(status)
    }
    pub(in crate::macos_privilege) fn reusable(&self) -> bool {
        (self.child.is_none() || self.reaped())
            && self
                .pipes
                .iter()
                .chain(&self.child_pipes)
                .chain(&self.preparing)
                .chain(&self.relocated)
                .flatten()
                .all(Original::closed)
    }
}
pub(in crate::macos_privilege) fn run(
    slot: &mut ChildOriginal,
    program: HelperProgram,
    arguments: &[String],
    input: Option<&str>,
    supervisor: Option<(RawFd, u64)>,
    output_limit: usize,
) -> io::Result<Output> {
    let executable = match program {
        HelperProgram::Ifconfig => "/sbin/ifconfig",
        HelperProgram::Route => "/sbin/route",
        HelperProgram::Pfctl => "/sbin/pfctl",
    };
    run_fixed(slot, executable, arguments, input, supervisor, output_limit)
}
/// Private fixed executable projection. The only callers are the closed
/// privileged enum above and local metadata queries below; never arbitrary IO.
fn run_fixed(
    slot: &mut ChildOriginal,
    executable: &'static str,
    arguments: &[String],
    input: Option<&str>,
    supervisor: Option<(RawFd, u64)>,
    output_limit: usize,
) -> io::Result<Output> {
    if !slot.reusable() {
        return Err(io::Error::other(
            "previous fixed tool originals are not retired",
        ));
    }
    *slot = ChildOriginal::new();
    let mut command = Command::new(executable);
    command.args(arguments).env("LC_ALL", "C");
    // Parent and child ends enter this exact slot BEFORE flags or spawning.
    // Command owns no implicit Stdio::piped originals in the Kernel path.
    slot.prepare_pipes(input.is_some())?;
    let descriptors = slot
        .child_pipes
        .each_ref()
        .map(|original| original.as_ref().map_or(-1, Original::fd));
    unsafe {
        command.pre_exec(move || {
            for (target, fd) in descriptors.into_iter().enumerate() {
                if fd >= 0 && libc::dup2(fd, target as i32) < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            if let Some((fd, nonce)) = supervisor {
                handshake::announce(fd, nonce)?;
            }
            Ok(())
        });
    }
    let spawned = command.spawn();
    let spawn_failure = match spawned {
        Ok(child) => {
            slot.child = Some(child);
            None
        }
        Err(source) => Some(source),
    };
    let child_ends = slot.close_child_ends();
    if let Some(source) = spawn_failure {
        let closed = slot.close_pipes();
        return Err(failures::spawn_failed(source, closed));
    }
    let output = slot.collect(input.unwrap_or("").as_bytes(), output_limit);
    // Even an IO failure must progress all independent close and wait duties.
    let close = slot.close_pipes();
    let waited = slot.wait();
    failures::finish(child_ends, output, close, waited)
}

pub(in crate::macos_privilege) fn run_query(
    slot: &mut ChildOriginal,
    query: super::Query,
    output_limit: usize,
) -> io::Result<Output> {
    let (executable, arguments) = match query {
        super::Query::Route(arguments) => ("/sbin/route", arguments),
        super::Query::RouteTable(ipv6) => (
            "/usr/sbin/netstat",
            vec![
                "-rnW".into(),
                "-f".into(),
                if ipv6 { "inet6" } else { "inet" }.into(),
            ],
        ),
        super::Query::SystemDns => ("/usr/sbin/scutil", vec!["--dns".into()]),
    };
    run_fixed(slot, executable, &arguments, None, None, output_limit)
}
