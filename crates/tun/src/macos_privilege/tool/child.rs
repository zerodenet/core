//! The helper owns the actual Child and each output/input pipe before IO.
use super::*;
use std::{
    os::unix::process::CommandExt,
    process::{Child, Command, ExitStatus, Output, Stdio},
};
mod handshake;
mod pipes;

pub(in crate::macos_privilege) struct ChildOriginal {
    child: Option<Child>,
    pipes: [Option<Original>; 3],
    waited: Option<ExitStatus>,
}
impl ChildOriginal {
    pub(in crate::macos_privilege) fn new() -> Self {
        Self {
            child: None,
            pipes: [None, None, None],
            waited: None,
        }
    }
    pub(in crate::macos_privilege) fn close_pipes(&mut self) -> io::Result<()> {
        let mut failure = None;
        for original in self.pipes.iter_mut().flatten() {
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
        if self.child.is_none() || self.reusable() {
            return Ok(());
        }
        // Same Child/pipes, no new command or guessed process-tree receipt.
        // This fixed local query has no input to reconstruct after interruption.
        let output = self.collect(&[], output_limit);
        let closed = self.close_pipes();
        let waited = self.wait();
        drop(output?);
        closed?;
        waited?;
        Ok(())
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
        self.child.is_none() || (self.reaped() && self.pipes.iter().flatten().all(Original::closed))
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
    command
        .args(arguments)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some((fd, nonce)) = supervisor {
        // Only stack data, getpid/read/write and errno handling run after fork.
        unsafe {
            command.pre_exec(move || handshake::announce(fd, nonce));
        }
    }
    let mut child = command.spawn()?;
    slot.pipes[0] = child
        .stdin
        .take()
        .map(|pipe| Original::new(pipe.into_raw_fd(), 0));
    slot.pipes[1] = child
        .stdout
        .take()
        .map(|pipe| Original::new(pipe.into_raw_fd(), 0));
    slot.pipes[2] = child
        .stderr
        .take()
        .map(|pipe| Original::new(pipe.into_raw_fd(), 0));
    slot.child = Some(child);
    let output = slot.collect(input.unwrap_or("").as_bytes(), output_limit);
    // Even an IO failure must progress all independent close and wait duties.
    let close = slot.close_pipes();
    let waited = slot.wait();
    let (stdout, stderr) = output?;
    close?;
    Ok(Output {
        status: waited?,
        stdout,
        stderr,
    })
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
