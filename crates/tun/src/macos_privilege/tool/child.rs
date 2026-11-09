//! The helper owns the actual Child and each output/input pipe before IO.
use super::*;
use std::{
    os::unix::process::CommandExt,
    process::{Child, Command, ExitStatus, Output, Stdio},
};
mod handshake;
mod pipes;

pub(super) struct ChildOriginal {
    child: Option<Child>,
    pipes: [Option<Original>; 3],
    waited: Option<ExitStatus>,
}
impl ChildOriginal {
    pub(super) fn new() -> Self {
        Self {
            child: None,
            pipes: [None, None, None],
            waited: None,
        }
    }
    pub(super) fn close_pipes(&mut self) -> io::Result<()> {
        let mut failure = None;
        for original in self.pipes.iter_mut().flatten() {
            if let Err(source) = original.close_once() {
                failure.get_or_insert(source);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    pub(super) fn reaped(&self) -> bool {
        self.waited.is_some()
    }
    pub(super) fn wait(&mut self) -> io::Result<ExitStatus> {
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
    pub(super) fn reusable(&self) -> bool {
        self.child.is_none() || (self.reaped() && self.pipes.iter().flatten().all(Original::closed))
    }
}
pub(super) fn run(
    slot: &mut ChildOriginal,
    program: HelperProgram,
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
    let executable = match program {
        HelperProgram::Ifconfig => "/sbin/ifconfig",
        HelperProgram::Route => "/sbin/route",
        HelperProgram::Pfctl => "/sbin/pfctl",
    };
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
