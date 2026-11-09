//! The same fixed local Child writes into the Source's prepaid result storage.
use super::*;
use crate::{
    macos_privilege::command::{inline::Storage, CommandBytes, CommandOutput},
    packet_memory::{PacketMemoryView, PacketMetadataArc},
};
pub(in crate::macos_privilege) fn run_owned(
    slot: &mut ChildOriginal,
    program: HelperProgram,
    arguments: &[String],
    input: Option<&str>,
    memory: &PacketMemoryView,
    storage: &[PacketMetadataArc<Storage>; 2],
    limit: usize,
) -> io::Result<CommandOutput> {
    let executable = match program {
        HelperProgram::Ifconfig => "/sbin/ifconfig",
        HelperProgram::Route => "/sbin/route",
        HelperProgram::Pfctl => "/sbin/pfctl",
    };
    run(slot, executable, arguments, input, memory, storage, limit)
}
pub(in crate::macos_privilege) fn run_query_owned(
    slot: &mut ChildOriginal,
    query: super::super::Query,
    memory: &PacketMemoryView,
    storage: &[PacketMetadataArc<Storage>; 2],
    limit: usize,
) -> io::Result<CommandOutput> {
    let (executable, arguments) = match query {
        super::super::Query::Route(arguments) => ("/sbin/route", arguments),
        super::super::Query::RouteTable(ipv6) => (
            "/usr/sbin/netstat",
            vec![
                "-rnW".into(),
                "-f".into(),
                if ipv6 { "inet6" } else { "inet" }.into(),
            ],
        ),
        super::super::Query::SystemDns => ("/usr/sbin/scutil", vec!["--dns".into()]),
    };
    run(slot, executable, &arguments, None, memory, storage, limit)
}
fn run(
    slot: &mut ChildOriginal,
    executable: &'static str,
    arguments: &[String],
    input: Option<&str>,
    memory: &PacketMemoryView,
    storage: &[PacketMetadataArc<Storage>; 2],
    limit: usize,
) -> io::Result<CommandOutput> {
    // Both claims precede even pipe creation. An outstanding previous result
    // rejects this new operation without overwriting it or launching a Child.
    let stdout = CommandBytes::inline(Storage::claim(&storage[0])?);
    let stderr = CommandBytes::inline(Storage::claim(&storage[1])?);
    let ends = start_fixed(slot, executable, arguments, input, None)?;
    let collected = slot.collect_owned(
        input.unwrap_or("").as_bytes(),
        memory,
        [stdout, stderr],
        limit,
    );
    let closed = slot.close_pipes();
    let waited = slot.wait();
    failures::finish_owned(ends, collected, closed, waited)
}
impl ChildOriginal {
    fn collect_owned(
        &mut self,
        input: &[u8],
        memory: &PacketMemoryView,
        mut outputs: [CommandBytes; 2],
        limit: usize,
    ) -> io::Result<[CommandBytes; 2]> {
        for original in self.pipes.iter().flatten() {
            let flags = unsafe { libc::fcntl(original.fd(), libc::F_GETFL) };
            if flags < 0
                || unsafe { libc::fcntl(original.fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                    < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let mut offset = 0;
        let mut storage_failure = None;
        while self.pipes.iter().flatten().any(Original::usable) {
            let mut descriptors = [libc::pollfd {
                fd: -1,
                events: 0,
                revents: 0,
            }; 3];
            for (index, original) in self.pipes.iter().enumerate() {
                if let Some(original) = original.as_ref().filter(|original| original.usable()) {
                    descriptors[index].fd = original.fd();
                    descriptors[index].events = if index == 0 {
                        libc::POLLOUT
                    } else {
                        libc::POLLIN
                    };
                }
            }
            if offset == input.len() {
                if let Some(stdin) = self.pipes[0].as_mut().filter(|original| original.usable()) {
                    stdin.close_once()?;
                }
                descriptors[0].fd = -1;
            }
            if descriptors.iter().all(|fd| fd.fd < 0) {
                break;
            }
            if unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, -1) } < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(source);
            }
            for (index, descriptor) in descriptors.iter().enumerate() {
                if descriptor.fd < 0 || descriptor.revents == 0 {
                    continue;
                }
                if index == 0 {
                    let count = unsafe {
                        libc::write(
                            descriptor.fd,
                            input[offset..].as_ptr().cast(),
                            input.len() - offset,
                        )
                    };
                    if count > 0 {
                        offset += count as usize;
                    } else if count < 0 {
                        check_error()?;
                    }
                } else {
                    // Original worker's fixed stack scratch also drains after a
                    // metadata refusal. No output allocation precedes admission.
                    let mut scratch = [0u8; 4096];
                    let count = unsafe {
                        libc::read(descriptor.fd, scratch.as_mut_ptr().cast(), scratch.len())
                    };
                    if count == 0 {
                        self.pipes[index].as_mut().unwrap().close_once()?;
                    } else if count > 0 && storage_failure.is_none() {
                        for byte in &scratch[..count as usize] {
                            if let Err(source) = outputs[index - 1].reserve_owned(memory, limit) {
                                storage_failure = Some(source);
                                break;
                            }
                            outputs[index - 1].append_reserved(std::slice::from_ref(byte))?;
                        }
                    } else if count < 0 {
                        check_error()?;
                    }
                }
            }
        }
        if let Some(source) = storage_failure {
            return Err(io::Error::other(source));
        }
        Ok(outputs)
    }
}
fn check_error() -> io::Result<()> {
    let source = io::Error::last_os_error();
    if matches!(
        source.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
    ) {
        Ok(())
    } else {
        Err(source)
    }
}

#[cfg(test)]
#[path = "owned/tests.rs"]
mod tests;
