//! The controlled helper retains originals before every fallible native step.
use super::*;
use super::{
    tool::ChildOriginal,
    transfer::{Original, Transfer},
};
use std::os::fd::IntoRawFd;
mod create;
mod supervisor;
struct Server {
    utun: Option<Original>,
    supervisor: Option<Original>,
    transfer: Option<Transfer>,
    child: ChildOriginal,
    request: Option<Box<[u8]>>,
}
pub(super) fn serve(stream: &mut UnixStream) -> io::Result<()> {
    serve_with_initial(stream, None)
}
pub(super) fn serve_with_initial(stream: &mut UnixStream, name: Option<&str>) -> io::Result<()> {
    let mut server = Server {
        utun: None,
        supervisor: None,
        transfer: None,
        child: ChildOriginal::new(),
        request: None,
    };
    let result = match name {
        Some(name) => server
            .create(stream, Some(name), None)
            .and_then(|()| server.run(stream)),
        None => server.run(stream),
    };
    // Explicit cleanup precedes process exit; no OwnedFd/Child Drop supplies
    // the parent's physical proof. Its original kqueue observes that exit.
    let closed = server.close();
    result.and(closed)
}
impl Server {
    fn run(&mut self, stream: &mut UnixStream) -> io::Result<()> {
        loop {
            // Never clone the control FD into a BufReader. Plain byte reads
            // must leave the next ancillary marker in its original channel.
            let mut bootstrap = [0_u8; 65_536];
            let bytes = self.request.as_deref_mut().unwrap_or(&mut bootstrap);
            let mut length = 0;
            loop {
                let mut byte = [0];
                let count = loop {
                    match stream.read(&mut byte) {
                        Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                        result => break result?,
                    }
                };
                if count == 0 {
                    return Ok(());
                }
                if byte[0] == b'\n' {
                    break;
                }
                if length == bytes.len() {
                    return Err(io::Error::other(
                        "helper request exceeds fixed control workspace",
                    ));
                }
                bytes[length] = byte[0];
                length += 1;
            }
            let request: HelperRequest =
                serde_json::from_slice(&bytes[..length]).map_err(io::Error::other)?;
            match request {
                HelperRequest::Hello { nonce } => write_helper_response(
                    stream,
                    &HelperResponse {
                        success: true,
                        code: 0,
                        stdout: nonce.to_be_bytes().to_vec(),
                        stderr: Vec::new(),
                    },
                )?,
                HelperRequest::InstallSupervisor { transfer, request_capacity } => {
                    self.install_supervisor(stream, transfer, request_capacity)?
                }
                HelperRequest::OwnedCreateUtun { name, transfer } => {
                    self.create(stream, name.as_deref(), Some(transfer))?
                }
                HelperRequest::CreateUtun { name } => self.create(stream, name.as_deref(), None)?,
                HelperRequest::OwnedCommand {
                    program,
                    arguments,
                    input,
                    nonce,
                    output_limit,
                } => {
                    let original = self
                        .supervisor
                        .as_ref()
                        .filter(|original| original.usable())
                        .ok_or_else(|| {
                            io::Error::other("owned command has no original supervisor")
                        })?;
                    let output = tool::run(
                        &mut self.child,
                        program,
                        &arguments,
                        input.as_deref(),
                        Some((original.fd(), nonce)),
                        output_limit,
                    )?;
                    write_output(stream, output)?;
                }
                HelperRequest::Command {
                    program,
                    arguments,
                    input,
                } => {
                    // Explicit old library execution keeps its original API;
                    // Kernel commands always carry their actual supervisor.
                    let output = tool::run(
                        &mut self.child,
                        program,
                        &arguments,
                        input.as_deref(),
                        None,
                        usize::MAX,
                    )?;
                    write_output(stream, output)?;
                }
                HelperRequest::Shutdown => return Ok(()),
            }
        }
    }
    fn close(&mut self) -> io::Result<()> {
        let mut first = None;
        for original in [&mut self.utun, &mut self.supervisor].into_iter().flatten() {
            if let Err(source) = original.close_once() {
                first.get_or_insert(source);
            }
        }
        if let Some(transfer) = &self.transfer {
            if let Err(source) = transfer.close_unclaimed() {
                first.get_or_insert(source);
            }
        }
        if let Err(source) = self.child.close_pipes() {
            first.get_or_insert(source);
        }
        if !self.child.reusable() {
            if let Err(source) = self.child.wait() {
                first.get_or_insert(source);
            }
        }
        first.map_or(Ok(()), Err)
    }
}
fn write_output(stream: &mut UnixStream, output: Output) -> io::Result<()> {
    write_helper_response(
        stream,
        &HelperResponse {
            success: output.status.success(),
            code: output.status.code().unwrap_or(1),
            stdout: output.stdout,
            stderr: output.stderr,
        },
    )
}

#[cfg(test)]
mod tests;
