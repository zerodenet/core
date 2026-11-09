//! Private owned-helper command bytes, with no raw Vec escape from Root custody.
mod buffer;
pub(super) mod control;
pub(super) mod inline;
pub(super) mod receive;
pub(super) mod wire;
pub(crate) use buffer::CommandBytes;
use std::{fmt, process::ExitStatus};
pub(crate) struct CommandOutput {
    pub(crate) status: ExitStatus,
    pub(crate) stdout: CommandBytes,
    pub(crate) stderr: CommandBytes,
}
impl CommandOutput {
    /// Only the existing explicit library helper branch uses this constructor.
    pub(crate) fn library(output: std::process::Output) -> Self {
        Self {
            status: output.status,
            stdout: CommandBytes::library(output.stdout),
            stderr: CommandBytes::library(output.stderr),
        }
    }
}
impl fmt::Debug for CommandOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandOutput")
            .field("status", &self.status)
            .field("stdout", &self.stdout)
            .field("stderr", &self.stderr)
            .finish()
    }
}
