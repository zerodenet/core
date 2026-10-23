//! A protocol-owned launch request; the host grants an executable and start budget.
use std::{collections::BTreeMap, ffi::OsString, path::PathBuf, process::Stdio};

/// Arguments and environment values may contain protocol credentials. Debug
/// output deliberately exposes neither; this value is not a static identity.
#[derive(Clone, PartialEq, Eq)]
pub struct ProcessRequest {
    pub program: PathBuf,
    pub arguments: Vec<OsString>,
    /// None removes an inherited variable; Some sets the actual value.
    pub environment: BTreeMap<OsString, Option<OsString>>,
}

impl std::fmt::Debug for ProcessRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessRequest")
            .field("program", &self.program)
            .field("argument_count", &self.arguments.len())
            .field("environment_count", &self.environment.len())
            .finish_non_exhaustive()
    }
}

impl ProcessRequest {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            arguments: Vec::new(),
            environment: BTreeMap::new(),
        }
    }

    /// This only materializes a command. Native spawning still belongs to the
    /// host grant during start, after the persistent lifecycle owner is installed.
    pub fn command(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(&self.program);
        command.args(&self.arguments);
        for (name, value) in &self.environment {
            match value {
                Some(value) => {
                    command.env(name, value);
                }
                None => {
                    command.env_remove(name);
                }
            }
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command.kill_on_drop(true);
        command
    }
}
