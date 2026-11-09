use std::error::Error;

use crate::cli::Command;

mod control;
mod inspect;
mod run;
pub(crate) mod services;
mod tun;

pub async fn execute(command: Command) -> Result<(), Box<dyn Error>> {
    let (command, limits) = match command {
        Command::ControlClient {
            max_frame_bytes,
            total_max_bytes,
            command,
        } => (
            *command,
            crate::ipc::client::Limits {
                frame: max_frame_bytes,
                total: total_max_bytes,
            },
        ),
        command => (command, Default::default()),
    };
    let client = if command.uses_control_client() {
        Some(crate::ipc::client::Caller::begin(limits).await?)
    } else {
        None
    };
    let result = dispatch(command, client.as_ref()).await;
    let cleanup = match client {
        Some(client) => client.finish().await,
        None => Ok(()),
    };
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Err(work), Err(cleanup)) => Err(Box::new(ClientExecutionFailure { work, cleanup })),
    }
}
#[derive(Debug)]
struct ClientExecutionFailure {
    work: Box<dyn Error>,
    cleanup: std::io::Error,
}
impl std::fmt::Display for ClientExecutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; control client cleanup: {}", self.work, self.cleanup)
    }
}
impl Error for ClientExecutionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.work.as_ref())
    }
}
async fn dispatch(
    command: Command,
    client: Option<&crate::ipc::client::Caller>,
) -> Result<(), Box<dyn Error>> {
    match command {
        command @ (Command::Mode { .. }
        | Command::Select { .. }
        | Command::Flows { .. }
        | Command::Policies { .. }
        | Command::Events { .. }
        | Command::Reload { .. }) => {
            control::execute(command, client.expect("actual client caller installed")).await
        }
        command @ (Command::TunStart { .. }
        | Command::TunRecover { .. }
        | Command::TunStop { .. }
        | Command::TunStatus { .. }) => tun::execute(command, client).await,
        command @ Command::MacosTunCreateHelper { .. } => tun::execute(command, client).await,
        command @ Command::Run { .. } => Box::pin(run::execute(command)).await,
        command => inspect::execute(command, client).await,
    }
}

fn resolve_socket(socket_path: Option<&str>) -> Result<String, Box<dyn Error>> {
    if let Some(path) = socket_path {
        return Ok(path.to_owned());
    }
    #[cfg(windows)]
    return Ok(crate::ipc::windows::PIPE_NAME.to_owned());
    #[cfg(unix)]
    {
        if let Ok(executable) = std::env::current_exe() {
            if let Some(directory) = executable.parent() {
                let sibling = directory.join("control.sock");
                if sibling.exists() {
                    return Ok(sibling.display().to_string());
                }
            }
        }
        let path = crate::ipc::default_ipc_path().ok_or_else(|| {
            std::io::Error::other(
                "cannot find control socket: $HOME is not set. Use --socket to specify a path.",
            )
        })?;
        if !path.exists() {
            return Err(std::io::Error::other(format!(
                "control socket not found at {} -- is zero running?",
                path.display()
            ))
            .into());
        }
        Ok(path.to_string_lossy().to_string())
    }
}
