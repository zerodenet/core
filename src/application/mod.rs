use std::error::Error;

use crate::cli::Command;

mod control;
mod inspect;
mod run;
mod services;
mod tun;

pub async fn execute(command: Command) -> Result<(), Box<dyn Error>> {
    match command {
        command @ (Command::Mode { .. }
        | Command::Select { .. }
        | Command::Flows { .. }
        | Command::Policies { .. }
        | Command::Events { .. }
        | Command::Reload { .. }) => control::execute(command),
        command @ (Command::TunStart { .. }
        | Command::TunStop { .. }
        | Command::TunStatus { .. }) => tun::execute(command),
        command @ Command::Run { .. } => run::execute(command).await,
        command => inspect::execute(command),
    }
}

fn resolve_socket(socket_path: Option<&str>) -> Result<String, Box<dyn Error>> {
    if let Some(path) = socket_path {
        return Ok(path.to_owned());
    }
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
