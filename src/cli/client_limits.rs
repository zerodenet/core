//! CLI application book options, never daemon configuration inference.
use super::*;
pub(super) fn parse(args: impl IntoIterator<Item = String>) -> Result<Command, CliError> {
    let mut args = args.into_iter();
    let first = args.next();
    let mut original = Vec::new();
    if let Some(first) = first {
        original.push(first);
    }
    let (mut frame, mut total) = (None, None);
    while let Some(arg) = args.next() {
        let selected = match arg.as_str() {
            "--control-client-max-frame-bytes" => Some(&mut frame),
            "--control-client-total-max-bytes" => Some(&mut total),
            _ => None,
        };
        if let Some(selected) = selected {
            if selected.is_some() {
                return Err(CliError::new(format!("duplicate `{arg}`")));
            }
            let value = args
                .next()
                .ok_or_else(|| CliError::new(format!("`{arg}` requires bytes")))?;
            *selected =
                Some(value.parse::<usize>().map_err(|_| {
                    CliError::new(format!("`{arg}` requires an integer byte count"))
                })?);
        } else {
            original.push(arg);
        }
    }
    let command = parse_original_args(original)?;
    if frame.is_none() && total.is_none() {
        return Ok(command);
    }
    if !command.uses_control_client() {
        return Err(CliError::new(
            "control-client budgets require a command using IPC",
        ));
    }
    let defaults = crate::ipc::client::Limits::default();
    let limits = crate::ipc::client::Limits {
        frame: frame.unwrap_or(defaults.frame),
        total: total.unwrap_or(defaults.total),
    }
    .validate()
    .map_err(|error| CliError::new(error.to_string()))?;
    Ok(Command::ControlClient {
        max_frame_bytes: limits.frame,
        total_max_bytes: limits.total,
        command: Box::new(command),
    })
}
#[cfg(test)]
#[path = "client_limits/tests.rs"]
mod tests;
