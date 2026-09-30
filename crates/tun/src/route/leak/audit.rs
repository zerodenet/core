//! Canonical, state-free snapshots of platform-owned firewall rules.

use std::io;

pub(super) fn snapshot(output: &[u8]) -> io::Result<String> {
    let rules = std::str::from_utf8(output)
        .map_err(io::Error::other)?
        .trim()
        .to_owned();
    if rules.is_empty() {
        return Err(io::Error::other("installed kill-switch ruleset is empty"));
    }
    Ok(rules)
}

/// Never bless a successful command alone as recovery. For unchanged policy,
/// readback must exactly match the last successfully installed ruleset. The
/// caller commits desired state only after this returns successfully.
pub(super) fn repair_policy(
    installed: &str,
    observed: Option<&str>,
    policy_changed: bool,
    install_and_read_back: impl FnOnce() -> io::Result<String>,
) -> io::Result<Option<String>> {
    if !policy_changed && observed == Some(installed) {
        return Ok(None);
    }
    let repaired = install_and_read_back()?;
    if !policy_changed && repaired != installed {
        return Err(io::Error::other(
            "kill-switch rules still differ after repair",
        ));
    }
    Ok(Some(repaired))
}

#[cfg(test)]
mod tests;
