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
        return Err(io::Error::other(format!(
            "kill-switch rules still differ after repair: {}",
            first_difference(installed, &repaired)
        )));
    }
    Ok(Some(repaired))
}

fn first_difference(expected: &str, observed: &str) -> String {
    let mut expected_lines = expected.lines();
    let mut observed_lines = observed.lines();
    for line in 1.. {
        let expected = expected_lines.next();
        let observed = observed_lines.next();
        if expected != observed {
            // Do not dump the whole firewall policy or unbounded tool output.
            let bounded =
                |value: Option<&str>| value.map(|line| line.chars().take(160).collect::<String>());
            return format!(
                "line {line}, expected {:?}, observed {:?}",
                bounded(expected),
                bounded(observed)
            );
        }
        if expected.is_none() {
            return "different raw formatting".to_owned();
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests;
