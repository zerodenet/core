use std::io;
use std::io::Write;
use std::net::IpAddr;
use std::process::{Command, Stdio};

use super::{normalized_exclusions, safe_resource_name, validate_interface_name};

#[derive(Debug)]
pub struct SystemLeakGuard {
    anchor: String,
    tun_name: String,
    excluded: Vec<IpAddr>,
    enable_token: Option<String>,
    active: bool,
}

impl SystemLeakGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        validate_interface_name(tun_name)?;
        verify_anchor_namespace()?;
        let anchor = format!("com.apple/zero_{}", safe_resource_name(recovery_key));
        let excluded = normalized_exclusions(excluded);
        apply_policy(&anchor, tun_name, &excluded)?;
        let enable_token = if pf_enabled()? {
            None
        } else {
            match enable_pf() {
                Ok(token) => token,
                Err(error) => {
                    let _ = flush_anchor(&anchor);
                    return Err(error);
                }
            }
        };
        Ok(Self {
            anchor,
            tun_name: tun_name.to_owned(),
            excluded,
            enable_token,
            active: true,
        })
    }

    pub fn reconcile(&mut self, excluded: &[IpAddr]) -> io::Result<bool> {
        let excluded = normalized_exclusions(excluded);
        if excluded == self.excluded {
            return Ok(false);
        }
        apply_policy(&self.anchor, &self.tun_name, &excluded)?;
        self.excluded = excluded;
        Ok(true)
    }

    pub fn close(mut self) -> io::Result<()> {
        self.cleanup()
    }

    fn cleanup(&mut self) -> io::Result<()> {
        if !self.active {
            return Ok(());
        }
        flush_anchor(&self.anchor)?;
        if let Some(token) = self.enable_token.take() {
            let output = Command::new("pfctl").args(["-X", &token]).output()?;
            if !output.status.success() {
                return Err(command_error("release pf enable token", &output.stderr));
            }
        }
        self.active = false;
        Ok(())
    }
}

impl Drop for SystemLeakGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn verify_anchor_namespace() -> io::Result<()> {
    let output = Command::new("pfctl").args(["-sr"]).output()?;
    if !output.status.success() {
        return Err(command_error("inspect pf rules", &output.stderr));
    }
    let rules = String::from_utf8_lossy(&output.stdout);
    if rules.contains("com.apple/*") {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "pf main ruleset does not evaluate the `com.apple/*` anchor namespace",
        ))
    }
}

fn pf_enabled() -> io::Result<bool> {
    let output = Command::new("pfctl").args(["-s", "info"]).output()?;
    if !output.status.success() {
        return Err(command_error("inspect pf status", &output.stderr));
    }
    Ok(String::from_utf8_lossy(&output.stdout).contains("Status: Enabled"))
}

fn enable_pf() -> io::Result<Option<String>> {
    let output = Command::new("pfctl").arg("-E").output()?;
    if !output.status.success() {
        return Err(command_error("enable pf", &output.stderr));
    }
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(combined
        .split_whitespace()
        .rev()
        .find(|value| value.chars().all(|character| character.is_ascii_digit()))
        .map(str::to_owned))
}

fn apply_policy(anchor: &str, tun_name: &str, excluded: &[IpAddr]) -> io::Result<()> {
    let rules = policy_rules(tun_name, excluded);
    let mut child = Command::new("pfctl")
        .args(["-a", anchor, "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "pfctl stdin unavailable"))?
        .write_all(rules.as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(command_error("install pf kill switch", &output.stderr))
    }
}

fn flush_anchor(anchor: &str) -> io::Result<()> {
    let output = Command::new("pfctl")
        .args(["-a", anchor, "-F", "all"])
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(command_error("flush pf kill-switch anchor", &output.stderr))
    }
}

fn policy_rules(tun_name: &str, excluded: &[IpAddr]) -> String {
    let uid = unsafe { libc::geteuid() };
    let mut rules = format!(
        "pass out quick on lo0 all\npass out quick on {tun_name} all\npass out quick user {uid} all\n"
    );
    for address in excluded {
        rules.push_str(&format!("pass out quick to {address}\n"));
    }
    rules.push_str("block drop out quick all\n");
    rules
}

fn command_error(action: &str, stderr: &[u8]) -> io::Error {
    io::Error::other(format!(
        "{action} failed: {}",
        String::from_utf8_lossy(stderr).trim()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pf_policy_ends_in_a_quick_block() {
        let rules = policy_rules("utun8", &["192.0.2.1".parse().unwrap()]);
        assert!(rules.contains("pass out quick on utun8 all"));
        assert!(rules.contains("pass out quick to 192.0.2.1"));
        assert!(rules.ends_with("block drop out quick all\n"));
    }
}
