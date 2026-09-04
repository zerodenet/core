use std::io;
use std::net::IpAddr;

use ipnet::IpNet;

use super::{
    normalized_exclusions, normalized_prefixes, safe_resource_name, validate_interface_name,
};
use crate::route::capture_route_prefixes_with_exclusions;

#[derive(Debug)]
pub struct SystemLeakGuard {
    anchor: String,
    tun_name: String,
    protected: Vec<IpNet>,
    excluded: Vec<IpAddr>,
    enable_token: Option<String>,
    active: bool,
}

impl SystemLeakGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        protected: &[IpNet],
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        validate_interface_name(tun_name)?;
        verify_anchor_namespace()?;
        let anchor = format!("com.apple/zero_{}", safe_resource_name(recovery_key));
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        apply_policy(&anchor, tun_name, &protected, &excluded)?;
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
            protected,
            excluded,
            enable_token,
            active: true,
        })
    }

    pub fn reconcile(&mut self, protected: &[IpNet], excluded: &[IpAddr]) -> io::Result<bool> {
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        if protected == self.protected && excluded == self.excluded {
            return Ok(false);
        }
        apply_policy(&self.anchor, &self.tun_name, &protected, &excluded)?;
        self.protected = protected;
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
            let output = run_pfctl(&["-X", &token])?;
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
    let output = run_pfctl(&["-sr"])?;
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
    let output = run_pfctl(&["-s", "info"])?;
    if !output.status.success() {
        return Err(command_error("inspect pf status", &output.stderr));
    }
    Ok(String::from_utf8_lossy(&output.stdout).contains("Status: Enabled"))
}

fn enable_pf() -> io::Result<Option<String>> {
    let output = run_pfctl(&["-E"])?;
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

fn apply_policy(
    anchor: &str,
    tun_name: &str,
    protected: &[IpNet],
    excluded: &[IpAddr],
) -> io::Result<()> {
    let rules = policy_rules(tun_name, protected, excluded);
    let arguments = ["-a", anchor, "-f", "-"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let output = crate::macos_privilege::output_with_input(pfctl_program(), &arguments, &rules)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(command_error("install pf kill switch", &output.stderr))
    }
}

fn flush_anchor(anchor: &str) -> io::Result<()> {
    let output = run_pfctl(&["-a", anchor, "-F", "all"])?;
    if output.status.success() {
        Ok(())
    } else {
        Err(command_error("flush pf kill-switch anchor", &output.stderr))
    }
}

fn run_pfctl(arguments: &[&str]) -> io::Result<std::process::Output> {
    let arguments = arguments
        .iter()
        .map(|argument| (*argument).to_owned())
        .collect::<Vec<_>>();
    crate::macos_privilege::output(pfctl_program(), &arguments)
}

fn pfctl_program() -> &'static str {
    if std::path::Path::new("/sbin/pfctl").exists() {
        "/sbin/pfctl"
    } else {
        "pfctl"
    }
}

fn policy_rules(tun_name: &str, protected: &[IpNet], excluded: &[IpAddr]) -> String {
    let uid = unsafe { libc::geteuid() };
    let mut rules = format!(
        "pass out quick on lo0 all\n\
         pass out quick to 127.0.0.0/8\n\
         pass out quick to ::1/128\n\
         pass out quick on {tun_name} all\n\
         pass out quick all user {uid}\n"
    );
    for address in excluded {
        rules.push_str(&format!("pass out quick to {address}\n"));
    }
    for prefix in protected_prefixes_without_loopback(protected) {
        rules.push_str(&format!("block drop out quick to {prefix}\n"));
    }
    rules
}

fn protected_prefixes_without_loopback(protected: &[IpNet]) -> Vec<IpNet> {
    let loopback_v4 = "127.0.0.0/8".parse().expect("valid IPv4 loopback CIDR");
    let loopback_v6 = "::1/128".parse().expect("valid IPv6 loopback CIDR");
    let mut prefixes = protected
        .iter()
        .copied()
        .flat_map(|prefix| {
            let loopback = if prefix.addr().is_ipv4() {
                loopback_v4
            } else {
                loopback_v6
            };
            capture_route_prefixes_with_exclusions(prefix.addr(), &[prefix], &[loopback])
        })
        .collect::<Vec<_>>();
    prefixes.sort_unstable();
    prefixes.dedup();
    prefixes
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
        let rules = policy_rules(
            "utun8",
            &["203.0.113.0/24".parse().unwrap()],
            &["192.0.2.1".parse().unwrap()],
        );
        let uid = unsafe { libc::geteuid() };
        assert!(rules.contains("pass out quick on utun8 all"));
        assert!(rules.contains(&format!("pass out quick all user {uid}\n")));
        assert!(!rules.contains("pass out quick user"));
        assert!(rules.contains("pass out quick to 127.0.0.0/8\n"));
        assert!(rules.contains("pass out quick to ::1/128\n"));
        assert!(rules.contains("pass out quick to 192.0.2.1"));
        assert!(rules.ends_with("block drop out quick to 203.0.113.0/24\n"));
    }

    #[test]
    fn pf_policy_exempts_loopback_destinations_before_protected_routes() {
        let rules = policy_rules(
            "utun8",
            &[
                "0.0.0.0/1".parse().unwrap(),
                "128.0.0.0/1".parse().unwrap(),
                "::/1".parse().unwrap(),
                "8000::/1".parse().unwrap(),
            ],
            &[],
        );
        let ipv4_pass = rules.find("pass out quick to 127.0.0.0/8").unwrap();
        let ipv6_pass = rules.find("pass out quick to ::1/128").unwrap();
        let first_block = rules.find("block drop out quick").unwrap();
        assert!(ipv4_pass < first_block);
        assert!(ipv6_pass < first_block);
        let blocked = rules
            .lines()
            .filter_map(|line| line.strip_prefix("block drop out quick to "))
            .map(|prefix| prefix.parse::<IpNet>().unwrap())
            .collect::<Vec<_>>();
        assert!(!blocked
            .iter()
            .any(|prefix| prefix.contains(&"127.0.0.1".parse::<IpAddr>().unwrap())));
        assert!(!blocked
            .iter()
            .any(|prefix| prefix.contains(&"::1".parse::<IpAddr>().unwrap())));
    }
}
