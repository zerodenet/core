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
    enable_tokens: Vec<String>,
    installed_rules: String,
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
        // Establish ownership before fallible readback/activation, so failure
        // rolls back the installed anchor through the normal cleanup handle.
        let mut guard = Self {
            anchor,
            tun_name: tun_name.to_owned(),
            protected,
            excluded,
            enable_tokens: Vec::new(),
            installed_rules: String::new(),
            active: true,
        };
        guard.installed_rules = super::audit::snapshot(&anchor_rules(&guard.anchor)?)?;
        if !pf_enabled()? {
            guard.enable_tokens.push(enable_pf()?);
            if !pf_enabled()? {
                return Err(io::Error::other("pf is still disabled after installation"));
            }
        }
        Ok(guard)
    }

    pub fn reconcile(&mut self, protected: &[IpNet], excluded: &[IpAddr]) -> io::Result<bool> {
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        // Audit activation and the actual anchor even when desired configuration
        // is unchanged. A retained but flushed anchor is not leak protection.
        verify_anchor_namespace()?;
        let observed = anchor_rules(&self.anchor)?;
        let observed = std::str::from_utf8(&observed)
            .map_err(io::Error::other)?
            .trim();
        let repaired = super::audit::repair_policy(
            &self.installed_rules,
            Some(observed),
            protected != self.protected || excluded != self.excluded,
            || {
                apply_policy(&self.anchor, &self.tun_name, &protected, &excluded)?;
                super::audit::snapshot(&anchor_rules(&self.anchor)?)
            },
        )?;
        let reenabled = if pf_enabled()? {
            false
        } else {
            // Retain every reference acquired by this guard for normal cleanup.
            self.enable_tokens.push(enable_pf()?);
            if !pf_enabled()? {
                return Err(io::Error::other("pf is still disabled after recovery"));
            }
            true
        };
        if let Some(installed_rules) = repaired.as_ref() {
            self.installed_rules = installed_rules.clone();
        }
        self.protected = protected;
        self.excluded = excluded;
        Ok(repaired.is_some() || reenabled)
    }

    pub fn close(mut self) -> io::Result<()> {
        self.cleanup()
    }

    fn cleanup(&mut self) -> io::Result<()> {
        if !self.active {
            return Ok(());
        }
        flush_anchor(&self.anchor)?;
        while let Some(token) = self.enable_tokens.last() {
            let output = run_pfctl(&["-X", token])?;
            if !output.status.success() {
                return Err(command_error("release pf enable token", &output.stderr));
            }
            self.enable_tokens.pop();
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
    if evaluates_anchor_namespace(&rules) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "pf main ruleset requires an unconditional `anchor \"com.apple/*\" all` rule",
        ))
    }
}

fn evaluates_anchor_namespace(rules: &str) -> bool {
    // Accept only the canonical unconditional anchor emitted by pfctl. A
    // direction/interface/family/address-limited reference can leave protected
    // outbound traffic outside our anchor, even though its name is present.
    rules.lines().any(|line| {
        line.split_whitespace()
            .eq(["anchor", "\"com.apple/*\"", "all"])
    })
}

fn anchor_rules(anchor: &str) -> io::Result<Vec<u8>> {
    // No verbose counters/state table or -r/-P name resolution. Do not use
    // -n here: for pfctl it means no-action, not numeric output.
    let output = run_pfctl(&["-a", anchor, "-sr"])?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(command_error(
            "inspect pf kill-switch anchor",
            &output.stderr,
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

fn enable_pf() -> io::Result<String> {
    let output = run_pfctl(&["-E"])?;
    if !output.status.success() {
        return Err(command_error("enable pf", &output.stderr));
    }
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    combined
        .split_whitespace()
        .rev()
        .find(|value| value.chars().all(|character| character.is_ascii_digit()))
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("pf did not return an enable reference token"))
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
    // macOS PF has no Linux-style per-socket mark/AppID match. This UID-wide
    // exception is required by current direct underlay sockets, so strict route
    // does NOT isolate other programs running as Zero's effective UID. Do not
    // replace it with endpoint-only rules: arbitrary direct destinations are
    // supported. A narrower identity requires a separate platform mechanism.
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
mod tests;
