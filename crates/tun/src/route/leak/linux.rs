use std::io;
use std::io::Write;
use std::net::IpAddr;
use std::process::{Command, Stdio};

use ipnet::IpNet;

use super::{
    normalized_exclusions, normalized_prefixes, safe_resource_name, strict_route_socket_mark,
    validate_interface_name,
};

#[derive(Debug)]
pub struct SystemLeakGuard {
    table: String,
    tun_name: String,
    protected: Vec<IpNet>,
    excluded: Vec<IpAddr>,
    socket_mark: u32,
    active: bool,
    installed_rules: String,
}

impl SystemLeakGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        protected: &[IpNet],
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        validate_interface_name(tun_name)?;
        let table = format!("zero_killswitch_{}", safe_resource_name(recovery_key));
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        let socket_mark = strict_route_socket_mark(recovery_key);
        let exists = table_snapshot(&table)?.is_some();
        apply_policy(&table, tun_name, &protected, &excluded, socket_mark, exists)?;
        let mut guard = Self {
            installed_rules: String::new(),
            table,
            tun_name: tun_name.to_owned(),
            protected,
            excluded,
            socket_mark,
            active: true,
        };
        guard.installed_rules = required_table_snapshot(&guard.table)?;
        Ok(guard)
    }

    pub fn reconcile(&mut self, protected: &[IpNet], excluded: &[IpAddr]) -> io::Result<bool> {
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        let policy_changed = protected != self.protected || excluded != self.excluded;
        let observed = table_snapshot(&self.table)?;
        let exists = observed.is_some();
        let repaired = super::audit::repair_policy(
            &self.installed_rules,
            observed.as_deref(),
            policy_changed,
            || {
                apply_policy(
                    &self.table,
                    &self.tun_name,
                    &protected,
                    &excluded,
                    self.socket_mark,
                    exists,
                )?;
                required_table_snapshot(&self.table)
            },
        )?;
        let Some(installed_rules) = repaired else {
            return Ok(false);
        };
        self.installed_rules = installed_rules;
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
        let output = Command::new("nft")
            .args(["delete", "table", "inet", &self.table])
            .output()?;
        if !output.status.success() && table_snapshot(&self.table)?.is_some() {
            return Err(command_error(
                "delete nftables kill-switch table",
                &output.stderr,
            ));
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

// Read the complete canonical ruleset, including chain hook/priority and rule
// order. Table existence alone cannot detect a flushed chain or deleted reject.
// Stateless, numeric output has no counters, handle IDs, or name resolution.
fn table_snapshot(table: &str) -> io::Result<Option<String>> {
    let output = Command::new("nft")
        .args(["--stateless", "--numeric", "list", "table", "inet", table])
        .output()?;
    if output.status.success() {
        return super::audit::snapshot(&output.stdout).map(Some);
    }
    // A failed inspection is not proof of absence (e.g. permission loss). Only
    // an authoritative successful inventory may authorize table recreation.
    let inventory = Command::new("nft")
        .args(["--json", "list", "tables"])
        .output()?;
    if !inventory.status.success() {
        return Err(command_error(
            "inspect nftables kill switch",
            &output.stderr,
        ));
    }
    let inventory: serde_json::Value =
        serde_json::from_slice(&inventory.stdout).map_err(io::Error::other)?;
    let tables = inventory["nftables"].as_array().ok_or_else(|| {
        io::Error::other("nftables table inventory is missing the nftables array")
    })?;
    if tables
        .iter()
        .any(|entry| entry["table"]["family"] == "inet" && entry["table"]["name"] == table)
    {
        Err(command_error(
            "inspect nftables kill switch",
            &output.stderr,
        ))
    } else {
        Ok(None)
    }
}

fn required_table_snapshot(table: &str) -> io::Result<String> {
    table_snapshot(table)?
        .ok_or_else(|| io::Error::other("nftables kill-switch table missing after installation"))
}

fn apply_policy(
    table: &str,
    tun_name: &str,
    protected: &[IpNet],
    excluded: &[IpAddr],
    socket_mark: u32,
    exists: bool,
) -> io::Result<()> {
    let script = policy_script(table, tun_name, protected, excluded, socket_mark, exists);
    let mut child = Command::new("nft")
        .args(["-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "nft stdin unavailable"))?
        .write_all(script.as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(command_error(
            "install nftables kill switch",
            &output.stderr,
        ))
    }
}

fn policy_script(
    table: &str,
    tun_name: &str,
    protected: &[IpNet],
    excluded: &[IpAddr],
    socket_mark: u32,
    exists: bool,
) -> String {
    let mut script = String::new();
    if exists {
        script.push_str(&format!("delete table inet {table}\n"));
    }
    script.push_str(&format!("add table inet {table}\n"));
    script.push_str(&format!(
        "add chain inet {table} output {{ type filter hook output priority -200; policy accept; }}\n"
    ));
    script.push_str(&format!(
        "add rule inet {table} output oifname \"lo\" accept\n"
    ));
    script.push_str(&format!(
        "add rule inet {table} output oifname \"{tun_name}\" accept\n"
    ));
    script.push_str(&format!(
        "add rule inet {table} output meta mark {socket_mark:#x} accept\n"
    ));
    for address in excluded {
        let family = if address.is_ipv4() { "ip" } else { "ip6" };
        script.push_str(&format!(
            "add rule inet {table} output {family} daddr {address} accept\n"
        ));
    }
    for prefix in protected {
        let family = if prefix.addr().is_ipv4() { "ip" } else { "ip6" };
        script.push_str(&format!(
            "add rule inet {table} output {family} daddr {prefix} reject\n"
        ));
    }
    script
}

fn command_error(action: &str, stderr: &[u8]) -> io::Error {
    io::Error::other(format!(
        "{action} failed: {}",
        String::from_utf8_lossy(stderr).trim()
    ))
}

#[cfg(test)]
mod tests;
