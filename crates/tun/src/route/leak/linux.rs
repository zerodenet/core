use std::io;
use std::io::Write;
use std::net::IpAddr;
use std::process::{Command, Stdio};

use ipnet::IpNet;

use super::{
    normalized_exclusions, normalized_prefixes, safe_resource_name, validate_interface_name,
};

#[derive(Debug)]
pub struct SystemLeakGuard {
    table: String,
    tun_name: String,
    protected: Vec<IpNet>,
    excluded: Vec<IpAddr>,
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
        let table = format!("zero_killswitch_{}", safe_resource_name(recovery_key));
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        let exists = table_exists(&table)?;
        apply_policy(&table, tun_name, &protected, &excluded, exists)?;
        Ok(Self {
            table,
            tun_name: tun_name.to_owned(),
            protected,
            excluded,
            active: true,
        })
    }

    pub fn reconcile(&mut self, protected: &[IpNet], excluded: &[IpAddr]) -> io::Result<bool> {
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        if protected == self.protected && excluded == self.excluded {
            return Ok(false);
        }
        apply_policy(&self.table, &self.tun_name, &protected, &excluded, true)?;
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
        if !output.status.success() && table_exists(&self.table)? {
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

fn table_exists(table: &str) -> io::Result<bool> {
    let output = Command::new("nft")
        .args(["list", "table", "inet", table])
        .output()?;
    Ok(output.status.success())
}

fn apply_policy(
    table: &str,
    tun_name: &str,
    protected: &[IpNet],
    excluded: &[IpAddr],
    exists: bool,
) -> io::Result<()> {
    let script = policy_script(table, tun_name, protected, excluded, exists);
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
    let uid = unsafe { libc::geteuid() };
    script.push_str(&format!(
        "add rule inet {table} output meta skuid {uid} accept\n"
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
mod tests {
    use super::*;

    #[test]
    fn nft_policy_is_atomic_and_fail_closed() {
        let script = policy_script(
            "zero_killswitch_test",
            "tun0",
            &[
                "0.0.0.0/1".parse().unwrap(),
                "8000::/1".parse().unwrap(),
            ],
            &["192.0.2.1".parse().unwrap(), "2001:db8::1".parse().unwrap()],
            true,
        );
        assert!(script.starts_with("delete table inet zero_killswitch_test\n"));
        assert!(script.contains("add table inet zero_killswitch_test\n"));
        assert!(script.contains("oifname \"tun0\" accept"));
        assert!(script.contains("ip daddr 192.0.2.1 accept"));
        assert!(script.contains("ip6 daddr 2001:db8::1 accept"));
        assert!(script.contains("ip daddr 0.0.0.0/1 reject"));
        assert!(script.ends_with("ip6 daddr 8000::/1 reject\n"));
    }
}
