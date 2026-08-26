use std::io;
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use serde::{Deserialize, Serialize};

use super::{
    normalized_exclusions, normalized_prefixes, safe_resource_name, validate_interface_name,
};
use crate::route::journal::route_state_root;

#[derive(Debug)]
pub struct SystemLeakGuard {
    group: String,
    journal_path: PathBuf,
    journal: FirewallJournal,
    tun_name: String,
    protected: Vec<IpNet>,
    excluded: Vec<IpAddr>,
    active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FirewallJournal {
    schema: String,
    profiles: Vec<ProfilePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProfilePolicy {
    name: String,
    action: String,
}

impl SystemLeakGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        protected: &[IpNet],
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        validate_interface_name(tun_name)?;
        let safe_name = safe_resource_name(recovery_key);
        let group = format!("ZeroKillSwitch-{safe_name}");
        let journal_path = route_state_root()?.join(format!("leak-{safe_name}.json"));
        let journal = match read_journal(&journal_path)? {
            Some(journal) => journal,
            None => {
                let journal = snapshot_profiles()?;
                persist_journal(&journal_path, &journal)?;
                journal
            }
        };
        let protected = normalized_prefixes(protected);
        if let Err(error) = install_rules(&group, tun_name, &complement_prefixes(&protected)) {
            let rollback = restore_profiles(&group, &journal);
            return Err(with_rollback_error(error, rollback));
        }
        Ok(Self {
            group,
            journal_path,
            journal,
            tun_name: tun_name.to_owned(),
            protected,
            excluded: normalized_exclusions(excluded),
            active: true,
        })
    }

    pub fn reconcile(&mut self, protected: &[IpNet], excluded: &[IpAddr]) -> io::Result<bool> {
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        if protected == self.protected && excluded == self.excluded {
            return Ok(false);
        }
        install_rules(
            &self.group,
            &self.tun_name,
            &complement_prefixes(&protected),
        )?;
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
        restore_profiles(&self.group, &self.journal)?;
        match std::fs::remove_file(&self.journal_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
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

fn snapshot_profiles() -> io::Result<FirewallJournal> {
    let script = "$ErrorActionPreference='Stop'; \
        [Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false); \
        $profiles=@(foreach($name in @('Domain','Private','Public')) { \
            $profile=Get-NetFirewallProfile -Name $name -ErrorAction Stop; \
            if($null -eq $profile) { throw \"Windows Firewall profile '$name' is unavailable\" }; \
            [pscustomobject]@{name=$profile.Name;action=$profile.DefaultOutboundAction.ToString()} \
        }); \
        $snapshot=[pscustomobject]@{schema='zero.tun.leak-guard.v1';profiles=$profiles}; \
        $json=ConvertTo-Json -InputObject $snapshot -Depth 3 -Compress; \
        [Console]::Out.Write($json)";
    let output = run_powershell(script)?;
    parse_profile_snapshot(&output)
}

fn parse_profile_snapshot(output: &[u8]) -> io::Result<FirewallJournal> {
    if output.iter().all(u8::is_ascii_whitespace) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows Firewall profile snapshot produced empty output",
        ));
    }
    let journal = serde_json::from_slice::<FirewallJournal>(output).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse Windows Firewall profile snapshot: {error}"),
        )
    })?;
    validate_journal(&journal)?;
    Ok(journal)
}

fn validate_journal(journal: &FirewallJournal) -> io::Result<()> {
    const PROFILE_NAMES: [&str; 3] = ["Domain", "Private", "Public"];
    let complete = journal.schema == "zero.tun.leak-guard.v1"
        && journal.profiles.len() == PROFILE_NAMES.len()
        && PROFILE_NAMES.iter().all(|name| {
            journal
                .profiles
                .iter()
                .filter(|profile| profile.name.as_str() == *name)
                .count()
                == 1
        })
        && journal
            .profiles
            .iter()
            .all(|profile| matches!(profile.action.as_str(), "Allow" | "Block" | "NotConfigured"));
    if !complete {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows Firewall profile snapshot is incomplete",
        ));
    }
    Ok(())
}

fn install_rules(group: &str, tun_name: &str, allowed: &[IpNet]) -> io::Result<()> {
    let executable = std::env::current_exe()?;
    let executable = executable.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Zero executable path is not valid UTF-8",
        )
    })?;
    let group = quote_powershell(group);
    let tun_name = quote_powershell(tun_name);
    let executable = quote_powershell(executable);
    let mut script = format!(
        "$ErrorActionPreference='Stop'; \
         Get-NetFirewallRule -Group '{group}' -ErrorAction SilentlyContinue | Remove-NetFirewallRule; \
         New-NetFirewallRule -DisplayName '{group}-Core' -Group '{group}' -Direction Outbound -Action Allow -Program '{executable}' -Profile Any | Out-Null; \
         New-NetFirewallRule -DisplayName '{group}-Tun' -Group '{group}' -Direction Outbound -Action Allow -InterfaceAlias '{tun_name}' -Profile Any | Out-Null; "
    );
    for (index, prefixes) in allowed.chunks(64).enumerate() {
        let addresses = prefixes
            .iter()
            .map(|prefix| format!("'{}'", quote_powershell(&prefix.to_string())))
            .collect::<Vec<_>>()
            .join(",");
        script.push_str(&format!(
            "New-NetFirewallRule -DisplayName '{group}-Bypass-{index}' -Group '{group}' -Direction Outbound -Action Allow -RemoteAddress {addresses} -Profile Any | Out-Null; "
        ));
    }
    script.push_str(
        "Set-NetFirewallProfile -Name Domain,Private,Public -DefaultOutboundAction Block",
    );
    run_powershell(&script).map(|_| ())
}

fn complement_prefixes(protected: &[IpNet]) -> Vec<IpNet> {
    let mut complement = Vec::new();
    for (ipv6, width) in [(false, 32_u8), (true, 128_u8)] {
        let mut ranges = protected
            .iter()
            .filter(|prefix| prefix.addr().is_ipv6() == ipv6)
            .map(prefix_range)
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        let mut merged: Vec<(u128, u128)> = Vec::new();
        for (start, end) in ranges {
            if let Some(last) = merged.last_mut() {
                if start <= last.1.saturating_add(1) {
                    last.1 = last.1.max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }

        let maximum = if ipv6 { u128::MAX } else { u32::MAX as u128 };
        let mut start = 0_u128;
        for (blocked_start, blocked_end) in merged {
            if start < blocked_start {
                append_range_prefixes(&mut complement, start, blocked_start - 1, width);
            }
            if blocked_end == maximum {
                start = maximum;
                break;
            }
            start = blocked_end + 1;
        }
        if start < maximum
            || (start == maximum
                && !protected.iter().any(|prefix| {
                    prefix.addr().is_ipv6() == ipv6 && prefix.contains(&ip(maximum, width))
                }))
        {
            append_range_prefixes(&mut complement, start, maximum, width);
        }
    }
    complement
}

fn prefix_range(prefix: &IpNet) -> (u128, u128) {
    let (start, width, prefix_len) = match prefix {
        IpNet::V4(prefix) => (
            u32::from(prefix.network()) as u128,
            32_u8,
            prefix.prefix_len(),
        ),
        IpNet::V6(prefix) => (u128::from(prefix.network()), 128_u8, prefix.prefix_len()),
    };
    let host_bits = width - prefix_len;
    let end = if host_bits == 128 {
        u128::MAX
    } else {
        start | ((1_u128 << host_bits) - 1)
    };
    (start, end)
}

fn append_range_prefixes(output: &mut Vec<IpNet>, mut start: u128, end: u128, width: u8) {
    if start == 0 && end == u128::MAX && width == 128 {
        output.push(IpNet::V6(Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 0).unwrap()));
        return;
    }
    while start <= end {
        let aligned_bits = (start.trailing_zeros() as u8).min(width);
        let remaining = end - start + 1;
        let fitting_bits = (127 - remaining.leading_zeros()) as u8;
        let host_bits = aligned_bits.min(fitting_bits).min(width);
        let prefix_len = width - host_bits;
        output.push(if width == 32 {
            IpNet::V4(Ipv4Net::new(Ipv4Addr::from(start as u32), prefix_len).unwrap())
        } else {
            IpNet::V6(Ipv6Net::new(Ipv6Addr::from(start), prefix_len).unwrap())
        });
        if host_bits == 128 {
            break;
        }
        let block_size = 1_u128 << host_bits;
        if block_size > end - start {
            break;
        }
        start += block_size;
    }
}

fn ip(value: u128, width: u8) -> IpAddr {
    if width == 32 {
        IpAddr::V4(Ipv4Addr::from(value as u32))
    } else {
        IpAddr::V6(Ipv6Addr::from(value))
    }
}

fn restore_profiles(group: &str, journal: &FirewallJournal) -> io::Result<()> {
    let mut script = "$ErrorActionPreference='Stop'; ".to_owned();
    for profile in &journal.profiles {
        script.push_str(&format!(
            "Set-NetFirewallProfile -Name '{}' -DefaultOutboundAction {}; ",
            quote_powershell(&profile.name),
            profile.action
        ));
    }
    script.push_str(&format!(
        "Get-NetFirewallRule -Group '{}' -ErrorAction SilentlyContinue | Remove-NetFirewallRule",
        quote_powershell(group)
    ));
    run_powershell(&script).map(|_| ())
}

fn read_journal(path: &Path) -> io::Result<Option<FirewallJournal>> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let journal = serde_json::from_slice::<FirewallJournal>(&raw).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse Windows kill-switch recovery journal: {error}"),
        )
    })?;
    validate_journal(&journal).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("Windows kill-switch recovery journal is incompatible: {error}"),
        )
    })?;
    Ok(Some(journal))
}

fn persist_journal(path: &Path, journal: &FirewallJournal) -> io::Result<()> {
    let temporary = path.with_extension("json.tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec(journal).map_err(io::Error::other)?,
    )?;
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    std::fs::rename(temporary, path)
}

fn run_powershell(script: &str) -> io::Result<Vec<u8>> {
    let mut child = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "PowerShell stdin unavailable"))?
        .write_all(script.as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "Windows Firewall kill-switch transaction failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ))
    }
}

fn quote_powershell(value: &str) -> String {
    value.replace('\'', "''")
}

fn with_rollback_error(error: io::Error, rollback: io::Result<()>) -> io::Error {
    match rollback {
        Ok(()) => error,
        Err(rollback) => io::Error::new(
            error.kind(),
            format!("{error}; rollback Windows Firewall policy: {rollback}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn powershell_values_are_single_quote_escaped() {
        assert_eq!(quote_powershell("a'b"), "a''b");
    }

    #[test]
    fn profile_snapshot_requires_explicit_complete_json() {
        let error = parse_profile_snapshot(b"").expect_err("empty output must fail closed");
        assert!(error.to_string().contains("empty output"));

        let journal = parse_profile_snapshot(
            br#"{"schema":"zero.tun.leak-guard.v1","profiles":[{"name":"Domain","action":"Allow"},{"name":"Private","action":"Block"},{"name":"Public","action":"NotConfigured"}]}"#,
        )
        .expect("complete profile snapshot");
        assert_eq!(journal.profiles.len(), 3);

        let error = parse_profile_snapshot(
            br#"{"schema":"zero.tun.leak-guard.v1","profiles":[{"name":"Domain","action":"Allow"}]}"#,
        )
        .expect_err("partial snapshot must fail closed");
        assert!(error.to_string().contains("incomplete"));
    }

    #[test]
    fn complement_preserves_only_non_captured_destinations() {
        let allowed = complement_prefixes(&[
            "10.0.0.0/8".parse().unwrap(),
            "2001:db8::/32".parse().unwrap(),
        ]);
        assert!(!allowed
            .iter()
            .any(|prefix| prefix.contains(&"10.1.2.3".parse().unwrap())));
        assert!(!allowed
            .iter()
            .any(|prefix| prefix.contains(&"2001:db8::1".parse().unwrap())));
        assert!(allowed
            .iter()
            .any(|prefix| prefix.contains(&"192.0.2.1".parse().unwrap())));
        assert!(allowed
            .iter()
            .any(|prefix| prefix.contains(&"2001:db9::1".parse().unwrap())));
    }

    #[test]
    fn split_defaults_have_an_empty_complement() {
        let allowed = complement_prefixes(&[
            "0.0.0.0/1".parse().unwrap(),
            "128.0.0.0/1".parse().unwrap(),
            "::/1".parse().unwrap(),
            "8000::/1".parse().unwrap(),
        ]);
        assert!(allowed.is_empty());
    }
}
