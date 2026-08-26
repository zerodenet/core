use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use super::{normalized_exclusions, safe_resource_name, validate_interface_name};
use crate::route::journal::route_state_root;

#[derive(Debug)]
pub struct SystemLeakGuard {
    group: String,
    journal_path: PathBuf,
    journal: FirewallJournal,
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
    pub fn install(tun_name: &str, recovery_key: &str, excluded: &[IpAddr]) -> io::Result<Self> {
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
        if let Err(error) = install_rules(&group, tun_name) {
            let rollback = restore_profiles(&group, &journal);
            return Err(with_rollback_error(error, rollback));
        }
        Ok(Self {
            group,
            journal_path,
            journal,
            excluded: normalized_exclusions(excluded),
            active: true,
        })
    }

    pub fn reconcile(&mut self, excluded: &[IpAddr]) -> io::Result<bool> {
        let excluded = normalized_exclusions(excluded);
        let changed = excluded != self.excluded;
        self.excluded = excluded;
        Ok(changed)
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
        && journal.profiles.iter().all(|profile| {
            matches!(profile.action.as_str(), "Allow" | "Block" | "NotConfigured")
        });
    if !complete {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows Firewall profile snapshot is incomplete",
        ));
    }
    Ok(())
}

fn install_rules(group: &str, tun_name: &str) -> io::Result<()> {
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
    let script = format!(
        "$ErrorActionPreference='Stop'; \
         Get-NetFirewallRule -Group '{group}' -ErrorAction SilentlyContinue | Remove-NetFirewallRule; \
         New-NetFirewallRule -DisplayName '{group}-Core' -Group '{group}' -Direction Outbound -Action Allow -Program '{executable}' -Profile Any | Out-Null; \
         New-NetFirewallRule -DisplayName '{group}-Tun' -Group '{group}' -Direction Outbound -Action Allow -InterfaceAlias '{tun_name}' -Profile Any | Out-Null; \
         Set-NetFirewallProfile -Name Domain,Private,Public -DefaultOutboundAction Block"
    );
    run_powershell(&script).map(|_| ())
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
    let output = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .output()?;
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
}
