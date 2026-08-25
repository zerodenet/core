//! Platform-owned strict-route leak protection.
//!
//! The runtime supplies only the neutral protection surface: the managed TUN
//! interface and explicit bootstrap/proxy endpoints. Each platform owns its
//! firewall transaction, recovery state, and rollback details.

use std::io;
use std::net::IpAddr;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
pub use linux::SystemLeakGuard;
#[cfg(target_os = "macos")]
pub use macos::SystemLeakGuard;
#[cfg(target_os = "windows")]
pub use windows::SystemLeakGuard;

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
#[derive(Debug)]
pub struct SystemLeakGuard;

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
impl SystemLeakGuard {
    pub fn install(
        _tun_name: &str,
        _recovery_key: &str,
        _excluded: &[IpAddr],
    ) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "strict-route leak protection is unsupported on this platform",
        ))
    }

    pub fn reconcile(&mut self, _excluded: &[IpAddr]) -> io::Result<bool> {
        Ok(false)
    }

    pub fn close(self) -> io::Result<()> {
        Ok(())
    }
}

fn safe_resource_name(recovery_key: &str) -> String {
    let safe = recovery_key
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .take(24)
        .collect::<String>();
    if safe.is_empty() {
        "tun".to_owned()
    } else {
        safe
    }
}

fn normalized_exclusions(excluded: &[IpAddr]) -> Vec<IpAddr> {
    let mut excluded = excluded.to_vec();
    excluded.sort_unstable();
    excluded.dedup();
    excluded
}

fn validate_interface_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > 128
        || name
            .chars()
            .any(|character| character.is_control() || matches!(character, '\'' | '"' | '\\'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TUN interface name is unsafe for a platform firewall rule",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_names_and_exclusions_are_bounded_and_deterministic() {
        assert_eq!(safe_resource_name("../My Tun!"), "MyTun");
        assert_eq!(safe_resource_name("---"), "tun");
        assert!(safe_resource_name(&"a".repeat(80)).len() <= 24);
        assert_eq!(
            normalized_exclusions(&[
                "2001:db8::1".parse().unwrap(),
                "192.0.2.1".parse().unwrap(),
                "192.0.2.1".parse().unwrap(),
            ]),
            vec![
                "192.0.2.1".parse().unwrap(),
                "2001:db8::1".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn rejects_firewall_metacharacters_in_interface_names() {
        assert!(validate_interface_name("znet-tun0").is_ok());
        assert!(validate_interface_name("bad\"name").is_err());
        assert!(validate_interface_name("bad\nname").is_err());
    }
}
