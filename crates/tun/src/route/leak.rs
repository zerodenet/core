//! Platform-owned strict-route leak protection.
//!
//! The runtime supplies only the neutral protection surface: the managed TUN
//! interface and explicit bootstrap/proxy endpoints. Each platform owns its
//! firewall transaction, recovery state, and rollback details.

use std::io;
use std::net::IpAddr;

use ipnet::IpNet;

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

/// Derive the stable, non-zero socket identity used by strict-route firewall
/// rules from the recovery key that already identifies the route transaction.
/// Stability lets a restarted process take over an orphaned kill switch, while
/// distinct keys do not grant one Zero instance access through another's rule.
pub fn strict_route_socket_mark(recovery_key: &str) -> u32 {
    const FNV_OFFSET: u32 = 0x811c_9dc5;
    const FNV_PRIME: u32 = 0x0100_0193;

    let mark = recovery_key.as_bytes().iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u32::from(*byte)).wrapping_mul(FNV_PRIME)
    });
    if mark == 0 {
        u32::MAX
    } else {
        mark
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
#[derive(Debug)]
pub struct SystemLeakGuard;

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
impl SystemLeakGuard {
    pub fn install(
        _tun_name: &str,
        _recovery_key: &str,
        _protected: &[IpNet],
        _excluded: &[IpAddr],
    ) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "strict-route leak protection is unsupported on this platform",
        ))
    }

    pub fn reconcile(&mut self, _protected: &[IpNet], _excluded: &[IpAddr]) -> io::Result<bool> {
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

fn normalized_prefixes(prefixes: &[IpNet]) -> Vec<IpNet> {
    let mut prefixes = prefixes.to_vec();
    prefixes.sort_unstable();
    prefixes.dedup();
    prefixes
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
                "192.0.2.1".parse::<IpAddr>().unwrap(),
                "2001:db8::1".parse::<IpAddr>().unwrap(),
            ]
        );
    }

    #[test]
    fn rejects_firewall_metacharacters_in_interface_names() {
        assert!(validate_interface_name("znet-tun0").is_ok());
        assert!(validate_interface_name("bad\"name").is_err());
        assert!(validate_interface_name("bad\nname").is_err());
    }

    #[test]
    fn strict_socket_marks_are_stable_non_zero_and_instance_scoped() {
        let mark = strict_route_socket_mark("tun-in");
        assert_ne!(mark, 0);
        assert_eq!(mark, strict_route_socket_mark("tun-in"));
        assert_ne!(mark, strict_route_socket_mark("other-tun"));
    }
}
