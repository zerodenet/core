//! The same firewall original persists across failed native installation.
use super::*;
impl SystemLeakGuard {
    /// The caller retains this exact native controller even if mutation or
    /// readback fails. Another install cannot replace unclosed partial work.
    pub fn install_retained(
        original: &mut Option<Self>,
        tun_name: &str,
        recovery_key: &str,
        protected: &[IpNet],
        excluded: &[IpAddr],
    ) -> io::Result<()> {
        if original.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "previous leak-protection original still requires confirmed cleanup",
            ));
        }
        validate_interface_name(tun_name)?;
        let table = format!("zero_killswitch_{}", safe_resource_name(recovery_key));
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        let socket_mark = strict_route_socket_mark(recovery_key);
        let exists = table_snapshot(&table)?.is_some();
        *original = Some(Self {
            installed_rules: String::new(),
            table,
            tun_name: tun_name.to_owned(),
            protected,
            excluded,
            socket_mark,
            active: true,
        });
        let guard = original
            .as_mut()
            .expect("same installed leak-protection original");
        apply_policy(
            &guard.table,
            tun_name,
            &guard.protected,
            &guard.excluded,
            socket_mark,
            exists,
        )?;
        guard.installed_rules = required_table_snapshot(&guard.table)?;
        Ok(())
    }
}
