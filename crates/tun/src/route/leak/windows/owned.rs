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

        let safe_name = safe_resource_name(recovery_key);
        let display_name = format!("Zero strict route ({safe_name})");
        let sublayer_key = stable_guid(recovery_key, "sublayer");
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        *original = Some(Self {
            sublayer_key,
            recovery_key: recovery_key.to_owned(),
            display_name,
            tun_name: tun_name.to_owned(),
            protected,
            excluded,
            active: true,
            legacy_migrated: false,
        });
        let guard = original
            .as_mut()
            .expect("same installed leak-protection original");
        migrate_legacy_policy(recovery_key)?;
        guard.legacy_migrated = true;
        install_policy(
            guard.sublayer_key,
            recovery_key,
            &guard.display_name,
            tun_name,
            &guard.protected,
            &guard.excluded,
        )
    }
}
