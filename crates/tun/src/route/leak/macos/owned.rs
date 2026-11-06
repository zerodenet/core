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
        verify_anchor_namespace()?;
        let anchor = format!("com.apple/zero_{}", safe_resource_name(recovery_key));
        let protected = normalized_prefixes(protected);
        let excluded = normalized_exclusions(excluded);
        *original = Some(Self {
            anchor,
            tun_name: tun_name.to_owned(),
            protected,
            excluded,
            enable_tokens: Vec::new(),
            installed_rules: String::new(),
            active: true,
        });
        let guard = original
            .as_mut()
            .expect("same installed leak-protection original");
        apply_policy(&guard.anchor, tun_name, &guard.protected, &guard.excluded)?;
        guard.installed_rules = super::super::audit::snapshot(&anchor_rules(&guard.anchor)?)?;
        if !pf_enabled()? {
            guard.enable_tokens.push(enable_pf()?);
            if !pf_enabled()? {
                return Err(io::Error::other("pf is still disabled after installation"));
            }
        }
        Ok(())
    }
}
