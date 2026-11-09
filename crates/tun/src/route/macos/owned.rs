//! Native installation retains the original before mutation or failed recovery.
use super::*;
impl SystemRouteGuard {
    /// Install into the caller's persistent original slot before changing
    /// native routes. Errors retain this exact lease, journal and partial work.
    pub fn install_retained_with_egress(
        original: &mut Option<Self>,
        request: crate::route::RouteInstallRequest<'_>,
        publish_egress: impl FnOnce(&FamilyEgressState) -> io::Result<()>,
    ) -> io::Result<()> {
        if original.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "previous route original still requires confirmed cleanup",
            ));
        }
        let crate::route::RouteInstallRequest {
            tun_name,
            recovery_key,
            address,
            netmask,
            captured,
            excluded,
        } = request;
        let ipv6 = address.is_ipv6();
        let lease = RouteLease::acquire(recovery_key, ipv6)?;
        let selected = select_physical_egress(ipv6, tun_name)?;
        let egress = selected.carrier;
        let gateway = selected.gateway;
        let tun_gateway = match address {
            IpAddr::V4(address) => Some(crate::macos::ipv4_peer(address, netmask)?.to_string()),
            IpAddr::V6(_) => None,
        };
        let journal = RouteJournal::new(lease, tun_name, ipv6, 0, egress.clone(), gateway.clone())?;
        let connected = exclusions::connected_networks(tun_name)?;
        let desired_exclusions = family_exclusions_for_egress(excluded, ipv6, &selected.family)
            .into_iter()
            .filter(|peer| {
                exclusions::requires_host_bypass(*peer, captured, &connected)
                    && !exclusions::has_native_route(*peer, captured, tun_name)
            })
            .collect::<Vec<_>>();
        *original = Some(Self {
            egress,
            family_egress: selected.family,
            tun_name: tun_name.to_owned(),
            ipv6,
            gateway,
            tun_gateway,
            excluded: desired_exclusions.clone(),
            captured: captured.to_vec(),
            journal,
            stale: None,
            recovered: false,
        });
        let guard = original.as_mut().expect("same installed route original");
        guard.recover_stale()?;
        publish_egress(&guard.family_egress)?;
        guard.install_scoped_bypass()?;
        for peer in desired_exclusions {
            guard.install_exclusion(peer)?;
        }
        for prefix in captured {
            let prefix = prefix.to_string();
            let _ = guard.remove(&prefix);
            guard.journal.record_route(&prefix)?;
            guard.add(&prefix)?;
        }
        guard.audit_routes()?;
        Ok(())
    }
    pub(super) fn recover_stale(&mut self) -> io::Result<()> {
        if self.recovered {
            return Ok(());
        }
        if self.stale.is_none() {
            let lease = self
                .journal
                ._lease
                .as_ref()
                .expect("same original route lease");
            self.stale = RouteJournal::load(lease, self.ipv6)?;
        }
        if let Some(stale) = self.stale.as_mut() {
            cleanup_stale_routes(stale)?;
        }
        self.stale.take();
        self.recovered = true;
        Ok(())
    }
}
