//! Actual fields and table mutations retain the same journal allocation owner.
use super::*;
impl RouteJournal {
    #[cfg(test)]
    pub(in crate::route) fn new(
        lease: RouteLease,
        tun_name: &str,
        ipv6: bool,
        tun_index: u32,
        egress: RouteInterface,
        gateway: Option<String>,
    ) -> io::Result<Self> {
        Self::new_borrowed(
            lease,
            tun_name,
            ipv6,
            tun_index,
            &egress,
            gateway.as_deref().map(Gateway::Text),
        )
    }
    pub(in crate::route) fn new_borrowed(
        lease: RouteLease,
        tun_name: &str,
        ipv6: bool,
        tun_index: u32,
        egress: &RouteInterface,
        gateway: Option<Gateway<'_>>,
    ) -> io::Result<Self> {
        let mut owner = metadata::Owner::new(lease.metadata());
        let tun_name = owner.string(tun_name).map_err(io::Error::other)?;
        let egress_name = owner.string(egress.name()).map_err(io::Error::other)?;
        let gateway = gateway.map(|value| value.copy(&mut owner)).transpose()?;
        let path = owner.path(&lease.journal_path).map_err(io::Error::other)?;
        Ok(Self {
            tun_name,
            ipv6,
            tun_index,
            egress: RouteInterface {
                name: egress_name,
                index: egress.index(),
            },
            gateway,
            excluded: Vec::new(),
            installed: Vec::new(),
            scoped_bypass: false,
            path,
            #[cfg(target_os = "macos")]
            source: lease.source(),
            _lease: Some(lease),
            _metadata: owner,
            _removed_routes: 0,
            _removed_exclusions: 0,
        })
    }

    pub(in crate::route) fn record_exclusion(&mut self, address: IpAddr) -> io::Result<()> {
        if !self.excluded.contains(&address) {
            self._metadata
                .push(&mut self.excluded, address)
                .map_err(|(error, _)| io::Error::other(error))?;
        }
        self.persist()
    }

    pub(in crate::route) fn forget_exclusion(&mut self, address: IpAddr) -> io::Result<()> {
        self.excluded.retain(|item| *item != address);
        self.persist_or_clear()
    }

    #[cfg(test)]
    pub(in crate::route) fn replace_egress(
        &mut self,
        egress: RouteInterface,
        gateway: Option<String>,
    ) -> io::Result<()> {
        self.replace_egress_borrowed(&egress, gateway.as_deref().map(Gateway::Text))
    }
    pub(in crate::route) fn replace_egress_borrowed(
        &mut self,
        egress: &RouteInterface,
        gateway: Option<Gateway<'_>>,
    ) -> io::Result<()> {
        let name = self
            ._metadata
            .string(egress.name())
            .map_err(io::Error::other)?;
        let gateway = match gateway
            .map(|value| value.copy(&mut self._metadata))
            .transpose()
        {
            Ok(gateway) => gateway,
            Err(error) => {
                self._metadata
                    .release_string(name)
                    .map_err(io::Error::other)?;
                return Err(error);
            }
        };
        let previous = std::mem::replace(
            &mut self.egress,
            RouteInterface {
                name,
                index: egress.index(),
            },
        );
        self._metadata
            .release_string(previous.name)
            .map_err(io::Error::other)?;
        if let Some(previous) = std::mem::replace(&mut self.gateway, gateway) {
            self._metadata
                .release_string(previous)
                .map_err(io::Error::other)?;
        }
        self.persist()
    }

    pub(in crate::route) fn record_route(&mut self, prefix: &str) -> io::Result<()> {
        let prefix = self._metadata.string(prefix).map_err(io::Error::other)?;
        if let Err((error, original)) = self._metadata.push(&mut self.installed, prefix) {
            self._metadata
                .release_string(original)
                .map_err(io::Error::other)?;
            return Err(io::Error::other(error));
        }
        self.persist()
    }
}
