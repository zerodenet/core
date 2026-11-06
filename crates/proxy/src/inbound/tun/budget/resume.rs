//! Cancelled starts retain this exact capsule on the new original control.
use super::*;
impl Proxy {
    pub(super) async fn resume_tun_budget_locked(
        &self,
        config: Arc<zero_config::RuntimeConfig>,
    ) -> Result<(), EngineError> {
        let Some((id, completion, recovery)) =
            self.tun_control
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|control| {
                    control
                        .budget_recovery
                        .clone()
                        .map(|recovery| (control.id, control.completion.clone(), recovery))
                })
        else {
            return Ok(());
        };
        let desired = config.runtime.tun.as_ref();
        recovery.validate_desired(desired)?;
        {
            let resumed = recovery.resumed.lock().unwrap();
            if resumed.as_ref().is_some_and(|resumed| {
                resumed.id == id
                    && resumed
                        .config
                        .upgrade()
                        .is_some_and(|actual| Arc::ptr_eq(&actual, &config))
            }) {
                if completion.source_running()
                    || resumed.as_ref().is_some_and(|state| state.removed)
                {
                    return Ok(());
                }
            }
        }
        recovery.resumed.lock().unwrap().take();
        if !completion.resources_complete().await {
            self.stop_tun_locked(true, true).await?;
        }
        if !completion.resources_complete().await || !recovery.original.resources_complete().await {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "System TUN budget recovery still retains original users/Scope/native cleanup",
            )
            .into());
        }
        if recovery.restart.managed_config.is_some() && desired.is_none() {
            // Explicit configured withdrawal. Keep the actual closed control
            // and intent until the matching config transaction commits.
            recovery.record_resumed(&config, id, true);
            return Ok(());
        }
        if recovery.restart.managed_config.as_ref() == desired
            && desired.is_none_or(|desired| {
                desired.effective_mtu(config.runtime.network.mtu) == recovery.restart.mtu
            })
        {
            self.start_managed_tun(
                (*recovery.restart).clone(),
                config.clone(),
                Some(recovery.clone()),
            )
            .await?;
        } else {
            let desired = desired.expect("configured restore or retained command input");
            self.start_tun_prepared(
                TunStartSpec {
                    name: desired.name.as_deref(),
                    addr: &desired.addr,
                    mask: &desired.mask,
                    secondary_addr: desired.secondary_addr.as_deref(),
                    mtu: desired.effective_mtu(config.runtime.network.mtu),
                    tag: &desired.tag,
                    options: TunRuntimeOptions {
                        auto_route: desired.auto_route,
                        include_cidrs: desired.include_cidrs.clone(),
                        exclude_cidrs: desired.exclude_cidrs.clone(),
                        dual_stack: desired.dual_stack,
                        strict_route: desired.strict_route,
                        dns_hijack: desired.dns_hijack,
                    },
                    managed_config: Some(desired.clone()),
                    prepared_network: None,
                },
                TunStartPreparation {
                    config: Some(config.clone()),
                    recovery: Some(recovery.clone()),
                },
            )
            .await?;
        }
        let controls = self.tun_control.lock().unwrap();
        let control = controls
            .as_ref()
            .filter(|control| {
                control
                    .budget_recovery
                    .as_ref()
                    .is_some_and(|actual| Arc::ptr_eq(actual, &recovery))
                    && control.id != recovery.original_id
                    && control.completion.source_running()
            })
            .ok_or_else(|| {
                io::Error::other(
                    "restored System TUN changed its actual recovery or failed readiness",
                )
            })?;
        recovery.record_resumed(&config, control.id, false);
        Ok(())
    }
}
