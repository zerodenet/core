use super::*;
/// Facts of this installed owning factory. These values do not grant IO,
/// register a task or replace the actual carrier's sealed receiver.
pub struct OwnedXhttpContext<'a> {
    pub instance: zero_core::component::ComponentInstanceId,
    pub scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    pub binding: &'a zero_platform_tokio::contract::network::UpstreamBinding,
    pub memory: zero_platform_tokio::contract::component::PacketDriverMetadataView,
}
impl XhttpClientPool {
    /// Slot retirement removes only this actual factory's groups. Existing
    /// users/driver/Native/Host ACK still belong to their original Root rows.
    pub fn retire_owned(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        scope: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
    ) {
        loop {
            let removed = {
                let mut state = self.0.state.lock().unwrap();
                let index = state
                    .groups
                    .iter()
                    .position(|group| group.same_factory(instance, scope));
                index.map(|index| state.groups.remove(index))
            };
            match removed {
                Some(group) => drop(group),
                None => break,
            }
        }
    }
    async fn return_old_idle_originals(&self, owner: &OwnerKey) -> io::Result<()> {
        loop {
            let removed = {
                let mut state = self.0.state.lock().unwrap();
                let index = state.groups.iter().position(|group| {
                    group.same_factory(owner.instance, &owner.scope)
                        && !group.matches_owner(Some(owner))
                        && group.active() == 0
                        && !group.has_responses()
                });
                index.map(|index| state.groups.remove(index))
            };
            let Some(group) = removed else { return Ok(()) };
            let original = group.connections.retire_owned().await;
            drop(group);
            if let Some(original) = original {
                original
                    .wait_settled_original()
                    .await
                    .map_err(io::Error::other)?;
            }
        }
    }
    /// Same XMUX groups/reuse/request/age policy; every carrier must be a real
    /// owning H3 graph. A mismatched factory never starts a compatibility task.
    pub async fn connect_owned<P: SplitHttpTransportProfile + ?Sized>(
        &self,
        factory: XhttpCarrierFactory,
        config: &P,
        download: Option<XhttpDownload<'_>>,
        context: OwnedXhttpContext<'_>,
    ) -> Result<
        (
            XhttpStream,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ),
        crate::RuntimeError,
    > {
        let OwnedXhttpContext {
            instance,
            scope,
            binding,
            memory,
        } = context;
        let profile = Profile::new(config);
        if profile.mode == XhttpMode::StreamOne && download.is_some() {
            return Err(io::Error::other("stream-one cannot split download").into());
        }
        let usage_count = if download.is_some() { 2 } else { 1 };
        let usage_memory = memory
            .reserve(
                zero_traits::packet_metadata::PacketMetadataLayout::array::<UsageRef>(usage_count)
                    .map_err(io::Error::other)?,
            )
            .map_err(io::Error::other)?;
        let slots = Box::<[UsageRef]>::new_uninit_slice(usage_count);
        let ptr = Box::into_raw(slots) as *mut UsageRef;
        // SAFETY: exact prepaid array allocation; no live elements yet.
        let mut usages = unsafe { Vec::from_raw_parts(ptr, 0, usage_count) };
        let owner = OwnerKey::prepare(instance, scope, binding, memory)?;
        self.return_old_idle_originals(&owner).await?;
        let usage = UsageRef::new(self.select(Some(&owner))?, Some(&owner))?;
        let sender = request::Sender {
            pool: self.clone(),
            factory,
            usage: usage.clone(),
            owned: true,
            owner: Some(owner.clone()),
            logical: None,
        };
        let (runtime, origin) = sender.owned_original().await?;
        runtime.bind_pool(&self.0);
        let (down, download_profile, down_usage) = if let Some(download) = download {
            let usage = UsageRef::new(download.pool.select(Some(&owner))?, Some(&owner))?;
            let down = request::Sender {
                pool: download.pool.clone(),
                factory: download.factory,
                usage: usage.clone(),
                owned: true,
                owner: Some(owner.clone()),
                logical: None,
            };
            let down_runtime = down.owned_runtime().await?;
            down_runtime.bind_pool(&download.pool.0);
            (down, Profile::new(download.config), Some(usage))
        } else {
            (sender.clone(), profile.clone(), None)
        };
        let mut stream = owned::Runtime::connect_profiles(
            &runtime,
            client::carrier::Sender::Xmux(sender),
            client::carrier::Sender::Xmux(down),
            profile,
            download_profile,
        )
        .await?;
        usages.push(usage);
        if let Some(usage) = down_usage {
            usages.push(usage);
        }
        stream.usages = usages;
        stream._owned_usage_memory = Some(usage_memory);
        Ok((stream, origin))
    }
}
