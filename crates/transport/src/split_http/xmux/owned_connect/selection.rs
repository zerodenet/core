//! Concrete selection of existing groups, before either physical factory IO.
//! Claims hold the actual cached connection or its original opening gate.
use super::*;
use zero_traits::packet_metadata::PacketMetadataCredit;
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OwnedConnectionKind {
    Http1,
    Multiplexed,
}
#[derive(Clone, Copy)]
pub struct OwnedConnectionKinds {
    pub upload: OwnedConnectionKind,
    pub download: OwnedConnectionKind,
}
impl OwnedConnectionKinds {
    pub const fn multiplexed() -> Self {
        Self {
            upload: OwnedConnectionKind::Multiplexed,
            download: OwnedConnectionKind::Multiplexed,
        }
    }
}
struct Half {
    sender: request::Sender,
    profile: Profile,
    claim: Option<connection::ConnectionClaim>,
}
struct UsageStorage {
    // Actual backing dies before its ticket, also after any suspended/error
    // path in finish; destructuring independent locals would reverse this.
    values: Vec<UsageRef>,
    memory: PacketMetadataCredit,
}
pub struct PreparedOwnedXhttp {
    upload: Half,
    download: Option<Half>,
    usages: UsageStorage,
    logical: owned::PreparedLogical,
}
impl XhttpClientPool {
    pub async fn prepare_owned<P: SplitHttpTransportProfile + ?Sized>(
        &self,
        factory: OwnedXhttpCarrierFactory,
        config: &P,
        download: Option<OwnedXhttpDownload<'_>>,
        context: OwnedXhttpContext<'_>,
        kinds: OwnedConnectionKinds,
    ) -> Result<PreparedOwnedXhttp, crate::RuntimeError> {
        let OwnedXhttpContext {
            instance,
            scope,
            binding,
            memory,
            buffers,
        } = context;
        let profile = Profile::new(config);
        if profile.mode == XhttpMode::StreamOne && download.is_some() {
            return Err(io::Error::other("stream-one cannot split download").into());
        }
        // Reserve the real duplex/scratch/transmit peak and lifetime header
        // before either physical factory may acquire its first native IO.
        let logical = owned::PreparedLogical::prepare(&buffers, &memory, &profile)?;
        let count = if download.is_some() { 2 } else { 1 };
        let usage_memory = memory
            .reserve(
                zero_traits::packet_metadata::PacketMetadataLayout::array::<UsageRef>(count)
                    .map_err(io::Error::other)?,
            )
            .map_err(io::Error::other)?;
        let allocation = Box::<[UsageRef]>::new_uninit_slice(count);
        let ptr = Box::into_raw(allocation) as *mut UsageRef;
        // SAFETY: original exact prepaid array; zero initialized elements.
        let mut usages = UsageStorage {
            values: unsafe { Vec::from_raw_parts(ptr, 0, count) },
            memory: usage_memory,
        };
        let owner = OwnerKey::prepare(instance, scope, binding, memory)?;
        self.return_old_idle_originals(&owner).await?;
        let usage = UsageRef::new(self.select(Some(&owner))?, Some(&owner))?;
        let group = { usage.lock().unwrap().group.clone() };
        let claim = group.connections.select_owned(&group, kinds.upload).await?;
        usages.values.push(usage.clone());
        let upload = Half {
            sender: request::Sender {
                pool: self.clone(),
                factory,
                usage,
                owned: true,
                owner: Some(owner.clone()),
                logical: None,
            },
            profile,
            claim: Some(claim),
        };
        let download = if let Some(download) = download {
            download.pool.return_old_idle_originals(&owner).await?;
            let usage = UsageRef::new(download.pool.select(Some(&owner))?, Some(&owner))?;
            let group = { usage.lock().unwrap().group.clone() };
            // Identical multiplexed groups share their one exact opening
            // claim. Waiting for our own unstarted upload would self-deadlock.
            let shared = kinds.upload == OwnedConnectionKind::Multiplexed
                && kinds.download == kinds.upload
                && upload.claim.as_ref().unwrap().same_group(&group);
            let claim = if shared {
                None
            } else {
                Some(
                    group
                        .connections
                        .select_owned(&group, kinds.download)
                        .await?,
                )
            };
            usages.values.push(usage.clone());
            Some(Half {
                sender: request::Sender {
                    pool: download.pool.clone(),
                    factory: download.factory,
                    usage,
                    owned: true,
                    owner: Some(owner),
                    logical: None,
                },
                profile: Profile::new(download.config),
                claim,
            })
        } else {
            None
        };
        Ok(PreparedOwnedXhttp {
            upload,
            download,
            usages,
            logical,
        })
    }
}
impl PreparedOwnedXhttp {
    /// Pure cache-admission facts. This does not authorize IO or substitute
    /// for either half's real original grant, controller, Scope or receiver.
    pub fn needs_originals(&self) -> [bool; 2] {
        [
            self.upload
                .claim
                .as_ref()
                .is_some_and(|claim| claim.needs_original()),
            self.download
                .as_ref()
                .and_then(|half| half.claim.as_ref())
                .is_some_and(|claim| claim.needs_original()),
        ]
    }
    pub async fn finish(
        self,
    ) -> Result<
        (
            XhttpStream,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ),
        crate::RuntimeError,
    > {
        let Self {
            mut upload,
            mut download,
            usages,
            logical,
        } = self;
        let connection = upload
            .claim
            .take()
            .unwrap()
            .finish(&upload.sender.factory)
            .await?;
        let runtime = connection
            .owned_runtime()
            .ok_or_else(|| io::Error::from(io::ErrorKind::PermissionDenied))?;
        let origin = connection
            .owned_origin()
            .ok_or_else(|| io::Error::from(io::ErrorKind::PermissionDenied))?;
        runtime.bind_pool(&upload.sender.pool.0);
        return_initial(&upload, connection);
        if let Some(download) = &mut download {
            if let Some(claim) = download.claim.take() {
                let connection = claim.finish(&download.sender.factory).await?;
                let down_runtime = connection
                    .owned_runtime()
                    .ok_or_else(|| io::Error::from(io::ErrorKind::PermissionDenied))?;
                down_runtime.bind_pool(&download.sender.pool.0);
                return_initial(download, connection);
            }
        }
        let (down, down_profile) = if let Some(download) = download {
            (download.sender, download.profile)
        } else {
            (upload.sender.clone(), upload.profile.clone())
        };
        let mut stream = owned::Runtime::connect_prepared(
            &runtime,
            client::carrier::Sender::Xmux(upload.sender),
            client::carrier::Sender::Xmux(down),
            upload.profile,
            down_profile,
            logical,
        )
        .await?;
        stream.usages = usages.values;
        stream._owned_usage_memory = Some(usages.memory);
        Ok((stream, origin))
    }
}
fn return_initial(half: &Half, connection: connection::Connection) {
    if matches!(connection.sender, client::carrier::Sender::Http1(_)) {
        let group = { half.sender.usage.lock().unwrap().group.clone() };
        group.connections.recycle(connection);
    }
}

/// The two original fields, with closed callback allocation custody.
pub struct OwnedXhttpDownload<'a> {
    pub pool: &'a XhttpClientPool,
    pub factory: OwnedXhttpCarrierFactory,
    pub config: &'a (dyn SplitHttpTransportProfile + Sync),
}
