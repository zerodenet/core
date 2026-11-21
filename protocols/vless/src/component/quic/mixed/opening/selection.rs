//! Two actual selected cache misses are reserved before either can initialize.
use super::super::factory::Shared;
use super::*;
use zero_transport::split_http::{
    OwnedConnectionKinds, OwnedXhttpContext, OwnedXhttpDownload, XhttpStream,
};
impl Shared {
    pub async fn acquire_xhttp(
        &self,
        context: &OpenContext,
    ) -> Result<
        (
            XhttpStream,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ),
        ChannelError,
    > {
        let current_up = self.upload.current(context).await?;
        // The callable belongs to this logical caller. Freeze no selected
        // physical parent's stop token: every later miss selects the actual
        // currently polling driver/receiver again inside Half::open.
        let up = FactoryClaim::new(self.upload.clone(), context.clone())?;
        let current_down = match &self.download {
            Some(down) => Some(down.current(context).await?),
            None => None,
        };
        let down = self
            .download
            .as_ref()
            .zip(current_down.as_ref())
            .map(|(half, _)| FactoryClaim::new(half.clone(), context.clone()))
            .transpose()?;
        let (pool, profile) = self.options.upload.pool();
        let down_options = self.options.download.as_ref().map(options::Half::pool);
        let download = down_options
            .zip(down.as_ref())
            .map(|((pool, config), factory)| OwnedXhttpDownload {
                pool,
                config,
                factory: factory.factory.clone(),
            });
        let prepared = pool
            .prepare_owned(
                up.factory.clone(),
                profile,
                download,
                OwnedXhttpContext {
                    instance: self.instance,
                    scope: self.tasks.scope_identity(),
                    binding: context.upstream.binding(),
                    memory: self.memory.clone(),
                    buffers: self.buffers.clone(),
                },
                OwnedConnectionKinds {
                    upload: self.options.upload.kind(),
                    download: self
                        .options
                        .download
                        .as_ref()
                        .map_or(self.options.upload.kind(), options::Half::kind),
                },
            )
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?;
        let [up_position, down_position] = entry::Table::reserve_after_retiring(
            &self.entries,
            prepared.needs_originals(),
            context,
            &self.cancel,
        )
        .await?;
        if let Some(position) = up_position {
            up.install(self.upload.prepare(position, &current_up).await?)?;
        }
        if let Some(position) = down_position {
            let half = self
                .download
                .as_ref()
                .expect("selected real independent download");
            down.as_ref().unwrap().install(
                half.prepare(position, current_down.as_ref().unwrap())
                    .await?,
            )?;
        }
        // Each actual missing original is now strongly Root-held and published
        // to its real observer. Only this step may start either initializing Aux.
        prepared
            .finish()
            .await
            .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))
    }
}
