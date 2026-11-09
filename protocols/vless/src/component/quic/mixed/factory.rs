//! Both real half issuers register before fallible material or first IO.
mod lifecycle;
use super::*;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::*,
    lifecycle::{erase_lifecycle, BuildError},
    network::UpstreamTransport,
    scope::TaskRegistrar,
};
use zero_traits::packet_metadata::PacketMetadataArc;
pub struct Factory;
pub(super) struct Shared {
    pub options: Arc<Options>,
    pub upload: opening::Half,
    pub download: Option<opening::Half>,
    pub instance: zero_core::component::ComponentInstanceId,
    pub tasks: TaskRegistrar,
    pub memory: PacketDriverMetadataView,
    pub buffers: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    pub entries: PacketMetadataArc<Mutex<entry::Table>>,
}
pub(super) struct Outbound {
    pub shared: PacketMetadataArc<Shared>,
    pub admission: Arc<SessionAdmission>,
}
struct Owner {
    shared: PacketMetadataArc<Shared>,
    upload: runtime::Runtime,
    download: Option<runtime::Runtime>,
    registered: [bool; 2],
    started: bool,
    session: SessionOwner,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let description = resources::Description::new(options);
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: std::iter::once(RoleKind::Stream)
                .chain(
                    options
                        .upload
                        .protocol()
                        .flow
                        .is_none()
                        .then_some(RoleKind::Datagram),
                )
                .collect(),
            dependencies: options.dependencies.clone(),
            resources: description.resources,
            static_identity: options.identity.clone(),
            task_limit: Some(description.tasks),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let upload = context
            .background_carrier
            .clone()
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        let download = match options.download_endpoint() {
            None => None,
            Some(endpoint)
                if endpoint == options.endpoint()
                    && options.download_dial() == Some(options.dial()) =>
            {
                Some(upload.clone())
            }
            Some(_) => return Err(error(Stage::Prepare, Reason::InvalidConfig)),
        };
        self.build_with_upstreams(options, context, upload, download, None)
    }
}
impl Factory {
    /// Actual Proxy assembly supplies the two existing physical endpoint sources.
    /// Each source retains its ordinary Host/Scope/material/Node authority.
    pub fn build_with_upstreams(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        upload: UpstreamTransport,
        download: Option<UpstreamTransport>,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        if options.download.is_some() != download.is_some() {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let memory = context.resources.packet_driver_metadata()?;
        let buffers = context
            .resources
            .carrier_buffer_budget()
            .ok_or_else(|| error(Stage::Prepare, Reason::Unsupported))?;
        let entries = entry::Table::shared(&memory, options.origins.get())?;
        let description = resources::Description::new(&options);
        let upload = runtime::Runtime::build(
            &options.upload,
            description.upload,
            &context,
            upload,
            &entries,
            resolver.clone(),
        )?;
        let download = options
            .download
            .as_ref()
            .zip(description.download)
            .zip(download)
            .map(|((half, binding), upstream)| {
                runtime::Runtime::build(
                    half,
                    binding,
                    &context,
                    upstream,
                    &entries,
                    resolver.clone(),
                )
            })
            .transpose()?;
        let header = memory
            .reserve(PacketMetadataArc::<Shared>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let (session, admission) = SessionOwner::new(context.component, &context.cancel);
        let shared = PacketMetadataArc::from_prepaid(
            Shared {
                options,
                upload: opening::Half::from_runtime(&upload),
                download: download.as_ref().map(opening::Half::from_runtime),
                instance: context.instance(),
                tasks: context.tasks.clone(),
                memory,
                buffers,
                entries,
            },
            header,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                shared: shared.clone(),
                upload,
                download,
                registered: [false; 2],
                started: false,
                session,
            }),
            roles: ComponentRoles {
                stream: Some(zero_platform_tokio::contract::stream::erase_stream(
                    Outbound {
                        shared: shared.clone(),
                        admission: admission.clone(),
                    },
                )),
                datagram: shared.options.upload.protocol().flow.is_none().then(|| {
                    zero_platform_tokio::contract::datagram::erase_datagram(Outbound {
                        shared,
                        admission,
                    })
                }),
                ..Default::default()
            },
        })
    }
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}
