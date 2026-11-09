//! Selected actual half, with its first preinstalled Entry and later real misses.
mod pending;
mod selection;
use super::*;
use zero_platform_tokio::contract::context::OpenContext;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout};
use zero_transport::split_http::{OwnedXhttpCarrierFactory, XhttpCarrier};
#[derive(Clone)]
pub(super) enum Half {
    Quic(PacketMetadataArc<entry::Generation>),
    Tcp(PacketMetadataArc<runtime::TcpGeneration>),
}
pub(super) enum Prepared {
    Quic(connect::prepared_original::PreparedOriginal),
    Tcp(tcp::prepared::PreparedTcpOriginal),
}
impl Half {
    pub fn from_runtime(runtime: &runtime::Runtime) -> Self {
        match runtime {
            runtime::Runtime::Quic { generation, .. } => Self::Quic(generation.clone()),
            runtime::Runtime::Tcp { generation, .. } => Self::Tcp(generation.clone()),
        }
    }
    pub fn memory(&self) -> &PacketDriverMetadataView {
        match self {
            Self::Quic(generation) => &generation.metadata_memory,
            Self::Tcp(generation) => &generation.memory,
        }
    }
    pub async fn prepare(
        &self,
        position: entry::Position,
        context: &OpenContext,
    ) -> Result<Prepared, ChannelError> {
        match self {
            Self::Quic(generation) => {
                connect::prepared_original::PreparedOriginal::prepare(generation, position, context)
                    .await
                    .map(Prepared::Quic)
            }
            Self::Tcp(generation) => {
                tcp::prepared::PreparedTcpOriginal::new(generation, position, context)
                    .await
                    .map(Prepared::Tcp)
            }
        }
    }
    async fn current(&self, context: &OpenContext) -> Result<OpenContext, ChannelError> {
        // The logical caller remains real and current. A cached driver proof
        // cannot revive a cancelled/expired Query or impersonate its identity.
        if context.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Cancelled));
        }
        if context
            .deadline
            .is_some_and(|end| tokio::time::Instant::now() >= end)
        {
            return Err(error(Stage::Prepare, Reason::Timeout));
        }
        let physical = match self {
            Self::Quic(generation) => {
                super::super::pending::for_current(generation, context.upstream.binding())?
            }
            Self::Tcp(generation) => pending::for_current(generation, context.upstream.binding())?,
        };
        let mut context = context.clone();
        if let Some((upstream, stop)) = physical {
            // Only the actual already-owned driver/receiver/native task above
            // authorizes this physical continuation. Identity stays unchanged;
            // its real current logical work still owns/drops this factory call.
            context.upstream = upstream;
            context.cancel = stop;
            context.deadline = None;
        }
        Ok(context)
    }
    async fn open(
        &self,
        context: OpenContext,
        prepared: Option<Prepared>,
    ) -> Result<XhttpCarrier, ChannelError> {
        let context = self.current(&context).await?;
        match (self, prepared) {
            (Self::Quic(_), Some(Prepared::Quic(prepared))) => Ok(XhttpCarrier::OwnedHttp3(
                entry::Generation::acquire_prepared(prepared, &context)
                    .await?
                    .into_h3(),
            )),
            (Self::Quic(generation), None) => Ok(XhttpCarrier::OwnedHttp3(
                entry::Generation::acquire(generation, &context)
                    .await?
                    .into_h3(),
            )),
            (Self::Tcp(generation), prepared) => {
                let prepared = match prepared {
                    Some(Prepared::Tcp(prepared)) => prepared,
                    None => {
                        entry::Table::reap(&generation.entries);
                        let [position, _] =
                            entry::Table::reserve_needed(&generation.entries, [true, false])?;
                        tcp::prepared::PreparedTcpOriginal::new(
                            generation,
                            position.unwrap(),
                            &context,
                        )
                        .await?
                    }
                    _ => return Err(error(Stage::Prepare, Reason::StalePlan)),
                };
                Ok(XhttpCarrier::OwnedTcp(prepared.start()?.ready().await?))
            }
            _ => Err(error(Stage::Prepare, Reason::StalePlan)),
        }
    }
}
struct Initial {
    first: Mutex<(
        Option<Prepared>,
        Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    )>,
}
pub(super) struct FactoryClaim {
    pub factory: OwnedXhttpCarrierFactory,
    initial: PacketMetadataArc<Initial>,
    frame: PacketMetadataLayout,
    memory: PacketDriverMetadataView,
}
impl FactoryClaim {
    pub fn new(half: Half, context: OpenContext) -> Result<Self, ChannelError> {
        let memory = half.memory().clone();
        let credit = memory
            .reserve(PacketMetadataArc::<Initial>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let initial = PacketMetadataArc::from_prepaid(
            Initial {
                first: Mutex::new((None, None)),
            },
            credit,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        // Same concrete function/future type for both initial and later calls.
        // No IO is polled while its actual fixed frame layout is observed.
        let sample = work(half.clone(), context.clone(), None);
        let frame = zero_transport::split_http::OwnedXhttpFactoryCall::metadata_layout_for(&sample);
        drop(sample);
        let slot = initial.clone();
        let call_memory = memory.clone();
        let callback = move || {
            let (first, frame) = { std::mem::take(&mut *slot.first.lock().unwrap()) };
            let work = work(half.clone(), context.clone(), first);
            match frame {
                Some(credit) => {
                    zero_transport::split_http::OwnedXhttpFactoryCall::from_prepaid(work, credit)
                        .unwrap_or_else(|_| panic!("same initial factory frame promised"))
                }
                None => {
                    zero_transport::split_http::OwnedXhttpFactoryCall::prepare(&call_memory, work)
                }
            }
        };
        let layout = OwnedXhttpCarrierFactory::metadata_layout_for(&callback).map_err(capacity)?;
        let credit = memory.reserve(layout).map_err(capacity)?;
        let factory = OwnedXhttpCarrierFactory::from_prepaid(callback, credit)
            .map_err(|(source, _, _)| capacity(source))?;
        Ok(Self {
            factory,
            initial,
            frame,
            memory,
        })
    }
    pub fn install(&self, prepared: Prepared) -> Result<(), ChannelError> {
        let credit = self.memory.reserve(self.frame).map_err(capacity)?;
        let previous = {
            std::mem::replace(
                &mut *self.initial.first.lock().unwrap(),
                (Some(prepared), Some(credit)),
            )
        };
        assert!(previous.0.is_none() && previous.1.is_none());
        drop(previous);
        Ok(())
    }
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}

async fn work(
    half: Half,
    context: OpenContext,
    first: Option<Prepared>,
) -> Result<XhttpCarrier, zero_transport::RuntimeError> {
    half.open(context, first)
        .await
        .map_err(|source| std::io::Error::other(source).into())
}
