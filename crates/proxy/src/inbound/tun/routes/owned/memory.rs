//! Exact known route-owner layouts, all paid before new owning allocations.
use super::*;
impl OwnedRoutes {
    pub(in crate::inbound::tun) fn layout(
        start: &crate::inbound::tun::managed::Start,
    ) -> Result<PacketMetadataLayout, ChannelError> {
        let mut spec = PacketMetadataLayout::array::<(IpAddr, IpAddr)>(start.route_addresses.len())
            .map_err(|source| {
                error(Reason::Capacity, "route address layout overflow").with_source(source)
            })?;
        for layout in [
            PacketMetadataLayout::array::<ipnet::IpNet>(start.options.include_cidrs.len()),
            PacketMetadataLayout::array::<ipnet::IpNet>(start.options.exclude_cidrs.len()),
            PacketMetadataLayout::array::<u8>(start.tag.len()),
        ] {
            spec = spec
                .checked_add(layout.map_err(|source| {
                    error(Reason::Capacity, "route input layout overflow").with_source(source)
                })?)
                .map_err(|source| {
                    error(Reason::Capacity, "route input layout overflow").with_source(source)
                })?;
        }
        PacketMetadataArc::<Shared>::layout()
            .and_then(|layout| layout.checked_add(spec))
            .and_then(|layout| {
                layout.checked_add(native::Native::families_layout(
                    start.route_addresses.len(),
                )?)
            })
            .map_err(|source| {
                error(Reason::Capacity, "actual route owner layout cannot fit").with_source(source)
            })
    }
    pub(in crate::inbound::tun) fn from_prepaid(
        proxy: Proxy,
        spec: RouteRuntimeSpec,
        metadata: Arc<crate::inbound::tun::source::SystemTunSourceState>,
        cancel: CancellationToken,
        mut credit: PacketMetadataCredit,
    ) -> Result<Self, ChannelError> {
        let owner = PacketMetadataArc::<Shared>::layout().map_err(|source| {
            error(Reason::Capacity, "route owner layout overflow").with_source(source)
        })?;
        let families = native::Native::families_layout(spec.addresses.len()).map_err(|source| {
            error(Reason::Capacity, "route family layout overflow").with_source(source)
        })?;
        let family_credit = credit.split(families).map_err(|source| {
            error(Reason::Internal, "route families were not prepaid").with_source(source)
        })?;
        let shared_credit = credit.split(owner).map_err(|source| {
            error(Reason::Internal, "route owner was not prepaid").with_source(source)
        })?;
        let (jobs, received) = mpsc::channel(1);
        PacketMetadataArc::from_prepaid(
            Shared {
                proxy,
                metadata,
                native: Mutex::new(native::Native::from_prepaid(spec, family_credit, credit)?),
                observed: Mutex::default(),
                changed: Notify::new(),
                cancel,
                closing: AtomicBool::new(false),
                jobs,
                received: Mutex::new(Some(received)),
                monitor_io: tokio::sync::Mutex::new(RouteChangeMonitor::prepare()),
                worker: OnceLock::new(),
                monitor: OnceLock::new(),
            },
            shared_credit,
        )
        .map(Self)
        .map_err(|(source, _, _)| {
            error(Reason::Internal, "same route owner credit mismatch").with_source(source)
        })
    }
}

fn copy_array<T: Copy>(input: &[T]) -> Vec<T> {
    let mut values = Box::<[T]>::new_uninit_slice(input.len());
    for (target, value) in values.iter_mut().zip(input) {
        target.write(*value);
    }
    unsafe { values.assume_init() }.into_vec()
}
pub(in crate::inbound::tun) fn copy_addresses(input: &[(IpAddr, IpAddr)]) -> Vec<(IpAddr, IpAddr)> {
    copy_array(input)
}
pub(in crate::inbound::tun) fn copy_prefixes(input: &[ipnet::IpNet]) -> Vec<ipnet::IpNet> {
    copy_array(input)
}
pub(in crate::inbound::tun) fn copy_key(input: &str) -> String {
    unsafe { String::from_utf8_unchecked(copy_array(input.as_bytes())) }
}
