use super::{select_usable_default_route, MIB_IPFORWARD_ROW2};

#[test]
fn skips_a_disconnected_lower_metric_default_route() {
    let routes = [default_route(18, 5), default_route(10, 45)];

    let selected = select_usable_default_route(
        &routes,
        77,
        |interface_index| interface_index != 18,
        |route| u64::from(route.Metric),
    )
    .expect("connected higher-metric route should remain eligible");

    assert_eq!(selected.InterfaceIndex, 10);
}

#[test]
fn excludes_tun_and_non_default_routes_before_ranking() {
    let mut non_default = default_route(5, 0);
    non_default.DestinationPrefix.PrefixLength = 1;
    let routes = [default_route(77, 0), non_default, default_route(18, 25)];

    let selected =
        select_usable_default_route(&routes, 77, |_| true, |route| u64::from(route.Metric))
            .expect("physical default route should remain eligible");

    assert_eq!(selected.InterfaceIndex, 18);
}

#[test]
fn returns_none_when_every_physical_default_route_is_unusable() {
    let routes = [default_route(18, 5), default_route(10, 45)];

    assert!(
        select_usable_default_route(&routes, 77, |_| false, |route| u64::from(route.Metric),)
            .is_none()
    );
}

fn default_route(interface_index: u32, metric: u32) -> MIB_IPFORWARD_ROW2 {
    let mut route = MIB_IPFORWARD_ROW2::default();
    route.InterfaceIndex = interface_index;
    route.DestinationPrefix.PrefixLength = 0;
    route.Metric = metric;
    route
}
