use std::cell::{Cell, RefCell};
use std::io;

use super::{reconcile_with, table, ExpectedRoute, RouteEntry, RouteKind};

fn expected() -> Vec<ExpectedRoute> {
    vec![
        ExpectedRoute {
            kind: RouteKind::Scoped,
            prefix: "0.0.0.0/0".parse().unwrap(),
            interface: "en5".into(),
            gateway: Some("192.0.2.1".into()),
        },
        ExpectedRoute {
            kind: RouteKind::Exclusion("8.8.8.8".parse().unwrap()),
            prefix: "8.8.8.8/32".parse().unwrap(),
            interface: "en5".into(),
            gateway: Some("192.0.2.1".into()),
        },
        ExpectedRoute {
            kind: RouteKind::Capture,
            prefix: "128.0.0.0/1".parse().unwrap(),
            interface: "utun7".into(),
            gateway: Some("10.66.0.2".into()),
        },
    ]
}

fn entry(route: &ExpectedRoute) -> RouteEntry {
    RouteEntry {
        prefix: route.prefix,
        gateway: route.gateway.clone().unwrap(),
        interface: route.interface.clone(),
        flags: if matches!(route.kind, RouteKind::Scoped) {
            "UGSI"
        } else {
            "UGS"
        }
        .into(),
    }
}

#[test]
fn identical_en5_return_repairs_all_missing_routes_once() {
    let expected = expected();
    let os = RefCell::new(Vec::new());
    let repairs = Cell::new(0);
    let recover = || {
        reconcile_with(
            &expected,
            || Ok(os.borrow().clone()),
            |route| {
                repairs.set(repairs.get() + 1);
                os.borrow_mut().push(entry(route));
                Ok(())
            },
        )
    };
    assert!(recover().unwrap());
    assert_eq!(repairs.get(), 3);
    assert!(!recover().unwrap());
    assert_eq!(repairs.get(), 3);
    os.borrow_mut().clear(); // Same interface/gateway, OS lost its routes again.
    assert!(recover().unwrap());
    assert_eq!(repairs.get(), 6);
}

#[test]
fn native_scoped_route_is_borrowed_without_installing_or_claiming_it() {
    let expected = expected();
    let os = expected.iter().map(entry).collect::<Vec<_>>();
    assert!(!reconcile_with(
        &expected,
        || Ok(os.clone()),
        |_| panic!("must not mutate existing routes")
    )
    .unwrap());
}

#[test]
fn unrelated_native_default_does_not_replace_a_missing_scoped_bypass() {
    let expected = expected();
    let mut native = entry(&expected[0]);
    native.flags = "UGS".into();
    assert!(!expected[0].present(&[native]).unwrap());
}

#[test]
fn unrelated_route_conflicts_are_reported_before_any_repair() {
    let expected = expected();
    let mut foreign = entry(&expected[2]);
    foreign.interface = "utun99".into();
    let error = reconcile_with(
        &expected,
        || Ok(vec![foreign.clone()]),
        |_| panic!("no writes on conflict"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("conflict"));
}

#[test]
fn wrong_gateway_and_reject_routes_are_not_healthy() {
    let expected = expected();
    let mut wrong = entry(&expected[0]);
    wrong.gateway = "192.0.2.254".into();
    assert!(expected[0].present(&[wrong]).is_err());
    for flags in ["UGSR", "UGSB", "GS"] {
        let mut route = entry(&expected[2]);
        route.flags = flags.into();
        assert!(expected[2].present(&[route]).is_err());
    }
}

#[test]
fn successful_commands_without_actual_routes_do_not_acknowledge_recovery() {
    let error = reconcile_with(&expected(), || Ok(vec![]), |_| Ok(())).unwrap_err();
    assert!(error.to_string().contains("still missing"));
}

#[test]
fn partial_failure_is_retried_without_reinstalling_restored_routes() {
    let expected = expected();
    let os = RefCell::new(Vec::new());
    let installs = Cell::new(0);
    let error = reconcile_with(
        &expected,
        || Ok(os.borrow().clone()),
        |route| {
            installs.set(installs.get() + 1);
            if installs.get() == 2 {
                return Err(io::Error::other("network disappeared"));
            }
            os.borrow_mut().push(entry(route));
            Ok(())
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("network disappeared"));
    assert_eq!(os.borrow().len(), 1);
    let mut retry_installs = 0;
    assert!(reconcile_with(
        &expected,
        || Ok(os.borrow().clone()),
        |route| {
            retry_installs += 1;
            os.borrow_mut().push(entry(route));
            Ok(())
        }
    )
    .unwrap());
    assert_eq!(retry_installs, 2);
}

#[test]
fn query_and_readback_errors_are_not_treated_as_empty_tables() {
    assert!(reconcile_with(
        &expected(),
        || Err(io::Error::other("cannot read table")),
        |_| panic!("no writes")
    )
    .is_err());
    let reads = Cell::new(0);
    assert!(reconcile_with(
        &expected(),
        || {
            reads.set(reads.get() + 1);
            if reads.get() == 1 {
                Ok(vec![])
            } else {
                Err(io::Error::other("readback failed"))
            }
        },
        |_| Ok(())
    )
    .is_err());
}

#[test]
fn parses_bsd_abbreviated_networks_hosts_and_scope() {
    let rows = table::parse(b"Routing tables\nInternet:\nDestination Gateway Flags Netif Expire\ndefault 192.0.2.1 UGScI en5\n128/1 10.66.0.2 UGSc utun7\n192.168.0 link#5 UCS en5\n8.8.8.8 192.0.2.1 UGHS en5\n", false).unwrap();
    assert_eq!(rows[0].prefix.to_string(), "0.0.0.0/0");
    assert_eq!(rows[1].prefix.to_string(), "128.0.0.0/1");
    assert_eq!(rows[2].prefix.to_string(), "192.168.0.0/24");
    assert_eq!(rows[3].prefix.to_string(), "8.8.8.8/32");
    let rows = table::parse(b"Destination Gateway Flags Netif Expire\ndefault fe80::1%en5 UGcI en5\n8000::/1 link#7 US utun7\nfe80::%en5/64 link#5 UCI en5\n", true).unwrap();
    assert_eq!(rows[1].prefix.to_string(), "8000::/1");
    assert_eq!(rows[2].prefix.to_string(), "fe80::/64");
    let route = ExpectedRoute {
        kind: RouteKind::Capture,
        prefix: "8000::/1".parse().unwrap(),
        interface: "utun7".into(),
        gateway: None,
    };
    assert!(route.present(&rows).unwrap());
}

#[test]
fn malformed_route_tables_fail_closed() {
    for output in [
        "",
        "permission denied",
        "Destination Gateway Flags Netif\ninvalid row",
        "Destination Gateway Flags Netif\n999.0/1 link#5 US en5",
    ] {
        assert!(table::parse(output.as_bytes(), false).is_err());
    }
}

#[test]
fn native_route_table_can_be_read_without_network_mutation() {
    // Exercise the real tool format as well as synthetic failure cases.
    table::read(false).unwrap();
    table::read(true).unwrap();
}
