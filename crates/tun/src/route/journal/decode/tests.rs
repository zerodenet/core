use super::*;
use crate::packet_memory::{PacketMetadataLayout, PacketMetadataVec, PacketReturnTotals};
use std::sync::Arc;
const DOC:&[u8]=br#"{"tun_name":"utun-fixture","ipv6":false,"tun_index":7,"egress":{"name":"physical0","index":9},"gateway":"192.0.2.1","excluded":["192.0.2.4"],"installed":["0.0.0.0/1","128.0.0.0/1"]}"#;
fn actual_layout(journal: &RouteJournal) -> PacketMetadataLayout {
    let mut layout = PacketMetadataLayout::new(0, 0);
    for value in std::iter::once(&journal.tun_name)
        .chain(std::iter::once(&journal.egress.name))
        .chain(journal.gateway.iter())
        .chain(&journal.installed)
    {
        layout = layout
            .checked_add(PacketMetadataLayout::new(
                usize::from(value.capacity() != 0),
                value.capacity(),
            ))
            .unwrap();
    }
    for value in [
        PacketMetadataLayout::array::<IpAddr>(journal.excluded.capacity()).unwrap(),
        PacketMetadataLayout::array::<String>(journal.installed.capacity()).unwrap(),
    ] {
        layout = layout.checked_add(value).unwrap();
    }
    if journal.path.capacity() != 0 {
        layout = layout
            .checked_add(PacketMetadataLayout::new(1, journal.path.capacity()))
            .unwrap();
    }
    layout
}
fn original_wire(journal: &RouteJournal) -> Vec<u8> {
    #[derive(serde::Serialize)]
    struct Original<'a> {
        tun_name: &'a str,
        ipv6: bool,
        tun_index: u32,
        egress: &'a RouteInterface,
        gateway: &'a Option<String>,
        excluded: &'a Vec<IpAddr>,
        installed: &'a Vec<String>,
        scoped_bypass: bool,
    }
    serde_json::to_vec(&Original {
        tun_name: &journal.tun_name,
        ipv6: journal.ipv6,
        tun_index: journal.tun_index,
        egress: &journal.egress,
        gateway: &journal.gateway,
        excluded: &journal.excluded,
        installed: &journal.installed,
        scoped_bypass: journal.scoped_bypass,
    })
    .unwrap()
}
#[test]
fn actual_parsed_fields_and_arrays_stay_paid_after_decoder_and_scratch_are_destroyed() {
    let root = Arc::new(PacketReturnTotals::default());
    let baseline = root.driver_metadata().occupied;
    for bytes in [DOC,&br#"{"tun_name":"utun\u002dquote\"\u03bb","ipv6":false,"tun_index":7,"egress":{"name":"phy\\name","index":9},"gateway":null,"excluded":[],"installed":["a","b","c"]}"#[..]] {
        let legacy:RouteJournal=serde_json::from_slice(bytes).unwrap();
        let paid=decode(bytes,root.driver_metadata_view()).unwrap();
        assert_eq!(serde_json::to_vec(&paid).unwrap(),original_wire(&legacy));
        assert_eq!(root.driver_metadata().occupied,baseline.checked_add(actual_layout(&paid)).unwrap());
        drop(paid);
        assert_eq!(root.driver_metadata().occupied,baseline);
    }
}
#[test]
fn parsed_schema_errors_preserve_original_acceptance_and_return_all_partial_owners() {
    let root = Arc::new(PacketReturnTotals::default());
    let baseline = root.driver_metadata().occupied;
    let cases:[&[u8];6]=[
        br#"{"tun_name":"first","tun_name":"duplicate"}"#,
        br#"{"tun_name":"first","ipv6":false,"tun_index":7,"egress":{"name":"partial","index":"bad"}}"#,
        br#"{"tun_name":"bad\uD800"}"#,
        br#"["utun",false,1,{"name":"physical0","index":2},null,[],[],false]"#,
        br#"{"tun_name":"utun","ipv6":false,"tun_index":7,"egress":{"name":"physical0","index":2},"excluded":[],"installed":[],"ignored":{"nested":["unknown\u0020value"]}}"#,
        br#"{"tun_name":"utun","ipv6":false,"tun_index":7,"egress":{"name":"physical0","index":2},"excluded":[],"installed":[]} trailing"#,
    ];
    for bytes in cases {
        let legacy = serde_json::from_slice::<RouteJournal>(bytes);
        let paid = decode(bytes, root.driver_metadata_view());
        assert_eq!(paid.is_ok(), legacy.is_ok());
        if let (Ok(paid), Ok(legacy)) = (&paid, &legacy) {
            assert_eq!(
                serde_json::to_vec(paid).unwrap(),
                serde_json::to_vec(legacy).unwrap()
            );
        }
        drop(paid);
        assert_eq!(root.driver_metadata().occupied, baseline);
    }
}
#[test]
fn real_same_root_row_pressure_preserves_original_then_reuses_after_true_foreign_array_release() {
    let root = Arc::new(PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let mut journal = decode(DOC, memory.clone()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    journal.path = journal
        ._metadata
        .path(&directory.path().join("journal.json"))
        .unwrap();
    let mut foreign: [PacketMetadataVec<u8>; 3] = std::array::from_fn(|_| Default::default());
    for array in &mut foreign {
        for _ in 0..1024 {
            array.try_push_with_metadata(19, &memory, 1024).unwrap();
        }
    }
    let occupied = root.driver_metadata().occupied;
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        occupied,
    )
    .unwrap();
    let old = serde_json::to_vec(&journal).unwrap();
    let error = journal.record_route("10.0.0.0/8").unwrap_err();
    assert!(error
        .get_ref()
        .is_some_and(|source| source.is::<PacketMemoryError>()));
    assert_eq!(serde_json::to_vec(&journal).unwrap(), old);
    assert_eq!(root.driver_metadata().occupied, occupied);
    drop(foreign);
    journal.record_route("10.0.0.0/8").unwrap();
    assert_eq!(journal.installed.last().unwrap(), "10.0.0.0/8");
    let file = std::fs::read(&journal.path).unwrap();
    assert_eq!(file, serde_json::to_vec(&journal).unwrap());
}
#[test]
fn scratch_plan_ignores_whitespace_and_does_not_reserve_for_borrowed_unescaped_tokens() {
    assert!(
        scratch::layout(b"  \n\t {\"plain\":\"a long ordinary value\"}")
            .unwrap()
            .is_none()
    );
    let small = scratch::layout(br#""\n""#).unwrap().unwrap();
    assert_eq!(small.allocations(), 1);
    assert_eq!(small.bytes(), 8);
    let escaped = scratch::layout(br#"{"long\u0020name":"abcdefghijklmnopqrstuvwxyz\n"}"#)
        .unwrap()
        .unwrap();
    let padded = scratch::layout(
        br#"                  {"long\u0020name":"abcdefghijklmnopqrstuvwxyz\n"}            "#,
    )
    .unwrap()
    .unwrap();
    assert_eq!(escaped, padded);
}

mod cleanup;

#[test]
fn admitted_new_string_is_released_if_its_actual_table_growth_cannot_fit() {
    let root = Arc::new(PacketReturnTotals::default());
    let mut journal = decode(DOC, root.driver_metadata_view()).unwrap();
    let occupied = root.driver_metadata().occupied;
    let limit = occupied
        .checked_add(PacketMetadataLayout::new(1, 3))
        .unwrap();
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        limit,
    )
    .unwrap();
    let before = serde_json::to_vec(&journal).unwrap();
    let error = journal.record_route("new").unwrap_err();
    assert!(error
        .get_ref()
        .is_some_and(|source| source.is::<PacketMemoryError>()));
    assert_eq!(root.driver_metadata().occupied, occupied);
    assert_eq!(serde_json::to_vec(&journal).unwrap(), before);
}
