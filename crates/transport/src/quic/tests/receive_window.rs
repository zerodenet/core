use super::*;
#[path = "receive_window_reference.rs"]
mod reference;

#[test]
fn rapid_consumption_doubles_windows_within_the_ceiling() {
    let now = Instant::now();
    let mut window = Window::new(16_384, 65_536);
    window.received(now);
    window.read = 4095;
    assert!(!window.pending());
    window.read = 4096;
    assert_eq!(window.update(now, Duration::from_millis(100)), Some(20_480));
    assert_eq!(window.size, 16_384);
    window.read = 12_288;
    assert_eq!(
        window.update(now + Duration::from_millis(10), Duration::from_millis(100)),
        Some(45_056)
    );
    assert_eq!(window.size, 32_768);
    window.read = 40_000;
    window.update(now + Duration::from_millis(20), Duration::from_millis(100));
    assert_eq!(window.size, 65_536);
    window.read = 100_000;
    window.update(now + Duration::from_millis(30), Duration::from_millis(100));
    assert_eq!(window.size, 65_536);
}

#[test]
fn slow_reader_and_missing_rtt_do_not_expand_the_window() {
    for rtt in [Duration::ZERO, Duration::from_millis(100)] {
        let now = Instant::now();
        let mut window = Window::new(16_384, 65_536);
        window.received(now);
        window.read = 12_288;
        window.update(now + Duration::from_secs(5), rtt);
        assert_eq!(window.size, 16_384);
    }
}

#[test]
fn stream_growth_coordinates_connection_without_cross_stream_growth() {
    let now = Instant::now();
    let id = StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0);
    let other = StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 1);
    let mut controller = Controller {
        stream_initial: 16_384,
        stream_max: 65_536,
        streams: BTreeMap::new(),
        connection: Window::new(16_384, 40_000),
    };
    controller.received(id, now);
    controller.received(other, now);
    assert!(controller.read_stream(id, 12_288));
    controller.read_connection(12_288);
    controller.stream_update(
        id,
        now + Duration::from_millis(10),
        Duration::from_millis(100),
    );
    assert_eq!(controller.streams[&id].size, 32_768);
    assert_eq!(controller.streams[&other].size, 16_384);
    assert_eq!(controller.connection.size, 40_000);
    controller.closed(id);
    assert_eq!(controller.streams.len(), 1);
    controller.set_connection_window(20_000, 50_000);
    assert_eq!(controller.connection.maximum, 20_000);
}
