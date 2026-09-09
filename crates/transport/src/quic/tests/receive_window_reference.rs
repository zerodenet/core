use super::*;

#[test]
fn receive_window_state_matches_pinned_quic_go_reference() {
    let now = Instant::now();
    let mut name = "";
    let mut window = Window::new(1, 1);
    let mut expected = include_str!("receive_window_reference/expected.csv")
        .lines()
        .skip(1);
    for (index, line) in include_str!("receive_window_reference/events.csv")
        .lines()
        .skip(1)
        .enumerate()
    {
        let fields = line.split(',').collect::<Vec<_>>();
        let n = fields[1..]
            .iter()
            .map(|s| s.parse::<u64>().unwrap())
            .collect::<Vec<_>>();
        if name != fields[0] {
            name = fields[0];
            window = Window::new(n[3], n[4]);
            window.received(now);
        }
        window.read += n[1];
        let update = window
            .update(now + Duration::from_nanos(n[0]), Duration::from_nanos(n[2]))
            .unwrap_or(0);
        let actual = format!(
            "{name},{index},{},{},{update},{}",
            window.size, window.limit, window.epoch_read
        );
        assert_eq!(actual, expected.next().unwrap(), "reference event {index}");
    }
    assert!(expected.next().is_none());
}
