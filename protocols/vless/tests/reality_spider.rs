#![cfg(any(feature = "validation", feature = "runtime"))]
use vless::reality_spider::Profile;
#[test]
fn spider_ranges_are_removed_from_path_and_resource_bounds_are_validated() {
    let profile = Profile::parse("/entry?p=10-20&c=2&t=3&i=1-5&r=20&keep=yes").unwrap();
    assert_eq!(profile.path, "/entry?keep=yes");
    assert_eq!(profile.ranges, [(10, 20), (2, 2), (3, 3), (1, 5), (20, 20)]);
    assert_eq!(Profile::parse("").unwrap().path, "/");
    for value in [
        "bad",
        "//bad",
        "/?c=10000",
        "/?i=3-1",
        "/?r=-1",
        "/?t=999999",
        "/?c=64&t=256",
    ] {
        assert!(Profile::parse(value).is_err(), "{value}");
    }
}
