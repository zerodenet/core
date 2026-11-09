use super::*;
#[test]
fn inline_name_matches_original_normalization_without_an_owned_cache_string() {
    for name in [
        "Mixed.Example...",
        "",
        "127.0.0.1",
        "[::1]",
        "a..b",
        "-a.example",
        "a-.example",
        "a_foo.example",
        "é.example",
    ] {
        let original = super::super::normalize_sniffed_domain(name.to_owned());
        let fixed = InlineName::new(name);
        assert_eq!(
            fixed.as_ref().map(InlineName::as_str),
            original.as_deref(),
            "{name}"
        );
    }
    let long = "x".repeat(254);
    assert!(InlineName::new(&long).is_none());
}
