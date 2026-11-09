use super::*;

#[test]
fn borrowed_dynamic_and_fixed_argv_preserve_exact_command_values_and_order() {
    let dynamic = [
        "-n".to_owned(),
        "get".to_owned(),
        "address with quote\" λ".to_owned(),
    ];
    let mut command = Command::new("/sbin/route");
    Arguments::Strings(&dynamic).append_to(&mut command);
    assert!(command
        .get_args()
        .eq(dynamic.iter().map(std::ffi::OsStr::new)));
    for family in ["inet", "inet6"] {
        let expected = ["-rnW", "-f", family];
        let mut command = Command::new("/usr/sbin/netstat");
        Arguments::Slices(&expected).append_to(&mut command);
        assert!(command
            .get_args()
            .eq(expected.iter().map(std::ffi::OsStr::new)));
    }
    let mut command = Command::new("/usr/sbin/scutil");
    Arguments::Slices(&["--dns"]).append_to(&mut command);
    assert!(command.get_args().eq([std::ffi::OsStr::new("--dns")]));
}
