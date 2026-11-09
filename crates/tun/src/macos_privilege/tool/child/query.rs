//! Independent test queries keep the same real Child and pipe retirement.
use super::*;
pub(in crate::macos_privilege) fn run_query(
    slot: &mut ChildOriginal,
    query: super::super::Query<'_>,
    output_limit: usize,
) -> io::Result<Output> {
    match query {
        super::super::Query::Route(arguments) => {
            run_fixed(slot, "/sbin/route", arguments, None, None, output_limit)
        }
        super::super::Query::RouteTable(ipv6) => run_query_arguments(
            slot,
            "/usr/sbin/netstat",
            &["-rnW", "-f", if ipv6 { "inet6" } else { "inet" }],
            output_limit,
        ),
        super::super::Query::SystemDns => {
            run_query_arguments(slot, "/usr/sbin/scutil", &["--dns"], output_limit)
        }
    }
}
fn run_query_arguments(
    slot: &mut ChildOriginal,
    executable: &'static str,
    arguments: &[&str],
    output_limit: usize,
) -> io::Result<Output> {
    let ends = start_fixed(
        slot,
        executable,
        Arguments::Slices(arguments),
        None,
        None,
        None,
    )?;
    let output = slot.collect(b"", output_limit);
    let close = slot.close_pipes();
    let waited = slot.wait();
    failures::finish(ends, output, close, waited)
}
