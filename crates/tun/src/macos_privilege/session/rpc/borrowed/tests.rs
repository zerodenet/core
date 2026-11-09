use super::*;
use crate::macos_privilege::{
    session::rpc::{Request, Rpc, CAPACITY},
    HelperRequest,
};
use std::{io::Read, os::fd::AsRawFd, os::unix::net::UnixStream};

#[test]
fn borrowed_source_inputs_preserve_exact_existing_owned_wire_including_escaping() {
    let arguments = [
        "-n".to_owned(),
        "quote\" slash\\ newline\n unicode λ".to_owned(),
    ];
    let large = "{\"pf\":\"\\\"}\nλ".repeat(9000);
    for program in [
        HelperProgram::Ifconfig,
        HelperProgram::Route,
        HelperProgram::Pfctl,
    ] {
        for input in [None, Some(""), Some(large.as_str())] {
            let original = HelperRequest::OwnedCommand {
                program,
                arguments: arguments.to_vec(),
                input: input.map(str::to_owned),
                nonce: 991,
                output_limit: 65_536,
            };
            let borrowed = BorrowedRequest::OwnedCommand {
                program,
                arguments: &arguments,
                input,
                nonce: 991,
                output_limit: 65_536,
            };
            assert_eq!(
                serde_json::to_vec(&borrowed).unwrap(),
                serde_json::to_vec(&original).unwrap()
            );
        }
    }
    for name in [None, Some("utun27"), Some("quote\"λ")] {
        let original = HelperRequest::OwnedCreateUtun {
            name: name.map(str::to_owned),
            transfer: 891,
        };
        let borrowed = BorrowedRequest::OwnedCreateUtun {
            name,
            transfer: 891,
        };
        assert_eq!(
            serde_json::to_vec(&borrowed).unwrap(),
            serde_json::to_vec(&original).unwrap()
        );
    }
}

#[test]
fn actual_paid_rpc_owns_submitted_input_after_borrowed_caller_data_is_destroyed() {
    let root = std::sync::Arc::new(crate::packet_memory::PacketReturnTotals::default());
    let grant = root.native_grant(1).unwrap();
    let memory = grant.memory_view().unwrap();
    let baseline = root.driver_metadata().occupied;
    let credit = memory
        .reserve_driver_metadata(Request::layout(CAPACITY))
        .unwrap();
    let mut rpc = Rpc::from_prepaid(CAPACITY, credit).unwrap();
    {
        let arguments = ["-f".to_owned(), "-".to_owned()];
        let input = "actual rule with quote\" and λ\n".to_owned();
        rpc.begin(
            &BorrowedRequest::OwnedCommand {
                program: HelperProgram::Pfctl,
                arguments: &arguments,
                input: Some(&input),
                nonce: 418,
                output_limit: CAPACITY,
            },
            None,
        )
        .unwrap();
    }
    // No command/Child is started: this exercises the real paid byte continuation.
    assert!(rpc.pending());
    assert!(rpc.begin(&HelperRequest::Shutdown, None).is_err());
    let (parent, mut helper) = UnixStream::pair().unwrap();
    rpc.write_request(parent.as_raw_fd()).unwrap();
    assert!(rpc.submitted());
    let mut bytes = vec![0; rpc.length];
    helper.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes.pop(), Some(b'\n'));
    let parsed: HelperRequest = serde_json::from_slice(&bytes).unwrap();
    match parsed {
        HelperRequest::OwnedCommand {
            program: HelperProgram::Pfctl,
            arguments,
            input,
            nonce: 418,
            output_limit: CAPACITY,
        } => {
            assert_eq!(arguments, ["-f", "-"]);
            assert_eq!(input.as_deref(), Some("actual rule with quote\" and λ\n"));
        }
        _ => panic!("same original paid bytes changed request identity"),
    }
    assert_eq!(
        root.driver_metadata().occupied.bytes(),
        baseline.bytes() + CAPACITY
    );
    drop(rpc);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
