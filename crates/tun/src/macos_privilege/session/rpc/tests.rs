use super::*;
use std::{
    io::{Read, Write},
    os::fd::AsRawFd,
};

#[test]
fn retained_rpc_resumes_partial_reply_without_resending_the_original_request() {
    let (parent, mut helper) = UnixStream::pair().unwrap();
    parent.set_nonblocking(true).unwrap();
    let mut rpc = Rpc::new();
    let mut tools = tool::Supervisor::new();
    rpc.begin(&HelperRequest::Hello { nonce: 91 }, None)
        .unwrap();
    assert!(!rpc.progress(parent.as_raw_fd(), &mut tools).unwrap());
    assert!(rpc.submitted());
    let mut request = Vec::new();
    loop {
        let mut byte = [0];
        helper.read_exact(&mut byte).unwrap();
        request.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
    }
    let parsed: HelperRequest = serde_json::from_slice(&request).unwrap();
    assert!(matches!(parsed, HelperRequest::Hello { nonce: 91 }));
    helper.write_all(b"{\"success\":true,\"code\":0,").unwrap();
    assert!(!rpc.progress(parent.as_raw_fd(), &mut tools).unwrap());
    assert!(rpc.begin(&HelperRequest::Shutdown, None).is_err());
    helper.set_nonblocking(true).unwrap();
    assert_eq!(
        helper.read(&mut [0]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    helper.write_all(b"\"stdout\":[],\"stderr\":[]}\n").unwrap();
    assert!(rpc.progress(parent.as_raw_fd(), &mut tools).unwrap());
    let response = rpc.take_response().unwrap();
    assert!(response.success);
    assert!(!rpc.pending());
    rpc.begin(&HelperRequest::Shutdown, None).unwrap();
    rpc.write_request(parent.as_raw_fd()).unwrap();
    assert!(rpc.submitted());
}

#[test]
fn actual_owned_rpc_receives_a_large_reply_on_same_root_paid_storage_and_returns_exact_capacity() {
    let root = std::sync::Arc::new(crate::packet_memory::PacketReturnTotals::default());
    let grant = root.native_grant(1).unwrap();
    let memory = grant.memory_view().unwrap();
    let baseline = root.driver_metadata().occupied;
    let credit = memory
        .reserve_driver_metadata(Request::layout(CAPACITY))
        .unwrap();
    let mut rpc = Rpc::from_prepaid(CAPACITY, credit).unwrap();
    rpc.bind_metadata(memory, 20_000).unwrap();
    let (parent, mut helper) = UnixStream::pair().unwrap();
    parent.set_nonblocking(true).unwrap();
    let peer = std::thread::spawn(move || {
        loop {
            let mut byte = [0u8];
            helper.read_exact(&mut byte).unwrap();
            if byte[0] == b'\n' {
                break;
            }
        }
        write_helper_response(
            &mut helper,
            &HelperResponse {
                success: true,
                code: 0,
                stdout: vec![255; 18_000],
                stderr: Vec::new(),
            },
        )
        .unwrap();
    });
    rpc.begin(&HelperRequest::Hello { nonce: 92 }, None)
        .unwrap();
    let mut tools = tool::Supervisor::new();
    loop {
        if rpc.progress(parent.as_raw_fd(), &mut tools).unwrap() {
            break;
        }
        let mut fd = libc::pollfd {
            fd: parent.as_raw_fd(),
            events: libc::POLLIN | libc::POLLOUT,
            revents: 0,
        };
        assert!(unsafe { libc::poll(&mut fd, 1, 1000) } >= 0);
    }
    let response = rpc.take_response().unwrap();
    peer.join().unwrap();
    assert_eq!(response.stdout, vec![255; 18_000]);
    assert!(
        rpc.read > CAPACITY,
        "same original reply really crossed the old fixed workspace"
    );
    assert!(root.driver_metadata().occupied.bytes() > baseline.bytes() + CAPACITY);
    drop(response);
    drop(rpc);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
