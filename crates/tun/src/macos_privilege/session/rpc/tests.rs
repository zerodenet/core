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
