use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;

use std::io::{BufRead, BufReader};
use std::thread;

use super::{
    blocking_helper_stream, receive_fd, request_helper_utun, send_fd, HelperRequest,
    HelperResponse, PRIVILEGED_COMMAND_SCRIPT,
};

#[test]
fn privileged_script_shell_quotes_every_external_value() {
    assert!(PRIVILEGED_COMMAND_SCRIPT.contains("quoted form of"));
    assert!(PRIVILEGED_COMMAND_SCRIPT.contains("with administrator privileges"));
}

#[test]
fn transfers_a_file_descriptor_over_the_private_helper_socket() {
    let (sender, receiver) = UnixStream::pair().expect("create descriptor transport");
    let file = File::open("/dev/null").expect("open descriptor fixture");
    send_fd(sender.as_raw_fd(), file.as_raw_fd()).expect("send descriptor");
    let received = receive_fd(receiver.as_raw_fd()).expect("receive descriptor");
    assert!(received >= 0);
    unsafe { libc::close(received) };
}

#[test]
fn helper_control_stream_is_restored_to_blocking_io() {
    let (stream, _peer) = UnixStream::pair().expect("create helper control stream");
    stream
        .set_nonblocking(true)
        .expect("make helper control stream non-blocking");

    let stream = blocking_helper_stream(stream).expect("restore blocking helper stream");
    let flags = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_GETFL) };

    assert!(flags >= 0);
    assert_eq!(flags & libc::O_NONBLOCK, 0);
}

#[test]
fn one_helper_channel_serves_repeated_utun_requests() {
    let (mut parent, helper) = UnixStream::pair().expect("create helper channel");
    let server = thread::spawn(move || {
        let mut helper = helper;
        for expected in ["utun8", "utun9"] {
            let mut line = String::new();
            BufReader::new(&mut helper)
                .read_line(&mut line)
                .expect("read create request");
            let request: HelperRequest = serde_json::from_str(&line).expect("parse request");
            assert!(matches!(
                request,
                HelperRequest::CreateUtun { name: Some(name) } if name == expected
            ));
            super::write_helper_response(
                &mut helper,
                &HelperResponse {
                    success: true,
                    code: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                },
            )
            .expect("write create response");
            let file = File::open("/dev/null").expect("open descriptor fixture");
            send_fd(helper.as_raw_fd(), file.as_raw_fd()).expect("send descriptor");
        }
    });

    for name in ["utun8", "utun9"] {
        let fd = request_helper_utun(&mut parent, Some(name)).expect("request descriptor");
        unsafe { libc::close(fd) };
    }
    server.join().expect("helper server");
}

#[test]
fn helper_exits_when_parent_closes_control_channel() {
    let (parent, mut helper) = UnixStream::pair().expect("create helper channel");
    let server = thread::spawn(move || super::serve_helper(&mut helper));
    drop(parent);
    server.join().expect("helper thread").expect("parent EOF");
}
