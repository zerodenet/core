use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn responder() -> (TcpStream, thread::JoinHandle<std::io::Result<usize>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut accepted, _) = listener.accept().unwrap();
    // Reproduce Windows accept inheritance on every platform.
    accepted.set_nonblocking(true).unwrap();
    let (started, ready) = mpsc::channel();
    let worker = thread::spawn(move || {
        started.send(()).unwrap();
        super::respond(&mut accepted)
    });
    ready.recv_timeout(Duration::from_secs(2)).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    (client, worker)
}

fn assert_waiting(client: &mut TcpStream) {
    client
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let result = client.read(&mut [0; 7]);
    assert!(
        result.is_err_and(|error| matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )),
        "responder replied or closed before receiving a complete request"
    );
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
}

#[test]
fn waits_for_delayed_and_fragmented_tls_record_on_nonblocking_accept() {
    let (mut client, worker) = responder();
    assert_waiting(&mut client);
    client.write_all(&[22, 3]).unwrap();
    assert_waiting(&mut client);
    client.write_all(&[3, 0, 4, 1, 2]).unwrap();
    assert_waiting(&mut client);
    client.write_all(&[3, 4]).unwrap();
    let mut response = [0; 7];
    client.read_exact(&mut response).unwrap();
    assert_eq!(response, super::TLS_ALERT);
    assert_eq!(worker.join().unwrap().unwrap(), 9);
    assert_eq!(client.read(&mut [0; 1]).unwrap(), 0);
}

#[test]
fn incomplete_tls_record_is_an_error_without_a_success_alert() {
    let (mut client, worker) = responder();
    client.write_all(&[22, 3, 3, 0, 4, 1]).unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    assert_eq!(
        worker.join().unwrap().unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert_eq!(client.read(&mut [0; 7]).unwrap(), 0);
}
