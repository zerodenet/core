#[path = "http_client.rs"]
mod client;
#[path = "http_peer.rs"]
mod peer;

#[test]
fn controlled_http_peer_verifies_complete_split_and_early_close_connections() {
    let mut peer = peer::Peer::start("127.0.0.1:0".parse().unwrap());
    client::run_suite("local-peer", || std::net::TcpStream::connect(peer.address));
    peer.stop();
    peer.assert_observations(1);
}

#[test]
fn complete_reader_rejects_truncation_corruption_and_extra_data() {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    for fault in ["truncated", "corrupt", "extra"] {
        let case = format!("reader/{fault}/single");
        let mut response = peer::response(&case);
        match fault {
            "truncated" => {
                response.pop();
            }
            "corrupt" => {
                *response.last_mut().unwrap() ^= 1;
            }
            _ => response.push(0),
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut accepted, _) = listener.accept().unwrap();
        accepted
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        accepted
            .set_write_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let worker = std::thread::spawn(move || {
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                assert!(request.len() < 4096);
                let mut byte = [0];
                accepted.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            accepted.write_all(&response).unwrap();
        });
        let error = client::exchange(stream, &case, false).unwrap_err();
        assert!(
            error.to_string().contains("response mismatch"),
            "{fault}: {error}"
        );
        worker.join().unwrap();
    }
}

// Type-check the portable harness on every host; only Windows registers
// the privileged test entrypoint.
#[cfg_attr(not(windows), allow(dead_code))]
#[path = "http_windows.rs"]
mod windows;
