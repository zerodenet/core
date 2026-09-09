use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use super::peer;

pub(super) fn run_suite(label: &str, mut connect: impl FnMut() -> io::Result<TcpStream>) {
    for round in 0..16 {
        for mode in ["single", "split", "early", "after-early"] {
            let case = format!("{label}/{round}/{mode}");
            let result = connect().and_then(|stream| exchange(stream, &case, mode == "early"));
            assert!(result.is_ok(), "HTTP control case={case}: {result:?}");
        }
    }
    eprintln!("HTTP control suite={label}: 48 complete responses verified, 16 deliberate early closes; no retries");
}

pub(super) fn exchange(mut stream: TcpStream, case: &str, early: bool) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let source = stream.local_addr()?;
    let target = stream.peer_addr()?;
    eprintln!("HTTP client case={case} source={source} target={target} start");
    let request = format!(
        "GET /{case} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        peer::HOST
    );
    if case.ends_with("/split") {
        stream.write_all(&request.as_bytes()[..7])?;
        std::thread::sleep(Duration::from_millis(2));
        stream.write_all(&request.as_bytes()[7..])?;
    } else {
        stream.write_all(request.as_bytes())?;
    }
    let expected = peer::response(case);
    if early {
        let mut prefix = [0; 32];
        stream.read_exact(&mut prefix)?;
        if prefix != expected[..32] {
            return Err(io::Error::other("early response prefix mismatch"));
        }
        eprintln!("HTTP client case={case} source={source} closing after 32 bytes");
        return Ok(());
    }
    let mut received = Vec::new();
    // Bound accumulation and require the actual EOF, not just one read or
    // a matching prefix. Extra bytes, truncation and resets all fail.
    Read::by_ref(&mut stream)
        .take(expected.len() as u64 + 1)
        .read_to_end(&mut received)?;
    if received != expected {
        return Err(io::Error::other(format!(
            "response mismatch case={case}: received={} expected={}",
            received.len(),
            expected.len()
        )));
    }
    eprintln!(
        "HTTP client case={case} source={source} complete_bytes={}",
        received.len()
    );
    Ok(())
}
