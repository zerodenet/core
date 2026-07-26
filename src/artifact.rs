use std::fs::File;
use std::io::{self, Read};

use sha2::{Digest, Sha256};

pub fn current_executable_sha256() -> io::Result<String> {
    let path = std::env::current_exe()?;
    sha256_file(path)
}

fn sha256_file(path: impl AsRef<std::path::Path>) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
