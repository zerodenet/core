//! Cleanup discards bytes on the same fixed scratch, never a fresh output Vec.
use super::*;
impl ChildOriginal {
    pub(super) fn drain(&mut self) -> io::Result<()> {
        if let Some(stdin) = self.pipes[0].as_mut().filter(|original| original.usable()) {
            stdin.close_once()?;
        }
        for original in self.pipes[1..]
            .iter()
            .flatten()
            .filter(|original| original.usable())
        {
            let flags = unsafe { libc::fcntl(original.fd(), libc::F_GETFL) };
            if flags < 0
                || unsafe { libc::fcntl(original.fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                    < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        loop {
            let mut descriptors = [libc::pollfd {
                fd: -1,
                events: libc::POLLIN,
                revents: 0,
            }; 2];
            for (slot, original) in descriptors.iter_mut().zip(&self.pipes[1..]) {
                if let Some(original) = original.as_ref().filter(|original| original.usable()) {
                    slot.fd = original.fd();
                }
            }
            if descriptors.iter().all(|fd| fd.fd < 0) {
                return Ok(());
            }
            if unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) } < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(source);
            }
            for (index, descriptor) in descriptors.iter().enumerate() {
                if descriptor.fd < 0 || descriptor.revents == 0 {
                    continue;
                }
                let mut scratch = [0u8; 4096];
                let count = unsafe {
                    libc::read(descriptor.fd, scratch.as_mut_ptr().cast(), scratch.len())
                };
                if count == 0 {
                    self.pipes[index + 1].as_mut().unwrap().close_once()?;
                } else if count < 0 {
                    let source = io::Error::last_os_error();
                    if !matches!(
                        source.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) {
                        return Err(source);
                    }
                }
            }
        }
    }
}
