use super::*;
impl Entry {
    pub(super) fn close(self: &Arc<Self>) -> Result<IcmpCloseReceipt, Arc<std_io::Error>> {
        self.cancel.cancel();
        let mut native = self.native();
        match &*native {
            Native::Closed => {
                return Ok(IcmpCloseReceipt {
                    entry: self.clone(),
                })
            }
            Native::Reserved | Native::NoNative => {
                *native = Native::NoNative;
                return Err(Arc::new(std_io::Error::other("no ICMP original acquired")));
            }
            Native::Unknown(source) => {
                if !crate::TokioDatagramSocket::close_is_retryable(source) {
                    return Err(source.clone());
                }
                let source = source.clone();
                *native = Native::Unknown(self.uncertain.clone());
                return match crate::TokioDatagramSocket::retry_close(&source) {
                    Ok(true) => {
                        *native = Native::Closed;
                        Ok(IcmpCloseReceipt {
                            entry: self.clone(),
                        })
                    }
                    Ok(false) => {
                        *native = Native::Unknown(source.clone());
                        Err(source)
                    }
                    Err(error) => {
                        let source = Arc::new(error);
                        self.metadata()
                            .close_source
                            .get_or_insert_with(|| source.clone());
                        *native = Native::Unknown(source.clone());
                        Err(source)
                    }
                };
            }
            _ => {}
        }
        let original = std::mem::replace(&mut *native, Native::Unknown(self.uncertain.clone()));
        let result = match original {
            Native::Raw(socket) => crate::datagram::native_close(socket),
            Native::Live(socket) => socket.into_std().and_then(crate::datagram::native_close),
            _ => unreachable!("same original native gate"),
        };
        match result {
            Ok(()) => {
                *native = Native::Closed;
                Ok(IcmpCloseReceipt {
                    entry: self.clone(),
                })
            }
            Err(error) => {
                let source = Arc::new(error);
                self.metadata()
                    .close_source
                    .get_or_insert_with(|| source.clone());
                *native = Native::Unknown(source.clone());
                Err(source)
            }
        }
    }
}
