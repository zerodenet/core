use super::*;
pub(in crate::route) enum Gateway<'a> {
    Text(&'a str),
    #[cfg(any(windows, test))]
    Address(IpAddr),
}
impl Gateway<'_> {
    pub(super) fn copy(self, owner: &mut metadata::Owner) -> io::Result<String> {
        match self {
            Self::Text(value) => owner.string(value).map_err(io::Error::other),
            #[cfg(any(windows, test))]
            Self::Address(address) => {
                struct Text {
                    bytes: [u8; 45],
                    length: usize,
                }
                impl std::fmt::Write for Text {
                    fn write_str(&mut self, value: &str) -> std::fmt::Result {
                        let end = self
                            .length
                            .checked_add(value.len())
                            .ok_or(std::fmt::Error)?;
                        if end > self.bytes.len() {
                            return Err(std::fmt::Error);
                        }
                        self.bytes[self.length..end].copy_from_slice(value.as_bytes());
                        self.length = end;
                        Ok(())
                    }
                }
                let mut value = Text {
                    bytes: [0; 45],
                    length: 0,
                };
                std::fmt::write(&mut value, format_args!("{address}")).map_err(io::Error::other)?;
                let text =
                    std::str::from_utf8(&value.bytes[..value.length]).map_err(io::Error::other)?;
                Self::Text(text).copy(owner)
            }
        }
    }
}
