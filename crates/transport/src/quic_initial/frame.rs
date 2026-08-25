const MAX_CLIENT_HELLO_LENGTH: usize = 65_535;

#[derive(Default)]
pub(super) struct CryptoReassembler {
    bytes: Vec<u8>,
    present: Vec<bool>,
}

impl CryptoReassembler {
    fn insert(&mut self, offset: usize, data: &[u8]) -> Result<(), ()> {
        let end = offset.checked_add(data.len()).ok_or(())?;
        if end > MAX_CLIENT_HELLO_LENGTH + 4 {
            return Err(());
        }
        if self.bytes.len() < end {
            self.bytes.resize(end, 0);
            self.present.resize(end, false);
        }
        for (index, byte) in data.iter().copied().enumerate() {
            let position = offset + index;
            if self.present[position] && self.bytes[position] != byte {
                return Err(());
            }
            self.bytes[position] = byte;
            self.present[position] = true;
        }
        Ok(())
    }

    pub(super) fn client_hello(&self) -> Result<Option<Vec<u8>>, ()> {
        if self.present.len() < 4 || !self.present[..4].iter().all(|present| *present) {
            return Ok(None);
        }
        if self.bytes[0] != 0x01 {
            return Err(());
        }
        let length = ((self.bytes[1] as usize) << 16)
            | ((self.bytes[2] as usize) << 8)
            | self.bytes[3] as usize;
        if length > MAX_CLIENT_HELLO_LENGTH {
            return Err(());
        }
        let end = 4 + length;
        if self.present.len() < end || !self.present[..end].iter().all(|present| *present) {
            return Ok(None);
        }
        Ok(Some(self.bytes[..end].to_vec()))
    }
}

pub(super) fn collect_crypto_frames(
    plaintext: &[u8],
    crypto: &mut CryptoReassembler,
) -> Result<(), ()> {
    let mut offset = 0;
    while offset < plaintext.len() {
        let frame_type = read_varint(plaintext, &mut offset)?;
        match frame_type {
            0x00 => {}
            0x01 => {}
            0x02 | 0x03 => skip_ack(plaintext, &mut offset, frame_type == 0x03)?,
            0x06 => {
                let crypto_offset =
                    usize::try_from(read_varint(plaintext, &mut offset)?).map_err(|_| ())?;
                let length =
                    usize::try_from(read_varint(plaintext, &mut offset)?).map_err(|_| ())?;
                let end = offset.checked_add(length).ok_or(())?;
                let data = plaintext.get(offset..end).ok_or(())?;
                crypto.insert(crypto_offset, data)?;
                offset = end;
            }
            0x1c => skip_connection_close(plaintext, &mut offset)?,
            _ => return Err(()),
        }
    }
    Ok(())
}

fn skip_ack(bytes: &[u8], offset: &mut usize, ecn: bool) -> Result<(), ()> {
    read_varint(bytes, offset)?;
    read_varint(bytes, offset)?;
    let ranges = read_varint(bytes, offset)?;
    read_varint(bytes, offset)?;
    for _ in 0..ranges {
        read_varint(bytes, offset)?;
        read_varint(bytes, offset)?;
    }
    if ecn {
        read_varint(bytes, offset)?;
        read_varint(bytes, offset)?;
        read_varint(bytes, offset)?;
    }
    Ok(())
}

fn skip_connection_close(bytes: &[u8], offset: &mut usize) -> Result<(), ()> {
    read_varint(bytes, offset)?;
    read_varint(bytes, offset)?;
    let reason_length = usize::try_from(read_varint(bytes, offset)?).map_err(|_| ())?;
    *offset = (*offset).checked_add(reason_length).ok_or(())?;
    if *offset > bytes.len() {
        return Err(());
    }
    Ok(())
}

pub(super) fn read_varint(bytes: &[u8], offset: &mut usize) -> Result<u64, ()> {
    let first = *bytes.get(*offset).ok_or(())?;
    let length = 1_usize << (first >> 6);
    let end = (*offset).checked_add(length).ok_or(())?;
    let encoded = bytes.get(*offset..end).ok_or(())?;
    let mut value = u64::from(first & 0x3f);
    for byte in &encoded[1..] {
        value = (value << 8) | u64::from(*byte);
    }
    *offset = end;
    Ok(value)
}
