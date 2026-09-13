use super::*;
pub(super) async fn write<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    config: &Fragment,
    count: u64,
    input: &[u8],
) -> io::Result<()> {
    let hello = config.packets.minimum == 0 && config.packets.maximum == 1;
    if hello {
        if count != 1 || input.len() <= 5 || input[0] != 22 {
            return writer.write_all(input).await;
        }
        let end = 5 + usize::from(u16::from_be_bytes([input[3], input[4]]));
        if input.len() < end {
            return writer.write_all(input).await;
        }
        let data = &input[5..end];
        let mut records = Vec::new();
        for part in pieces(data, config) {
            let mut record = input[..3].to_vec();
            record.extend((part.len() as u16).to_be_bytes());
            record.extend(part);
            if config.delay_ms.maximum == 0 {
                records.extend(record);
            } else {
                writer.write_all(&record).await?;
                writer.flush().await?;
                tokio::time::sleep(Duration::from_millis(config.delay_ms.sample())).await;
            }
        }
        if !records.is_empty() {
            writer.write_all(&records).await?;
        }
        return writer.write_all(&input[end..]).await;
    }
    if config.packets.minimum != 0
        && (count < config.packets.minimum || count > config.packets.maximum)
    {
        return writer.write_all(input).await;
    }
    for part in pieces(input, config) {
        writer.write_all(part).await?;
        writer.flush().await?;
        if config.delay_ms.maximum != 0 {
            tokio::time::sleep(Duration::from_millis(config.delay_ms.sample())).await;
        }
    }
    Ok(())
}
fn pieces<'a>(mut input: &'a [u8], config: &Fragment) -> Vec<&'a [u8]> {
    let maximum = config.max_splits.sample();
    let mut out = Vec::new();
    let mut count = 0;
    while !input.is_empty() {
        count += 1;
        let length = if maximum > 0 && count >= maximum {
            input.len()
        } else {
            (config.length.sample() as usize).min(input.len())
        };
        let (part, remaining) = input.split_at(length);
        out.push(part);
        input = remaining;
    }
    out
}
