//! Config projection only. Transport owns masks, validation and socket wrapping.
pub(super) fn udp(
    config: &zero_config::FinalMaskConfig,
) -> Vec<zero_transport::finalmask::udp::Mask> {
    use zero_config::UdpMaskConfig as C;
    use zero_transport::finalmask::udp::Mask as M;
    config
        .udp
        .iter()
        .map(|mask| match mask {
            C::Xicmp { ip, id } => M::Xicmp {
                ip: ip.clone(),
                id: *id,
            },
            C::Xdns { domain } => M::Xdns {
                domain: domain.clone(),
            },
            C::Noise {
                reset_seconds,
                items,
            } => M::Noise(zero_transport::finalmask::noise::Settings {
                reset_seconds: range(reset_seconds),
                items: items
                    .iter()
                    .map(|item| zero_transport::finalmask::noise::Item {
                        packet: item.packet.clone(),
                        random_length: range(&item.random_length),
                        minimum_byte: item.minimum_byte,
                        maximum_byte: item.maximum_byte,
                        delay_ms: range(&item.delay_ms),
                    })
                    .collect(),
            }),
            C::Sudoku {
                password,
                ascii,
                custom_tables,
                padding_min,
                padding_max,
            } => M::Sudoku(zero_transport::finalmask::sudoku::Settings {
                password: password.clone(),
                ascii: ascii.clone(),
                custom_tables: custom_tables.clone(),
                padding_min: *padding_min,
                padding_max: *padding_max,
            }),
            C::HeaderDns { domain } => M::Dns {
                domain: domain.clone(),
            },
            C::HeaderDtls => M::Dtls,
            C::HeaderSrtp => M::Srtp,
            C::HeaderUtp => M::Utp,
            C::HeaderWechat => M::Wechat,
            C::HeaderWireguard => M::Wireguard,
            C::MkcpOriginal => M::MkcpOriginal,
            C::MkcpAes128Gcm { password } => M::MkcpAes128Gcm {
                password: password.clone(),
            },
            C::Salamander { password } => M::Salamander {
                password: password.clone(),
            },
            C::HeaderCustom { client, server } => M::Custom {
                client: client.iter().map(item).collect(),
                server: server.iter().map(item).collect(),
            },
        })
        .collect()
}
fn item(item: &zero_config::MaskItemConfig) -> zero_transport::finalmask::udp::Item {
    use zero_config::MaskItemConfig as C;
    use zero_transport::finalmask::udp::Item as I;
    match item {
        C::Bytes { bytes } => I::Bytes(bytes.clone()),
        C::Random {
            length,
            minimum,
            maximum,
        } => I::Random {
            length: *length,
            minimum: *minimum,
            maximum: *maximum,
        },
    }
}

pub(super) fn options(
    config: &zero_config::FinalMaskConfig,
) -> zero_transport::finalmask::Settings {
    zero_transport::finalmask::Settings {
        udp: udp(config),
        tcp: config.tcp.iter().map(tcp).collect(),
    }
}
fn range(value: &zero_config::MaskRangeConfig) -> zero_transport::finalmask::tcp::Range {
    zero_transport::finalmask::tcp::Range {
        minimum: value.minimum,
        maximum: value.maximum,
    }
}
fn sequences(
    values: &[Vec<zero_config::TcpMaskItemConfig>],
) -> Vec<Vec<zero_transport::finalmask::tcp::Item>> {
    values
        .iter()
        .map(|seq| {
            seq.iter()
                .map(|v| zero_transport::finalmask::tcp::Item {
                    delay_ms: range(&v.delay_ms),
                    content: item(&v.content),
                })
                .collect()
        })
        .collect()
}
fn tcp(config: &zero_config::TcpMaskConfig) -> zero_transport::finalmask::tcp::Mask {
    use zero_config::TcpMaskConfig as C;
    use zero_transport::finalmask::tcp as t;
    match config {
        C::HeaderCustom {
            clients,
            servers,
            errors,
        } => t::Mask::Custom(t::Custom {
            clients: sequences(clients),
            servers: sequences(servers),
            errors: sequences(errors),
        }),
        C::Fragment {
            packets,
            length,
            delay_ms,
            max_splits,
        } => t::Mask::Fragment(t::Fragment {
            packets: range(packets),
            length: range(length),
            delay_ms: range(delay_ms),
            max_splits: range(max_splits),
        }),
        C::Sudoku {
            password,
            ascii,
            custom_tables,
            padding_min,
            padding_max,
        } => t::Mask::Sudoku(zero_transport::finalmask::sudoku::Settings {
            password: password.clone(),
            ascii: ascii.clone(),
            custom_tables: custom_tables.clone(),
            padding_min: *padding_min,
            padding_max: *padding_max,
        }),
    }
}
