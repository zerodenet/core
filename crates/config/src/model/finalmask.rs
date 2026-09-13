use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FinalMaskConfig {
    pub tcp: Vec<TcpMaskConfig>,
    pub udp: Vec<UdpMaskConfig>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum UdpMaskConfig {
    Xicmp {
        #[serde(default)]
        ip: String,
        #[serde(default)]
        id: u16,
    },
    Xdns {
        domain: String,
    },
    Noise {
        #[serde(default)]
        reset_seconds: MaskRangeConfig,
        items: Vec<NoiseItemConfig>,
    },
    Sudoku {
        password: String,
        #[serde(default)]
        ascii: String,
        #[serde(default)]
        custom_tables: Vec<String>,
        #[serde(default)]
        padding_min: u32,
        #[serde(default)]
        padding_max: u32,
    },
    HeaderDns {
        #[serde(default)]
        domain: String,
    },
    HeaderDtls,
    HeaderSrtp,
    HeaderUtp,
    HeaderWechat,
    HeaderWireguard,
    HeaderCustom {
        client: Vec<MaskItemConfig>,
        server: Vec<MaskItemConfig>,
    },
    MkcpOriginal,
    MkcpAes128Gcm {
        password: String,
    },
    Salamander {
        password: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MaskItemConfig {
    Bytes {
        bytes: Vec<u8>,
    },
    Random {
        length: usize,
        #[serde(default)]
        minimum: u8,
        #[serde(default = "maximum_byte")]
        maximum: u8,
    },
}
fn maximum_byte() -> u8 {
    255
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MaskRangeConfig {
    pub minimum: u64,
    pub maximum: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpMaskItemConfig {
    #[serde(default)]
    pub delay_ms: MaskRangeConfig,
    pub content: MaskItemConfig,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum TcpMaskConfig {
    HeaderCustom {
        clients: Vec<Vec<TcpMaskItemConfig>>,
        #[serde(default)]
        servers: Vec<Vec<TcpMaskItemConfig>>,
        #[serde(default)]
        errors: Vec<Vec<TcpMaskItemConfig>>,
    },
    Fragment {
        packets: MaskRangeConfig,
        length: MaskRangeConfig,
        #[serde(default)]
        delay_ms: MaskRangeConfig,
        #[serde(default)]
        max_splits: MaskRangeConfig,
    },
    Sudoku {
        password: String,
        #[serde(default)]
        ascii: String,
        #[serde(default)]
        custom_tables: Vec<String>,
        #[serde(default)]
        padding_min: u32,
        #[serde(default)]
        padding_max: u32,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NoiseItemConfig {
    pub packet: Vec<u8>,
    pub random_length: MaskRangeConfig,
    pub minimum_byte: u8,
    #[serde(default = "maximum_byte")]
    pub maximum_byte: u8,
    pub delay_ms: MaskRangeConfig,
}
