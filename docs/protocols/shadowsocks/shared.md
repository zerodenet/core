# Shadowsocks 密码与共享原语

协议私有验证在 `protocols/shadowsocks/src/validation/`；数据面辅助函数在
`shared/{keys,eih,headers,aead,block,tcp,address,random}.rs`。根文件仅聚合入口。

完整方法目录见 [协议 README](../../../protocols/shadowsocks/README.md)。
包括历史流密码、plain、10 种 v1 AEAD 和四种 AEAD 2022；大小写与
`plain`/`none`、`cfb`/`cfb128` 别名按固定参考实现解析。

| 方法 | 主密钥 / TCP salt |
| --- | --- |
| AES-128-GCM、AES-128-CCM、AES-128-GCM-SIV、SM4 | 16 / 16 字节 |
| AES-256 与 ChaCha / XChaCha v1 AEAD | 32 / 32 字节 |
| 2022 AES-128 | 16 / 16 字节 |
| 2022 AES-256、ChaCha20、ChaCha8 | 32 / 32 字节 |

历史流密码 IV 按各方法定义，plain/table 无 IV。v1 密码使用
EVP_BytesToKey；AEAD 再以 salt 和 `ss-subkey` 进行 HKDF-SHA1。
2022 使用 base64 解码后的 PSK，BLAKE3 KDF 上下文为
`shadowsocks 2022 session subkey`；AES UDP 使用会话 ID 派生密钥。
2022 ChaCha UDP 使用 XChaCha 与 24 字节随机 nonce。
SIP023 AES key chain 会实际发送并解析 EIH，最终 uPSK 用于业务数据和响应。

TCP AEAD 使用小端递增 nonce；计数器超过可用范围失败，不能饱和或回绕复用。
常规 AEAD 块上限 16383 字节，2022 为 65535 字节。
地址格式为 `[ATYP][ADDR][PORT]`，支持 IPv4、IPv6 与一字节长度域名。
不完整域名和无效认证数据返回错误，不能 panic 或提前污染重放状态。

`validation` 不编译数据面；`runtime` 启用协议、密码及载体；`blake3`
额外启用 2022 密钥支持。未编译的 2022 能力在验证阶段明确拒绝。
