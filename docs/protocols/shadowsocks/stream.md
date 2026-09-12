# Shadowsocks Stream

`stream.rs` 持有协议读写状态，`stream/read.rs` 和 `stream/write.rs`
实现异步局部 I/O。协议状态不转移给 proxy。

流密码保持连续 cipher 状态；AEAD 保持独立上下行密钥和 nonce、待写密文、
写入位置及分阶段接收缓冲。`Poll::Pending` 和取消不会丢弃已收密文或重发
已经接受的明文。`flush` 清空待写密文，`shutdown` 先 flush，再关闭底层写端。

v1 AEAD 块上限为 16383 字节，2022 为 65535 字节。2022 第一个响应头包含
方向、时间戳、请求 salt 及首个 payload 长度；客户端先验证这些字段和响应
salt 重放，再提供明文。nonce 小端递增，超过可用范围失败，不能回绕复用。

完整块后的 EOF 正常结束；salt、长度或 payload 中途 EOF 返回错误。
历史流密码不声称提供 AEAD 完整性。目标地址跨块续读由握手层负责。
逐字节短读写、Pending、取消恢复、flush/shutdown、连续流密码与固定密码库
对照由 `reference_ciphers` 测试覆盖；2022 标准线路由官方双向矩阵覆盖。

参考 [parity.md](parity.md) 与 [状态说明](../shadowsocks-udp-state.md)。
