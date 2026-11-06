# Packet source device and TUN responsibility

Draft continuation for role C.

Source parent: `7dd242f4d9c0878f6011e0e1c3f365caa22eb0f4`. Base: `codex/v003-cloud-r2-integration`.

原 Source/driver/SDK、真实输入屏障、QueryCall 消费与局部任务额度保到 Host ACK。需 A 20035c04、M 23996e78/616911dc；standalone WG/provider/helper 和旧链删除仍继续。

本提交仅登记已有实际源码的审阅检查点；本域未结题。开发阶段不反复运行全量，
因此检查点跳过自动 CI；最终稳定源码必须另获精确 SHA、Rust 1.99.0、同一
candidate.Cargo.lock 的实际 CI 和完整资格。不得借历史 PR 或本记录赋予资格。
原始日志、失败/忽略/未执行及文件/产物摘要由 M 单环境队列保存并最终归档。
V1 配置/API/events/ACK/revision/计量/CLI/FFI 保持，不改消费者。
TEMP-WG-OUTER-UDP-01 仅原范围保留未关闭。未合 develop/main、发布或部署。
