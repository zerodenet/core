# 内部工程资料

本目录只保存 Zero Core 的架构、实现边界、工程约束和实现审查记录。
面向用户的配置、功能、协议能力与格式参考统一由
[ZeroDeNet 文档仓库](https://github.com/zerodenet/docs)维护，并发布在
<https://docs.zerodenet.org/projects/core/>。

## 架构与运行时

- [总体架构](./architecture.md)
- [请求生命周期](./lifecycle.md)
- [EnginePlan](./engine-plan.md)
- [API 能力模型](./api.md)
- [控制面规范](./control-plane.md)
- [Connector 通信边界](./connector-architecture.md)
- [通用受管材料事务设计](./managed-materials.md)

## 工程约束

- [工程规则](./tooling.md)
- [日志](./logging.md)
- [发布边界](./release-boundary.md)

## 项目决策与审查

- [项目定位](./positioning.md)
- [项目目标](./goals.md)
- [Connector 实现审查报告](./connector-change-review-2026-07-24.md)
- [Connector 能力提交分析报告](./connector-capability-commit-report-2026-07-26.md)
