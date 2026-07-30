---
title: 项目概览
---

# 项目概览

`project/` 保存 Zero 的当前架构事实、长期规范和发布边界。普通使用者通常只需阅读配置与协议文档；修改内核、控制面或文件格式时，再进入对应的架构和规范页面。

## 使用与配置

- [配置参考](./config.md)：运行时、入站、出站、路由、DNS 和控制面配置。
- [运行模式与出站组](./modes-and-groups.md)：`selector`、`url_test`、`fallback`、`relay` 和嵌套选择语义。
- [构建特性](./features.md)：Cargo feature 与可选组件。

## 架构

- [总体架构](./architecture.md)：crate 分层、依赖方向和运行时责任。
- [请求生命周期](./lifecycle.md)：连接从入站到路由、出站与观测的流转。
- [引擎计划](./engine-plan.md)：`RuntimeConfig`、`EnginePlan` 和运行状态的边界。
- [协议能力模型](./protocol-capabilities.md)：协议能力注册和机器可读状态。

## 格式与规范

- [Zero 规则 IR v1](./zero-rule-ir-v1.md)
- [ZRS 0.1 二进制格式](./zrs-0.1.md)
- [ZRS 0.1 Golden Vector](./zrs-0.1-golden.md)

## 工程与项目边界

- [日志](./logging.md)、[工程规则](./tooling.md)、[发布边界](./release-boundary.md)
- [项目定位](./positioning.md)、[项目目标](./goals.md)
- [API 能力模型](./api.md)、[控制面规范](./control-plane.md)、[Connector 通信边界](./connector-architecture.md)、[通用受管材料事务设计](./managed-materials.md)
- [Connector 真实实现审查报告](./connector-change-review-2026-07-24.md)、[公开 Connector 接入指南](https://docs.zerodenet.org/projects/core/guides/connector-integration)
