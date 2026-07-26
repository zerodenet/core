---
title: 控制面设计档案
---

# 控制面设计档案

::: warning 文档定位
本目录保存控制面建设过程中的路线图和设计草案，不是当前对外接口的权威规范。实现和对接应以 [控制与集成](../control-plane-api/) 中的配置、接口、事件和 IPC 契约为准。
:::

## 档案目录

| 文档 | 内容 |
| --- | --- |
| [实现路线图](./01-control-plane-roadmap.md) | 阶段划分与早期架构决策 |
| [API 端点草案](./02-api-endpoints.md) | 早期 HTTP 端点模型 |
| [HTTP 适配器设计](./03-http-adapter-design.md) | 路由、处理器与中间件方案 |
| [事件系统设计](./04-event-system.md) | 事件总线与 Sink 方案 |
| [认证与权限设计](./05-auth-and-permissions.md) | 认证和权限模型 |
| [服务提供者集成](./06-service-provider-integration.md) | 内核与上层业务边界 |
| [性能与限流设计](./08-performance-and-rate-limiting.md) | 性能估算和限流分层 |
