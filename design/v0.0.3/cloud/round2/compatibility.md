
### 续作控制借阅与取消（开发中）

Config/Policy、Runtime/Flows/Stats、PrincipalFlows、Sinks 原表借阅视图保持既有 V1 wire，
不启动额外 Proxy。gRPC 持久错误头保留实际字节计额，已结束 RPC 并发额度独立归还。
控制连接 stop 传递同次 Apply/Probe 调用：接受前取消，接受后保留原补偿与 Scope ACK；
coalesced Probe 的其他调用者保持原业务，最后调用者取消同一原件。其余 DTO、事件与
Target/DNS/Trace 资源责任继续实施；上述接口存在不表示完整职责域关闭。
