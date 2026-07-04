# Nacos 面试补充：配置长轮询 + 横向对比 + 生产实践

## 一、配置中心长轮询机制（面试高频）

### 1.1 为什么用长轮询？

| 方案 | 延迟 | 资源消耗 | Nacos 选择 |
|------|------|---------|-----------|
| **短轮询**（定时拉取） | 高（取决于拉取间隔） | 高（大量无效请求） | ❌ |
| **长连接**（WebSocket/SSE） | 低 | 中（服务端维护连接） | ❌（1.x HTTP 时代） |
| **长轮询**（hold 住请求） | 低 | 低（配置不变时无数据传输） | ✅ 1.x 客户端 |
| **gRPC 推送** | 最低 | 低 | ✅ 2.x 客户端 |

### 1.2 长轮询工作流程

```
客户端发起监听请求
    │  请求头：Long-Pulling-Timeout: 30000（30s）
    │  请求体：dataId=app.yaml&group=DEFAULT_GROUP&md5=abc123
    ▼
ConfigServletInner.doPollingConfig()
    │  判断是否支持长轮询（检查 Long-Pulling-Timeout 请求头）
    ▼
LongPollingService.addLongPollingClient()
    │
    ├── 第一步：MD5 对比
    │   MD5Util.compareMd5(clientMd5Map, serverMd5Map)
    │   ├── 有变化 → 立即返回变更的 groupKey 列表（不等 30s）
    │   └── 无变化 → 进入第二步
    │
    ├── 第二步：hold 住请求
    │   req.startAsync()                    ← Servlet 3.0 异步
    │   asyncContext.setTimeout(0L)          ← 禁用容器超时，自己控制
    │   timeout = 30000 - 500 = 29500ms     ← 提前 500ms 返回，避免客户端超时
    │   new ClientLongPolling(asyncContext, clientMd5Map, timeout)
    │   加入 allSubs 队列（ConcurrentLinkedQueue）
    │
    └── 第三步：两种触发返回的路径
        ├── 路径 A：超时 → 返回空（无变更）
        │   ConfigExecutor.scheduleLongPolling(timeoutTask, 29500ms)
        │   超时后：allSubs.remove() → asyncContext.complete()
        │
        └── 路径 B：配置变更 → 立即返回
            其他节点发布配置 → CP 协议同步到本节点
            → LocalDataChangeEvent 发布
            → LongPollingService 订阅者收到事件
            → DataChangeTask.run()
            → 遍历 allSubs，找到监听该 groupKey 的 ClientLongPolling
            → 取消超时定时器 → 立即返回变更列表 → asyncContext.complete()
```

### 1.3 关键源码证据

```java
// LongPollingService.java L200-L203
// Servlet 3.0 异步处理，hold 住请求不返回
final AsyncContext asyncContext = req.startAsync();
asyncContext.setTimeout(0L);  // 禁用容器超时

// LongPollingService.java L220-L222
// 超时时间 = 客户端指定的 30s - 500ms（提前返回避免客户端超时）
long timeout = Math.max(minLongPoolingTimeout,
    Long.parseLong(requestLongPollingTimeOut) - delayTime);

// LongPollingService.java L252-L267
// 订阅配置变更事件，变更时立即通知等待的客户端
NotifyCenter.registerSubscriber(new Subscriber() {
    public void onEvent(Event event) {
        if (event instanceof LocalDataChangeEvent) {
            LocalDataChangeEvent evt = (LocalDataChangeEvent) event;
            ConfigExecutor.executeLongPolling(new DataChangeTask(evt.groupKey));
        }
    }
});
```

### 1.4 面试标准回答

> **Nacos 配置变更通知机制是长轮询（1.x 客户端）+ gRPC 推送（2.x 客户端）的组合。**
>
> **1.x 客户端**：客户端发 HTTP 请求监听配置，服务端用 Servlet 3.0 AsyncContext hold 住请求最多 30 秒。如果配置变更，立即返回；如果 30 秒内没变更，返回空。客户端收到响应后立即发下一次监听请求。
>
> **为什么不用短轮询？** 短轮询间隔太长延迟高，间隔太短请求量大。长轮询兼顾低延迟和低资源消耗。
>
> **2.x 客户端**：改用 gRPC 长连接，服务端直接推送变更，延迟更低。

---

## 二、Nacos vs 同类组件横向对比

### 2.1 服务发现对比

| 维度 | Nacos | Eureka | Consul | Zookeeper |
|------|-------|--------|--------|-----------|
| **一致性模型** | AP + CP（可切换） | AP（最终一致） | CP（Raft） | CP（ZAB） |
| **临时实例** | ✅（Distro 协议） | ✅（心跳续约） | ✅（TTL check） | ✅（临时节点） |
| **持久实例** | ✅（JRaft） | ❌ | ✅ | ✅（持久节点） |
| **健康检查** | TCP/HTTP/MySQL/自定义 | 客户端心跳（30s） | TCP/HTTP/TTL/Script | Keep-Alive |
| **负载均衡** | 权重/元数据 | Ribbon | Fabio | — |
| **雪崩保护** | ✅（保护阈值） | ✅（自我保护） | ❌ | ❌ |
| **客户端协议** | gRPC + HTTP | HTTP | HTTP/DNS | TCP |
| **跨数据中心** | 需自行同步 | 多区域部署 | 原生支持 | 原生支持 |

### 2.2 配置中心对比

| 维度 | Nacos | Apollo | Spring Cloud Config |
|------|-------|--------|-------------------|
| **部署复杂度** | 低（单组件） | 高（Admin + Portal + MetaServer） | 中（依赖 Git） |
| **实时推送** | ✅（长轮询/gRPC） | ✅（长轮询） | ❌（需 Bus 配合） |
| **灰度发布** | ✅（Beta/Tag） | ✅（完善） | ❌ |
| **权限控制** | 基础（RBAC） | 完善（RBAC + 环境隔离） | 无 |
| **配置回滚** | ✅ | ✅ | ✅（Git revert） |
| **KV 配置** | ✅ | ❌（结构化） | ❌（文件） |

### 2.3 核心差异一句话总结

| 对比 | 一句话 |
|------|--------|
| **Nacos vs Eureka** | Nacos = Eureka + Spring Cloud Config + 管理后台，功能更全 |
| **Nacos vs Consul** | Consul 的 Raft 是原生集成的，Nacos 的 JRaft 是后来加的；Nacos 的 Distro 比 Consul 的 Gossip 更快 |
| **Nacos vs Zookeeper** | ZK 是通用协调服务，Nacos 是专为微服务设计的注册中心+配置中心 |
| **Nacos vs Apollo** | 配置中心选 Apollo（功能更完善），注册中心选 Nacos（更轻量） |

---

## 三、Nacos 2.x vs 1.x 核心区别

### 3.1 通信模型

| | 1.x | 2.x+ |
|--|-----|------|
| **客户端→服务端** | HTTP 短连接 | gRPC 长连接 |
| **配置变更通知** | HTTP 长轮询（hold 30s） | gRPC 服务端推送 |
| **服务发现** | HTTP 拉取 + UDP 推送 | gRPC 长连接 + 推送 |
| **心跳方式** | HTTP 定时心跳 | gRPC 双向心跳 |
| **连接数** | 每次请求一个连接 | 一个客户端一条长连接 |

### 3.2 架构变化

```
1.x 架构：
  Client ──HTTP──▶ Nacos Server
  Client ◀──UDP─── Nacos Server（服务推送）
  Client ──HTTP──▶ Nacos Server（配置长轮询，hold 30s）

2.x 架构：
  Client ═══gRPC══▶ Nacos Server（一条长连接搞定一切）
         双向心跳 + 服务端推送 + 请求响应
```

### 3.3 为什么改 gRPC？

| 问题（1.x） | 解决（2.x） |
|------------|------------|
| 长轮询 hold 住大量 HTTP 连接，Tomcat 线程池耗尽 | gRPC 异步多路复用，一条连接承载多个请求 |
| UDP 推送不可靠，可能丢包 | gRPC 基于 TCP，可靠传输 |
| 每次 HTTP 请求都要建连 | 长连接复用，减少握手开销 |
| 服务发现靠客户端拉取，有延迟 | 服务端主动推送，实时性更好 |

### 3.4 兼容性

- 2.x 服务端**兼容** 1.x 客户端（HTTP 接口保留）
- 1.x 服务端**不兼容** 2.x 客户端（gRPC 端口不存在）
- 升级策略：先升服务端，再升客户端

---

## 四、生产实践要点

### 4.1 集群部署建议

| 规模 | 节点数 | 配置 | 说明 |
|------|--------|------|------|
| 小型（<100 服务） | 3 节点 | 4C8G | 最小集群，容忍 1 节点挂 |
| 中型（100-1000 服务） | 3-5 节点 | 8C16G | Raft 选举需要奇数节点 |
| 大型（>1000 服务） | 5-7 节点 | 16C32G | 超过 7 节点 Raft 选举变慢 |

**为什么节点数不能太多？**
- JRaft（Raft）选举需要多数节点同意，节点越多选举越慢
- Distro 全量验证任务需要遍历所有节点，节点多时验证耗时长

**为什么必须是奇数？**
- Raft 选举需要 `(N/2)+1` 个节点同意
- 4 节点和 3 节点都只能容忍 1 节点挂，但 4 节点选举更慢 → 不如 3 节点

### 4.2 关键配置调优

```properties
# Distro 同步（临时实例同步速度）
nacos.core.protocol.distro.data.sync.delayMs=1000      # 攒批延迟，默认 1s
nacos.core.protocol.distro.data.verify.intervalMs=5000  # 全量验证间隔，默认 5s

# JRaft 选举
nacos.core.protocol.raft.election.timeout.ms=5000       # 选举超时，默认 5s
# 网络不稳定时可以调大到 10s，减少不必要的主从切换

# gRPC 连接（2.x）
nacos.remote.server.rpc.executor.times.of.processors=16  # gRPC 线程池 = CPU × 16
nacos.remote.server.rpc.queue.size=16384                 # 请求队列大小

# JVM 参数（生产必须）
-Xms4g -Xmx4g           # 堆内存固定，避免 GC 抖动
-XX:+UseG1GC            # G1 垃圾收集器
-XX:MaxGCPauseMillis=100 # 最大 GC 停顿时间
-XX:+ParallelRefProcEnabled
```

### 4.3 常见故障与排查

| 故障现象 | 可能原因 | 排查方法 |
|---------|---------|---------|
| 服务注册后其他节点看不到 | Distro 同步延迟 / 网络不通 | 查 `distro.log`，看 sync 是否成功 |
| 配置发布后客户端没更新 | CP 协议未就绪 / 长轮询连接断开 | 查 `raft.log`，看 isLeader() |
| 频繁 Leader 切换 | 网络抖动 / 选举超时太短 | 调大 `election.timeout.ms` |
| 节点状态 SUSPICIOUS | 心跳失败次数 ≤ 3 | 查 `member-change.log` |
| 节点状态 DOWN | 心跳失败次数 > 3 或 Connection Refused | 检查目标节点是否存活 |
| 客户端连接频繁断开 | gRPC 心跳超时 / GC 停顿 | 调大 `-XX:MaxGCPauseMillis` |

### 4.4 监控指标（面试加分项）

```
# Nacos 内置 Prometheus 指标
nacos_monitor{name="configCount"}       # 配置总数
nacos_monitor{name="serviceCount"}      # 服务总数
nacos_monitor{name="instanceCount"}     # 实例总数
nacos_monitor{name="longPolling"}       # 长轮询客户端数（1.x）
nacos_monitor{name="currentConnections"} # gRPC 连接数（2.x）
nacos_monitor{name="raftLeader"}        # Raft Leader 数量
```

---

## 五、面试高频 Q&A

### Q1：Nacos 服务注册后其他节点多久能看到？

> **临时实例（Distro/AP）**：变更数据攒批 1s 后同步到所有节点。由于 99 个节点并行探测，平均延迟约 1-2s。
>
> **持久实例（JRaft/CP）**：需要等 Raft Leader 复制日志到多数节点，通常 <100ms。

### Q2：Nacos 怎么保证配置不丢失？

> 配置写入走 JRaft（CP 协议），Raft 日志必须复制到多数节点才算成功。即使 Leader 挂了，新 Leader 的日志里一定有这条数据。
>
> 此外还有 Dump 机制：每次配置变更会写入本地 Derby/MySQL，重启时从数据库加载。

### Q3：Nacos 和 Eureka 的自我保护有什么区别？

> **Eureka 自我保护**：当 15 分钟内超过 85% 的节点心跳异常时，进入自我保护模式，不剔除任何实例。目的是防止网络分区导致服务全部下线。
>
> **Nacos 保护阈值**：`nacos.naming.distro.initData = true`，通过 `SwitchDomain` 控制。Nacos 的保护机制更细粒度，可以按服务配置不同的保护阈值。

### Q4：Nacos 集群能水平扩容吗？

> 可以，但有约束：
> - **CP 数据**（配置/元数据）：Raft 选举要求奇数节点，扩容需要调整集群配置
> - **AP 数据**（临时实例）：Distro 协议天然支持动态扩缩，新节点启动后自动从其他节点加载数据
> - 实际操作：建议初始 3 或 5 节点，通过垂直扩容（加 CPU/内存）而不是水平扩容

### Q5：Nacos 的 Distro 和 Consul 的 Gossip 有什么区别？

| | Distro | Gossip |
|--|--------|--------|
| 数据所有权 | ✅（每个节点负责一部分数据） | ❌（每个节点持有全量数据） |
| 同步方式 | 变更后主动推给所有节点 | 随机选节点交换数据 |
| 收敛速度 | 快（1-2s） | 慢（需要多轮随机交换） |
| 一致性保证 | 最终一致 | 最终一致 |

> Distro 的优势在于**数据分片**——每个节点只管理自己负责的客户端数据，变更时直接推送，不需要像 Gossip 那样多轮随机交换才能收敛。

---

## 六、文件清单

| 文件路径 | 角色 |
|---------|------|
| `config/.../service/LongPollingService.java` | 长轮询核心（hold 请求 + 变更通知） |
| `config/.../controller/ConfigServletInner.java` | 配置监听入口（判断长短轮询） |
| `config/.../utils/ConfigExecutor.java` | 配置模块线程池（长轮询超时调度） |
| `config/.../model/event/LocalDataChangeEvent.java` | 配置变更事件（触发立即返回） |
