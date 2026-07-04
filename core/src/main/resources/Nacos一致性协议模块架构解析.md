# Nacos 一致性协议（distributed）模块架构解析

## 一、为什么需要一致性协议？

Nacos 是多节点集群部署的。当客户端向任意一个节点写入数据时，这个数据必须同步到其他节点，否则不同节点看到的数据不一致。

Nacos 采用了**混合一致性模型**：

| 数据类型 | 一致性协议 | 特点 |
|---------|-----------|------|
| **临时实例**（ephemeral） | **Distro（AP）** | 每个节点负责自己管理的客户端数据，主动推给其他节点，最终一致 |
| **持久配置/元数据** | **JRaft（CP）** | 通过 Raft 选举 Leader，写操作必须经 Leader 复制日志，强一致 |

一句话总结：**临时数据用 AP（快、允许短暂不一致），持久数据用 CP（慢、保证强一致）**。

---

## 二、整体架构

### 2.1 数据流转图（全貌）

```
                          ┌─────────────────────────────────────────┐
                          │              消费端（调用方）              │
                          └─────────────────────────────────────────┘
                                           │
        ┌──────────────────────────────────┼──────────────────────────────────┐
        │                                  │                                  │
        ▼                                  ▼                                  ▼
  ┌──────────────┐               ┌──────────────────┐              ┌──────────────────┐
  │  Config 模块  │               │   Naming 模块    │              │   Lock 模块      │
  │ EmbeddedDump │               │ ServerStatusMgr  │              │ LockOperationSvc │
  │ Service      │               │ MetadataProcessor│              │                  │
  │ PluginState  │               │ SwitchManager    │              │                  │
  │ Processor    │               │                  │              │                  │
  └──────┬───────┘               └────────┬─────────┘              └────────┬─────────┘
         │ getCpProtocol()                 │ getCpProtocol()                │ getCpProtocol()
         │                                 │                                │
         └──────────────┬──────────────────┘                                │
                        ▼                                                   │
              ┌──────────────────────────┐                                  │
              │      ProtocolManager     │◄─────────────────────────────────┘
              │  (Spring @Component)     │
              │                          │
              │  getCpProtocol() ← DCL懒加载
              │  getApProtocol() ← DCL懒加载
              │  onEvent(MembersChangeEvent) → 通知协议层节点变更
              └────────────┬─────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
    ┌──────────────────┐     ┌──────────────────────────────┐
    │  CPProtocol      │     │  Distro（AP，不走 APProtocol） │
    │  (JRaft 实现)    │     │  直接注入到 Naming 模块使用     │
    └────────┬─────────┘     └──────────────┬───────────────┘
             ▼                              ▼
    ┌──────────────────┐     ┌──────────────────────────────┐
    │    JRaftServer   │     │       DistroProtocol         │
    │  (SOFAJRaft 封装) │     │  ┌─ DistroComponentHolder ─┐ │
    │                  │     │  │  DataProcessor           │ │
    │  多 RaftGroup    │     │  │  TransportAgent          │ │
    │  独立状态机      │     │  │  DataStorage             │ │
    └──────────────────┘     │  │  FailedTaskHandler       │ │
                             │  └──────────────────────────┘ │
                             │                               │
                             │  ┌─ DistroTaskEngineHolder ─┐ │
                             │  │  DelayTaskExecuteEngine  │ │
                             │  │  ExecuteTaskExecuteEngine│ │
                             │  └──────────────────────────┘ │
                             └──────────────────────────────┘
```

### 2.2 类关系图（UML 精简版）

```
                         ┌──────────────────────────────┐
                         │  ConsistencyConfiguration    │
                         │  <<static @Configuration>>   │
                         │  strongAgreementProtocol()   │
                         │  SPI 加载 CPProtocol        │
                         └──────────────┬───────────────┘
                                        │ 创建
                                        ▼
┌─────────────────────────────┐   ┌──────────────────────────┐
│   <<interface>>             │   │  <<abstract>>            │
│   ConsistencyProtocol       │◄──│  AbstractConsistencyProto │
│   init() / write() / getData│   │  processorMap            │
│   memberChange() / shutdown │   │  metaData                │
└──────────┬──────────────────┘   └───────────┬──────────────┘
           │                                   │
     ┌─────┴─────┐                             │
     ▼           ▼                             │
┌─────────┐ ┌─────────┐                        │
│CPProtocol│ │APProtocol│                       │
│isLeader()│ │(空接口)   │                      │
└────┬────┘ └──────────┘                       │
     │                                          │
     ▼                                          │
┌──────────────────┐                            │
│  JRaftProtocol   │────────────────────────────┘
│  raftServer      │
│  write/getData   │
│  memberChange    │
└────────┬─────────┘
         │ 持有
         ▼
┌──────────────────┐     ┌────────────────────┐     ┌─────────────────┐
│   JRaftServer    │     │  NacosStateMachine  │     │ JRaftMaintainSvc│
│  multiRaftGroup  │────▶│  (SOFAJRaft SM)    │     │  peerChange()   │
│  commit/get      │     └────────────────────┘     └─────────────────┘
└──────────────────┘

---

┌──────────────────┐     ┌──────────────────────┐
│ ProtocolManager  │────▶│   ProtocolExecutor   │
│ getCpProtocol()  │     │  cpMemberChange()    │
│ getApProtocol()  │     │  apMemberChange()    │
│ onEvent(MCE)     │     │  (各一个单线程池)     │
└──────────────────┘     └──────────────────────┘

---

┌──────────────────────┐     ┌────────────────────────┐
│    DistroProtocol    │────▶│  DistroComponentHolder │
│  sync() / onReceive()│     │  TransportAgent map    │
│  onVerify()/onQuery()│     │  DataStorage map       │
│  onSnapshot()        │     │  DataProcessor map     │
└──────────┬───────────┘     │  FailedTaskHandler map │
           │                 └────────────────────────┘
           │ 持有
           ▼
┌──────────────────────┐     ┌────────────────────────────┐
│ DistroTaskEngineHolder│    │  DistroDelayTaskExecuteEngine│
│ delayTaskExecuteEngine│───▶│  (按 resourceType 分发)      │
│ executeWorkersManager │    └────────────────────────────┘
└──────────────────────┘

图例：
  <<interface>>  — 接口
  <<abstract>>   — 抽象类
  ◀△             — 继承/实现
  ──▶            — 依赖（调用）
  ──◆            — 组合（持有）
  ────           — 关联
```

### 2.3 设计模式分析

| 模式 | 体现在哪 | 解决什么问题 | 好处 | 坏处 |
|------|---------|-------------|------|------|
| **策略模式** | CP/Distro 两种协议独立实现，ProtocolManager 统一管理 | 不同数据需要不同一致性保证 | 新增协议只需实现接口，不改消费方 | 消费方需要知道用哪种协议 |
| **模板方法** | AbstractConsistencyProtocol 固定 processorMap + metaData，子类只实现协议逻辑 | 公共逻辑不重复 | 协议实现者只关注核心逻辑 | 抽象层较薄，收益有限 |
| **SPI 扩展** | ConsistencyConfiguration 用 NacosServiceLoader 加载 CPProtocol，找不到才用 JRaftProtocol | 允许外部替换一致性实现 | 可扩展，默认实现可靠 | 实际几乎没有自定义 CP 实现 |
| **双重检查锁（DCL）** | ProtocolManager.getCpProtocol/getApProtocol 的懒加载 | 协议初始化较重，按需加载 | 避免启动时阻塞，线程安全 | 代码略复杂 |
| **注册表模式** | DistroComponentHolder 按 resourceType 注册 4 种组件 | Distro 协议不感知具体业务数据 | 业务模块自行注册，Distro 层零耦合 | 注册时机隐含在 @PostConstruct 中，不直观 |
| **延迟任务引擎** | DistroDelayTaskExecuteEngine → DistroDelayTaskProcessor → ExecuteTask | 数据变更先攒批再批量同步 | 减少网络请求，避免风暴 | 引入 1s 默认延迟，非实时 |
| **观察者模式** | ProtocolManager 订阅 MembersChangeEvent，通知协议层节点变更 | 集群拓扑变化自动同步到协议层 | 解耦集群管理与一致性协议 | 事件顺序重要，需要单线程串行化 |

**整体设计的优点**：
1. **AP/CP 混合模型**：临时数据走 AP 保证低延迟，持久数据走 CP 保证强一致，各取所长
2. **协议层与业务层解耦**：Distro 通过 DistroComponentHolder 不知道业务数据是什么，Naming 自行注册
3. **按需初始化**：CP 协议通过 DCL 懒加载，只有第一次使用时才初始化 JRaft
4. **多 RaftGroup 隔离**：每个 LogProcessor 有独立的状态机，一个模块卡住不影响其他模块

**潜在问题**：
1. **APProtocol 接口名存实亡**：Distro 没有实现 APProtocol 接口，直接作为 @Component 注入使用
2. **Distro 组件注册时机隐式**：依赖 @PostConstruct 顺序，调试时不容易发现注册是否完成
3. **CP 协议初始化较重**：JRaftServer 启动涉及选举超时、RPC 端口绑定，首次调用可能阻塞
4. **节点变更串行化**：CP 和 AP 的 memberChange 各用一个单线程池，大量节点变更时可能积压

### 2.4 关键设计决策

**为什么 Distro 没有实现 APProtocol 接口？**

从规范文档（`foundation-ap-consistency-spec.md`）可以看到：
> 历史上 consistency 模块中存在 APProtocol 接口，但当前活跃 AP 实现是 Distro 基础能力和 Config Notify 路径。

Distro 是 Nacos 自定义的 AP 协议，不是通用的 ConsistencyProtocol 实现。它通过 `DistroProtocol` 直接提供 `sync()`、`onReceive()` 等语义化方法，而不是走 `write()`、`getData()` 的通用接口。

---

## 三、核心数据结构

### 3.1 DistroKey — Distro 数据的唯一标识

```java
DistroKey {
    resourceKey: "1.2.3.4:8848#DEFAULT_GROUP@@service-name"  // 具体数据标识
    resourceType: "Distro"                                     // 数据类型（用于路由到组件）
    targetServer: "1.2.3.5:8848"                              // 同步目标节点
}
```

### 3.2 DistroData — Distro 传输的数据载体

```java
DistroData {
    distroKey: DistroKey          // 数据标识
    type: DataOperation           // ADD / CHANGE / DELETE / VERIFY / SNAPSHOT / QUERY
    content: byte[]               // 序列化后的业务数据
}
```

### 3.3 RaftConfig — JRaft 协议配置

```java
RaftConfig {
    selfAddress: "1.2.3.4:7848"              // 自身 Raft 地址
    members: ["1.2.3.4:7848", "1.2.3.5:7848"]  // Raft 集群成员
    data: Map<String, String>                 // KV 配置（选举超时、RPC 超时等）
    strictMode: boolean                        // 是否严格模式
}
```

---

## 四、数据流转全景

### 4.1 启动阶段

```
Spring Boot 启动
    │
    ├── ConsistencyConfiguration.strongAgreementProtocol()
    │       SPI 加载 CPProtocol → 找不到 → 使用 JRaftProtocol
    │       注册为 Spring Bean
    │
    ├── DistroProtocol 构造器
    │       startDistroTask()
    │       ├── startLoadTask()  → DistroLoadDataTask
    │       │       等待 serverList 初始化
    │       │       等待 DistroDataStorage 注册
    │       │       从远程节点拉取快照 → 成功后 isInitialized = true
    │       │
    │       └── startVerifyTask() → DistroVerifyTimedTask（每 5s 执行）
    │
    └── DistroClientComponentRegistry.doRegister() (@PostConstruct)
            注册 4 种组件到 DistroComponentHolder：
            ├── DataStorage:   DistroClientDataProcessor
            ├── DataProcessor: DistroClientDataProcessor
            ├── TransportAgent: DistroClientTransportAgent
            └── FailedTaskHandler: DistroClientTaskFailedHandler
```

### 4.2 运行时 — 临时实例数据同步（Distro）

```
客户端注册临时实例 → Naming 节点 A
    │
    ▼
DistroClientDataProcessor.onClientChanged(client)
    │  监听客户端变更事件
    ▼
distroProtocol.sync(distroKey, DataOperation.CHANGE)
    │  DistroProtocol.java L106
    ▼
遍历所有远程节点，每个节点创建一个 DistroDelayTask
    │  DistroProtocol.java L117-L121
    ▼
DistroDelayTaskExecuteEngine（攒批 1s）
    │  默认延迟 1000ms
    ▼
DistroDelayTaskProcessor.process()
    │  按 action 分发：CHANGE → DistroSyncChangeTask
    ▼
DistroSyncChangeTask.doExecute()
    │  从 DataStorage 取数据 → TransportAgent.syncData()
    ▼
DistroClientTransportAgent（通过 ClusterRpcClientProxy 发送 gRPC 请求）
    │
    ▼
远端节点 DistroDataRequestHandler.handle()
    │  收到 DistroDataRequest
    ▼
distroProtocol.onReceive(distroData)
    │  DistroProtocol.java L150
    ▼
dataProcessor.processData(distroData)
    │  反序列化 ClientSyncData → 更新本地 ClientManager
    ▼
远端节点数据已同步
```

### 4.3 运行时 — 持久数据写入（JRaft/CP）

```
客户端发布配置 → 任意 Nacos 节点
    │
    ▼
ConfigService.write() → ProtocolManager.getCpProtocol()
    │  DCL 懒加载 JRaftProtocol
    ▼
JRaftProtocol.write(WriteRequest)
    │  JRaftProtocol.java L176-L180
    │  超时 10 秒等待结果
    ▼
raftServer.commit(group, request, future)
    │  JRaftServer 内部
    ▼
JRaft Leader 将写请求追加到 Raft 日志 → 复制到 Follower
    │
    ▼
NacosStateMachine.onApply(committer)
    │  SOFAJRaft 状态机回调
    ▼
找到 group 对应的 RequestProcessor4CP → processor.onApply(request)
    │
    ▼
配置落盘 → 返回 Response
```

### 4.4 运行时 — 集群成员变更

```
ServerMemberManager 检测到节点变化
    │  发布 MembersChangeEvent
    ▼
ProtocolManager.onEvent(MembersChangeEvent)
    │  ProtocolManager.java L162-L179
    ▼
    ├── ProtocolExecutor.apMemberChange(() ->
    │       apProtocol.memberChange(AP地址集合))
    │       单线程池，保证顺序
    │
    └── ProtocolExecutor.cpMemberChange(() ->
            cpProtocol.memberChange(CP地址集合:raftPort))
            单线程池，保证顺序

Distro.memberChange → 更新 memberManager 列表
JRaft.memberChange  → raftServer.peerChange()（最多重试 5 次，每次间隔 100ms）
```

---

## 五、核心类详解

### 5.1 ProtocolManager — 协议生命周期管理者

**定位**：Spring @Component，统一管理 CP/AP 两种协议的初始化和生命周期。

**核心机制**：
- **DCL 懒加载**：`getCpProtocol()` / `getApProtocol()` 首次调用时才初始化，避免启动阻塞
- **Spring Bean 发现**：通过 `ApplicationUtils.getBeanIfExist()` 查找协议实现
- **成员变更通知**：订阅 `MembersChangeEvent`，通过 `ProtocolExecutor` 串行通知两种协议
- **统一销毁**：`@PreDestroy` + `DisposableBean`，关闭时 shutdown 两种协议

**初始化链**（以 CP 为例）：
```
getCpProtocol() → initCPProtocol()
  ├── ApplicationUtils.getBeanIfExist(CPProtocol.class, ...)  // 从 Spring 获取
  ├── ClassUtils.resolveGenericType(protocol.getClass())       // 反射获取 Config 泛型
  ├── ApplicationUtils.getBean(configType)                     // 获取 RaftConfig
  ├── injectMembers4CP(config)                                // 注入集群成员
  └── protocol.init(config)                                   // 初始化 JRaft
```

### 5.2 ConsistencyConfiguration — SPI 扩展入口

**定位**：Spring @Configuration，为 CP 协议提供 SPI 扩展点。

**逻辑**：
```java
// ConsistencyConfiguration.java L40-L43
CPProtocol protocol = getProtocol(CPProtocol.class, () -> new JRaftProtocol(memberManager));
```

```
NacosServiceLoader.load(CPProtocol.class)
  ├── SPI 找到实现 → 使用第一个（只取 iterator.next()）
  └── SPI 没找到 → 使用默认 builder：new JRaftProtocol(memberManager)
```

### 5.3 DistroProtocol — AP 一致性核心

**定位**：Distro 协议的入口类（@Component），不实现 APProtocol 接口，直接提供语义化 API。

**存储**：
- `DistroComponentHolder`：按 resourceType 存储 4 种组件
- `DistroTaskEngineHolder`：延迟任务引擎 + 执行任务引擎

**核心方法**：

| 方法 | 用途 | 调用方 |
|------|------|--------|
| `sync(distroKey, action)` | 数据变更后触发同步 | DistroClientDataProcessor |
| `onReceive(distroData)` | 接收远端同步数据 | DistroDataRequestHandler |
| `onVerify(distroData, source)` | 接收验证数据 | DistroDataRequestHandler |
| `onQuery(distroKey)` | 响应单条数据查询 | DistroDataRequestHandler |
| `onSnapshot(type)` | 响应全量快照请求 | DistroDataRequestHandler |

### 5.4 DistroComponentHolder — 组件注册表

**定位**：按 resourceType 路由的组件注册表（@Component），存储 4 种组件：

| 组件 | 接口 | 职责 | Naming 注册的实现 |
|------|------|------|------------------|
| TransportAgent | `DistroTransportAgent` | gRPC 数据传输 | DistroClientTransportAgent |
| DataStorage | `DistroDataStorage` | 本地数据存储/查询 | DistroClientDataProcessor |
| DataProcessor | `DistroDataProcessor` | 处理收到的同步/验证数据 | DistroClientDataProcessor |
| FailedTaskHandler | `DistroFailedTaskHandler` | 同步失败重试 | DistroClientTaskFailedHandler |

**注册时机**：`DistroClientComponentRegistry.doRegister()` (@PostConstruct)

### 5.5 DistroTaskEngineHolder — 双层任务引擎

**定位**：管理 Distro 同步的两级任务引擎。

```
数据变更
    │
    ▼
第一级：DistroDelayTaskExecuteEngine（攒批）
    │  NacosDelayTaskExecuteEngine
    │  相同 DistroKey 的任务会合并（只保留最新的）
    │  默认延迟 1000ms
    ▼
DistroDelayTaskProcessor.process()
    │  按 action 分流
    ▼
第二级：DistroExecuteTaskExecuteEngine（执行）
    │  线程池执行 DistroSyncChangeTask / DistroSyncDeleteTask
    │  通过 TransportAgent 发送 gRPC 数据
    ▼
远程节点
```

**为什么需要两级？**
- **第一级（攒批）**：同一个 key 在 1s 内多次变更，只同步最后一次，减少网络开销
- **第二级（执行）**：真正的 gRPC 发送是 I/O 操作，需要独立线程池避免阻塞攒批逻辑

### 5.6 JRaftProtocol — CP 协议实现

**定位**：ConsistencyProtocol 的 CP 实现，封装 SOFAJRaft。

**核心存储**：
- `JRaftServer raftServer`：Raft 服务实例，管理多个 RaftGroup
- `JRaftMaintainService`：运维操作（节点变更、状态查询）

**初始化链**：
```
JRaftProtocol.init(RaftConfig)
    ├── NotifyCenter.registerToSharePublisher(RaftEvent.class)
    ├── raftServer.init(config)
    │       ├── RaftExecutor.init(config)       // 初始化 Raft 线程池
    │       ├── 解析 selfIp / selfPort
    │       ├── 设置选举超时（默认 5s）
    │       └── 创建 CliService
    ├── raftServer.start()
    │       ├── 创建 RpcServer（Bolt）
    │       ├── 注册所有 RequestProcessor4CP → 创建 RaftGroup
    │       └── 启动 RpcServer
    └── NotifyCenter.registerSubscriber(RaftEvent)
            监听 Leader 变更 → 更新 ProtocolMetaData
```

**多 RaftGroup 设计**：
```
JRaftServer.multiRaftGroup = {
    "naming_service_metadata" → RaftGroupTuple(node1, processor1, fsm1)
    "naming_instance_metadata" → RaftGroupTuple(node2, processor2, fsm2)
    "nacos_config_group"       → RaftGroupTuple(node3, processor3, fsm3)
    "lock_group"               → RaftGroupTuple(node4, processor4, fsm4)
}
```

每个 RaftGroup 有独立的 Leader 选举和日志复制，互不干扰。

### 5.7 JRaftServer — Raft 底层封装

**定位**：SOFAJRaft 的封装层（不纳入 Spring 管理），管理多 RaftGroup。

**核心方法**：

| 方法 | 用途 |
|------|------|
| `init(RaftConfig)` | 解析配置，初始化 NodeOptions |
| `start()` | 创建 RpcServer，注册所有 RaftGroup |
| `createMultiRaftGroup(processors)` | 按 processor.group() 创建独立 RaftGroup |
| `commit(group, request, future)` | 写操作：找 Leader → 追加日志 → 等待状态机 apply |
| `get(request)` | 读操作：readIndex 或直接从本地读 |
| `peerChange(service, addresses)` | 成员变更：通过 CliService 修改集群配置 |

---

## 六、Distro 定时任务详解

### 6.1 三大任务

| 任务 | 触发时机 | 间隔 | 职责 |
|------|---------|------|------|
| **DistroLoadDataTask** | 启动时 | 失败后 30s 重试 | 从远程拉取全量快照，填充本地数据 |
| **DistroVerifyTimedTask** | 启动后持续运行 | 5s | 遍历所有数据，向所有节点发送验证请求 |
| **DistroDelayTask**（按需） | 数据变更时 | 延迟 1s（攒批） | 将变更数据同步到目标节点 |

### 6.2 LoadDataTask 流程

```
DistroLoadDataTask.run()
    │
    ├── 等待 serverList 初始化（阻塞）
    ├── 等待 DistroDataStorage 注册（阻塞）
    │
    ├── 遍历所有 resourceType
    │       找 TransportAgent → getDatumSnapshot(远端地址)
    │       找 DataProcessor → processSnapshot(快照数据)
    │       成功 → finishInitial + 标记完成
    │       失败 → 尝试下一个节点
    │
    └── checkCompleted()
            全部完成 → callback.onSuccess() → isInitialized = true
            未全部完成 → 30s 后重试
```

### 6.3 VerifyTimedTask 流程

```
DistroVerifyTimedTask.run()  （每 5s）
    │
    ├── 获取所有远程节点
    ├── 遍历所有 DataStorageType
    │       从 DataStorage 取 verifyData（全量数据的 key 列表）
    │       对每个远程节点 → 创建 DistroVerifyExecuteTask
    │
    └── ExecuteEngine 执行 DistroVerifyExecuteTask
            TransportAgent.syncVerifyData(远端, 验证数据)
            │
            ▼ 远端收到验证
            DistroProtocol.onVerify(distroData, sourceAddress)
                DataProcessor.processVerifyData()
                │  对比本地数据，缺失的主动拉取
                ▼
```

---

## 七、CP 协议消费方清单

| 消费方 | 模块 | 使用方式 |
|--------|------|---------|
| EmbeddedDumpService | config | getCpProtocol() 写入配置变更 |
| DistributedDatabaseOperateImpl | core | getCpProtocol() 分布式数据库操作 |
| PluginStateProcessor | core | getCpProtocol().addRequestProcessors() 注册自己 |
| RaftPluginStateSynchronizer | core | getCpProtocol() 插件状态同步 |
| InstanceMetadataProcessor | naming | getCpProtocol().addRequestProcessors() 注册自己 |
| ServiceMetadataProcessor | naming | getCpProtocol().addRequestProcessors() 注册自己 |
| NamingMetadataOperateService | naming | getCpProtocol() 元数据写操作 |
| SwitchManager | naming | getCpProtocol().addRequestProcessors() + write() |
| LockOperationServiceImpl | lock | getCpProtocol() 分布式锁操作 |
| ServerStatusManager | naming | getCpProtocol().isReady() 判断集群就绪 |
| CoreOpsControllerV3 | core | getCpProtocol().execute() 运维命令 |

---

## 八、Distro 配置参数速查

| 参数 | 配置键 | 默认值 | 说明 |
|------|--------|--------|------|
| 同步延迟 | `nacos.core.protocol.distro.data.sync.delayMs` | 1000ms | 数据变更后的攒批延迟 |
| 同步超时 | `nacos.core.protocol.distro.data.sync.timeoutMs` | 3000ms | gRPC 同步请求超时 |
| 同步重试间隔 | `nacos.core.protocol.distro.data.sync.retryDelayMs` | 3000ms | 同步失败后重试间隔 |
| 验证间隔 | `nacos.core.protocol.distro.data.verify.intervalMs` | 5000ms | 全量验证执行间隔 |
| 验证超时 | `nacos.core.protocol.distro.data.verify.timeoutMs` | 3000ms | 验证请求超时 |
| 加载重试间隔 | `nacos.core.protocol.distro.data.load.retryDelayMs` | 30000ms | 快照加载失败后重试 |
| 加载超时 | `nacos.core.protocol.distro.data.load.timeoutMs` | 30000ms | 快照加载超时 |

---

## 九、相关文件清单

| 文件路径 | 模块 | 角色 |
|---------|------|------|
| `consistency/.../ConsistencyProtocol.java` | consistency | 协议顶层接口 |
| `consistency/.../cp/CPProtocol.java` | consistency | CP 协议接口（+isLeader） |
| `consistency/.../ap/APProtocol.java` | consistency | AP 协议接口（空，未使用） |
| `core/.../AbstractConsistencyProtocol.java` | core | 抽象基类（processorMap + metaData） |
| `core/.../ProtocolManager.java` | core | 协议管理器（DCL 懒加载 + 成员变更通知） |
| `core/.../ProtocolExecutor.java` | core | 成员变更执行器（CP/AP 各一个单线程池） |
| `core/.../ConsistencyConfiguration.java` | core | SPI 加载 CP 协议（fallback JRaftProtocol） |
| `core/.../distro/DistroProtocol.java` | core | Distro 协议入口（sync/onReceive/onVerify） |
| `core/.../distro/DistroConfig.java` | core | Distro 配置（单例，支持动态刷新） |
| `core/.../distro/DistroConstants.java` | core | Distro 配置键和默认值 |
| `core/.../distro/component/DistroComponentHolder.java` | core | 组件注册表（4 种组件按 type 路由） |
| `core/.../distro/entity/DistroKey.java` | core | Distro 数据标识（resourceKey + type + target） |
| `core/.../distro/entity/DistroData.java` | core | Distro 传输数据载体 |
| `core/.../distro/task/DistroTaskEngineHolder.java` | core | 双层任务引擎持有者 |
| `core/.../distro/task/delay/DistroDelayTaskExecuteEngine.java` | core | 第一级：攒批引擎 |
| `core/.../distro/task/delay/DistroDelayTaskProcessor.java` | core | 攒批后按 action 分发 |
| `core/.../distro/task/execute/DistroSyncChangeTask.java` | core | 第二级：同步变更执行 |
| `core/.../distro/task/execute/DistroSyncDeleteTask.java` | core | 第二级：同步删除执行 |
| `core/.../distro/task/load/DistroLoadDataTask.java` | core | 启动时快照加载 |
| `core/.../distro/task/verify/DistroVerifyTimedTask.java` | core | 定时全量验证 |
| `core/.../raft/JRaftProtocol.java` | core | CP 协议实现（封装 JRaftServer） |
| `core/.../raft/JRaftServer.java` | core | SOFAJRaft 封装（多 RaftGroup） |
| `core/.../raft/RaftConfig.java` | core | Raft 配置（@ConfigurationProperties） |
| `core/.../raft/NacosStateMachine.java` | core | Raft 状态机（apply 日志） |
| `naming/.../DistroClientComponentRegistry.java` | naming | Naming 的 Distro 组件注册 |
| `naming/.../DistroClientDataProcessor.java` | naming | Naming 的 Distro 数据处理器 |
| `naming/.../DistroDataRequestHandler.java` | naming | Distro gRPC 请求接收端 |
