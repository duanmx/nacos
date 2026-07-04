# Nacos 集群成员管理模块深度解析

> **定位**：Nacos 的 Cluster 模块是一个**事件驱动的集群成员发现与状态管理框架**——通过 MemberLookup 策略模式发现成员，通过 gRPC 心跳维持状态，通过 MembersChangeEvent 事件链通知所有下游（一致性协议、分片映射、连接池）集群拓扑变化。

---

## 一、为什么需要集群成员管理？

### 问题场景

Nacos 支持多节点集群部署，节点之间需要知道"谁在集群里、谁健康、谁挂了"：

```
  Node A (8848)        Node B (8849)        Node C (8850)
  ┌──────────┐         ┌──────────┐         ┌──────────┐
  │ 我认识 B、C │  ←──→  │ 我认识 A、C │  ←──→  │ 我认识 A、B │
  └──────────┘         └──────────┘         └──────────┘
```

问题：
- **发现**：三个节点怎么互相知道对方的存在？
- **心跳**：怎么检测某个节点是否健康？
- **通知**：某个节点挂了，一致性协议、分片映射怎么及时知道？
- **通信**：节点之间怎么互相发送数据（配置同步、Distro 协议）？

### 解决方案

**三层抽象**：

1. **成员发现层**（MemberLookup 策略）—— Standalone / FileConfig / AddressServer 三种发现方式
2. **成员管理层**（ServerMemberManager）—— 维护成员列表、心跳上报、状态转换
3. **事件通知层**（MembersChangeEvent 事件链）—— 通知 ProtocolManager / DistroMapper / ClusterRpcClientProxy

---

## 二、整体架构

核心思想：**Lookup 负责"怎么找到成员"，ServerMemberManager 负责"怎么管理成员"，MembersChangeEvent 负责"怎么通知变更"**。

### 2.1 数据流转图（全貌）

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           启动阶段：成员发现                                   │
│                                                                            │
│  ServerMemberManager @Component  L90-L91                                   │
│    │                                                                       │
│    ├─ init()  L153-L175                                                    │
│    │    ├─ 构建 self（本地节点 Member）  L156-L164                           │
│    │    ├─ registerClusterEvent()  L192-L224                                │
│    │    │    └─ NotifyCenter.registerPublisher(MembersChangeEvent)          │
│    │    └─ initAndStartLookup()  L226-L230                                  │
│    │         └─ LookupFactory.createLookUp(this)  L227                     │
│    │              ├─ standalone=true → StandaloneMemberLookup              │
│    │              ├─ cluster.conf 存在 → FileConfigMemberLookup             │
│    │              └─ 默认 → AddressServerMemberLookup                       │
│    │                                                                         │
│    └─ Lookup.start()  L229                                                 │
│         ├─ StandaloneMemberLookup.doStart()  L33-L36                       │
│         │    └─ afterLookup(本地地址) → memberManager.memberChange()        │
│         ├─ FileConfigMemberLookup.doStart()  L56-L67                       │
│         │    ├─ readClusterConfFromDisk()  L79-L91                         │
│         │    │    └─ afterLookup(磁盘读取的成员列表)                         │
│         │    └─ WatchFileCenter.registerWatcher(cluster.conf)  L62         │
│         └─ AddressServerMemberLookup.doStart()  L98-L103                   │
│              ├─ initAddressSys()  → 构建 addressServerUrl                  │
│              ├─ syncFromAddressUrl()  L137-L160（重试 5 次）                 │
│              └─ scheduleByCommon(AddressServerSyncTask, 5s)  L159          │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│                           运行阶段：心跳上报与状态转换                         │
│                                                                            │
│  setSelfReady()  L479-L489 → 调度两个定时任务                               │
│    │                                                                       │
│    ├─ MemberInfoReportTask  L522-L675（每 2s 轮询下一个成员）                │
│    │    ├─ cursor = (cursor+1) % members.size()  L548                      │
│    │    ├─ grpcReportEnabled=true → reportByGrpc(target)  L616-L648        │
│    │    │    └─ ClusterRpcClientProxy.sendRequest(MemberReportRequest)      │
│    │    │         → MemberReportHandler.handle()  L57-L71                  │
│    │    │              └─ memberManager.update(node)  → 更新状态            │
│    │    └─ grpcReportEnabled=false → reportByHttp(target)  L562-L614       │
│    │         └─ POST /cluster/report                                       │
│    │                                                                         │
│    └─ UnhealthyMemberInfoReportTask  L677-L704（每 5s 遍历所有不健康成员）   │
│         └─ 对每个 SUSPICIOUS/DOWN 成员执行 reportByGrpc/Http               │
│                                                                            │
│  状态转换逻辑（MemberUtil）：                                                │
│    ├─ onSuccess()  L131-L159                                               │
│    │    └─ state=UP, failAccessCnt=0, notifyMemberChange()                │
│    └─ onFail()  L167-L197                                                  │
│         └─ state=SUSPICIOUS, failAccessCnt++                                │
│         └─ failAccessCnt > max(3) → state=DOWN, notifyMemberChange()      │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│                           事件通知层：MembersChangeEvent                      │
│                                                                            │
│  触发点：                                                                    │
│    ├─ memberChange()  L357-L416 → NotifyCenter.publishEvent()  L408       │
│    ├─ update()  L250-L276 → notifyMemberChange()  L271                    │
│    └─ MemberUtil.onSuccess/onFail → notifyMemberChange()                  │
│                                                                            │
│  订阅方（extends MemberChangeListener）：                                    │
│    ├─ ClusterRpcClientProxy  L61                                           │
│    │    └─ onEvent()  L244-L253 → refresh() → 创建/销毁 gRPC 客户端        │
│    ├─ ProtocolManager（core/distributed）                                   │
│    │    └─ 更新 Distro / JRaft 协议的集群成员列表                            │
│    ├─ DistroMapper（naming/core）                                           │
│    │    └─ 重新计算服务分片映射（哪些服务归哪个节点管）                        │
│    └─ NacosMaintainerClientHolder（console）                               │
│         └─ 更新 Console 到各节点的连接                                       │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│                           健康检查子模块                                       │
│                                                                            │
│  ModuleHealthCheckerHolder 单例  L29-L31                                    │
│    │                                                                       │
│    ├─ AbstractModuleHealthChecker 构造器自动注册  L26-L28                    │
│    │    ├─ ConfigReadinessCheckService（config 模块）                        │
│    │    └─ NamingReadinessCheckService（naming 模块）                        │
│    │                                                                       │
│    └─ checkReadiness()  L52-L66                                            │
│         └─ 遍历所有 checker，任一 readiness()=false → 返回失败模块名         │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 类关系图（UML）

```
                    ┌──────────────────────────────────┐
                    │       <<interface>>               │
                    │      NacosMemberManager           │
                    │  ─────────────────────────────────│
                    │  + memberChange(Collection)       │
                    │  + allMembers() : Collection      │
                    └──────────────┬───────────────────┘
                                   △  实现
                    ┌──────────────┴───────────────────┐
                    │  @Component                       │
                    │  ServerMemberManager              │
                    │  ─────────────────────────────────│
                    │  - serverList: ConcurrentSkipList │
                    │  - self: Member                   │
                    │  - lookup: MemberLookup           │
                    │  + memberChange()                 │
                    │  + update(Member)                 │
                    │  + setSelfReady()                 │
                    └──────────────┬───────────────────┘
                                   │  持有
                                   ◆
                    ┌──────────────┴───────────────────┐
                    │       <<interface>>               │
                    │         MemberLookup              │
                    │  ─────────────────────────────────│
                    │  + start()                        │
                    │  + afterLookup(Collection)        │
                    │  + useAddressServer() : boolean   │
                    │  + destroy()                      │
                    └──────────────┬───────────────────┘
                                   △  实现
                    ┌──────────────┴───────────────────┐
                    │  <<abstract>>                     │
                    │  AbstractMemberLookup             │
                    │  ─────────────────────────────────│
                    │  # memberManager: NacosMemberMgr  │
                    │  + afterLookup() → memberChange() │
                    │  # doStart()  ← 抽象              │
                    │  # doDestroy()  ← 抽象            │
                    └──────────────┬───────────────────┘
                                   △  继承
              ┌────────────────────┼────────────────────────────┐
              │                    │                            │
┌─────────────┴──────┐ ┌──────────┴───────────┐ ┌──────────────┴──────────┐
│StandaloneMember    │ │FileConfigMember      │ │AddressServerMember      │
│Lookup              │ │Lookup                │ │Lookup                   │
│单机模式             │ │读 cluster.conf       │ │轮询 address server      │
│+ FileWatcher 监听   │ │+ 定时同步 5s          │ │                         │
└────────────────────┘ └──────────────────────┘ └─────────────────────────┘

  ═══════════════════════════════════════════════════════════════════════
  事件通知链路

  ┌─────────────────────────────────┐
  │  <<abstract>>                   │
  │  MemberChangeListener           │
  │  extends Subscriber<Members-    │
  │  ChangeEvent>                   │
  └───────────────┬─────────────────┘
                  △  继承
    ┌─────────────┼──────────────────────────────┐
    │             │                              │
┌───┴──────────────────┐  ┌────────────────────────┐  ┌──────────────────────┐
│ClusterRpcClientProxy │  │ProtocolManager         │  │DistroMapper          │
│(core/cluster/remote) │  │(core/distributed)      │  │(naming/core)         │
│管理节点间 gRPC 连接    │  │更新 Distro/JRaft 成员   │  │重新计算服务分片        │
└──────────────────────┘  └────────────────────────┘  └──────────────────────┘

  ═══════════════════════════════════════════════════════════════════════
  健康检查子模块

  ┌───────────────────────────────┐
  │  <<singleton>>                │
  │  ModuleHealthCheckerHolder    │
  │  ──────────────────────────── │
  │  + getInstance()              │
  │  + registerChecker()          │
  │  + checkReadiness() : Result  │
  └──────────────┬────────────────┘
                 │  持有
                 ◆
  ┌──────────────┴────────────────┐
  │  <<abstract>>                 │
  │  AbstractModuleHealthChecker  │
  │  ──────────────────────────── │
  │  + readiness() : boolean      │
  │  + getModuleName() : String   │
  └──────────────┬────────────────┘
                 △  继承
       ┌─────────┴──────────────┐
       │                        │
┌──────┴────────────────┐ ┌─────┴────────────────┐
│ConfigReadinessCheck   │ │NamingReadinessCheck  │
│Service (config)       │ │Service (naming)      │
└───────────────────────┘ └──────────────────────┘

  ═══════════════════════════════════════════════════════════════════════
  工厂 + 工具类

  ┌─────────────────────────────────┐
  │  <<static>>                     │
  │  LookupFactory                  │
  │  ────────────────────────────── │
  │  + createLookUp(ServerMemberMgr)│
  │  + switchLookup(name, mgr)      │
  │  + destroy()                    │
  │  enum LookupType {              │
  │    FILE_CONFIG, ADDRESS_SERVER  │
  │  }                              │
  └─────────────────────────────────┘

  ┌─────────────────────────────────┐
  │  <<static>>                     │
  │  MemberUtil                     │
  │  ────────────────────────────── │
  │  + onSuccess(manager, member)   │
  │  + onFail(manager, member, ex)  │
  │  + syncToFile(members)          │
  │  + readServerConf(list)         │
  │  + singleParse(address)         │
  └─────────────────────────────────┘

  ═══════════════════════════════════════════════════════════════════════
  图例：
    ────▶  委托/调用
    ──△    继承/实现（子类指向父类）
    ◆      组合/持有
    <<X>>  构造型标注
```

### 2.3 用了哪些设计模式？为什么这么设计？

| 模式 | 体现在哪 | 解决什么问题 | 好处 | 坏处 |
|------|---------|-------------|------|------|
| **策略模式** | `MemberLookup` 接口 + 三种实现（Standalone / FileConfig / AddressServer），由 `LookupFactory` 根据环境选择 | 不同部署方式（单机/文件/地址服务器）需要不同的成员发现机制 | 新增发现方式只需加一个 Lookup 子类，不改 Manager | Lookup 切换需要重启或手动调 `switchLookup()`，不支持自动降级 |
| **模板方法** | `AbstractMemberLookup.start()/destroy()` 封装 `AtomicBoolean` 状态守卫，子类只实现 `doStart()/doDestroy()`；`Task.run()` 封装 `shutdown` 守卫 + 异常处理，子类只实现 `executeBody()` | 生命周期管理（防重复启动/销毁）和异常隔离需要统一处理 | 子类代码简洁，无需关心并发状态管理 | `AbstractMemberLookup.start` 用 `AtomicBoolean` 而非 `synchronized`，极端并发下理论上有竞态窗口 |
| **观察者模式** | `ServerMemberManager.notifyMemberChange()` 发布 `MembersChangeEvent`，四个 `MemberChangeListener` 订阅（ClusterRpcClientProxy / ProtocolManager / DistroMapper / NacosMaintainerClientHolder） | 集群成员变更时，一致性协议、分片映射、连接池都需要感知变化 | 解耦：Manager 不知道谁关心变更，新增订阅方只需继承 `MemberChangeListener` | 事件是异步的，订阅方处理延迟可能导致短暂不一致 |
| **工厂模式** | `LookupFactory.createLookUp()` 根据配置和环境自动选择 `LookupType`，`switchLookup()` 支持运行时切换 | 创建 Lookup 的逻辑需要集中管理，避免散落在各处 | 切换寻址模式只需一行 API 调用 | `LOOK_UP` 是静态变量，不支持多实例，全局共享同一个 Lookup |
| **自注册（构造器注入）** | `AbstractModuleHealthChecker` 构造器自动调 `ModuleHealthCheckerHolder.registerChecker(this)` | HealthChecker 分散在不同模块（config/naming），需要自动收集 | 新增模块只需继承抽象类，无需手动注册 | 依赖构造器调用时机，如果模块未加载则不会注册 |

### 2.4 整体设计的优点

- **发现方式可插拔**：三种 MemberLookup 策略对应三种部署场景，通过 `LookupFactory` 自动选择，运维无感
- **心跳机制双通道**：gRPC 优先、HTTP 兜底，自动降级（`grpcReportEnabled` 标记），兼容新旧版本
- **状态机三态**：`UP → SUSPICIOUS → DOWN` 渐进式降级，避免网络抖动导致误判
- **事件驱动解耦**：成员变更通过 `MembersChangeEvent` 广播，Manager 与下游完全解耦
- **健康检查可扩展**：`AbstractModuleHealthChecker` 自注册，config/naming 模块各自提供 readiness 检查

### 2.5 潜在问题

- **Lookup 全局单例**：`LookupFactory.LOOK_UP` 是静态变量，不支持多个 `ServerMemberManager` 实例各自持有不同 Lookup
- **心跳间隔固定**：`MemberInfoReportTask` 每 2s 一次、`UnhealthyMemberInfoReportTask` 每 5s 一次，不可配置，大规模集群可能产生过多心跳流量
- **事件通知无重试**：`NotifyCenter.publishEvent()` 是异步投递，如果某个订阅方处理失败不会重试
- **FileConfigMemberLookup 的文件监听依赖 OS**：`WatchFileCenter` 底层依赖 inotify/kqueue，某些文件系统（如 NFS）可能不触发变更事件

---

## 三、核心概念

### 3.1 三种寻址模式（LookupType）

| 模式 | 触发条件 | 实现类 | 工作原理 |
|------|---------|-------|---------|
| `Standalone` | `nacos.standalone=true` | `StandaloneMemberLookup` | 只有本机一个节点，直接 `afterLookup(本地地址)` |
| `FILE_CONFIG` | `cluster.conf` 文件存在 或 `nacos.core.member-list` 配置不为空 | `FileConfigMemberLookup` | 读取文件 + 注册 `FileWatcher` 监听文件变更 |
| `ADDRESS_SERVER` | 默认（文件不存在且非单机） | `AddressServerMemberLookup` | 轮询 `http://jmenv.tbsite.net:8080/serverlist` 获取成员列表 |

选择逻辑（`LookupFactory.chooseLookup()` L124-L136）：
```
配置了 nacos.core.member.lookup.type → 用配置的值
否则：
  cluster.conf 存在 或 member-list 不为空 → FILE_CONFIG
  否则 → ADDRESS_SERVER
```

### 3.2 成员状态机（NodeState）

```
       心跳成功
    ┌─────────────┐
    │             ▼
  STARTING ──→ UP ←─── 心跳成功
    │          │
    │          │ 心跳失败（failAccessCnt++）
    │          ▼
    │     SUSPICIOUS
    │          │
    │          │ failAccessCnt > 3 或 Connection Refused
    │          ▼
    └──────→ DOWN
```

- `UP`：节点健康，可以处理请求
- `SUSPICIOUS`：心跳失败但还在重试窗口内（failAccessCnt ≤ 3）
- `DOWN`：节点不可达，从健康列表中移除

### 3.3 Member 数据模型

```java
Member extends NacosMember {
    String ip;                    // 节点 IP
    int port;                     // 节点端口（默认 8848）
    String address;               // ip:port
    NodeState state;              // UP / SUSPICIOUS / DOWN / STARTING
    ServerAbilities abilities;    // 节点能力（通过能力协商获得）
    Map<String, Object> extendInfo;  // 扩展信息
    int failAccessCnt;            // 连续失败次数（transient，不序列化）
    boolean grpcReportEnabled;    // 是否用 gRPC 上报（兼容 2.3 以下版本）
}
```

扩展信息中的关键 metadata：

| 常量 | 值 | 含义 |
|------|-----|------|
| `VERSION` | `"version"` | Nacos 版本号，用于升级兼容判断 |
| `RAFT_PORT` | `"raftPort"` | JRaft 端口（默认 主端口-1000） |
| `SUPPORT_GRAY_MODEL` | `"supportGrayModel"` | 是否支持灰度升级 |
| `LAST_REFRESH_TIME` | `"lastRefreshTime"` | 最后刷新时间戳 |

### 3.4 心跳上报机制

两种上报任务（`MemberInfoReportTask` + `UnhealthyMemberInfoReportTask`）：

| 任务 | 调度间隔 | 目标 | 行为 |
|------|---------|------|------|
| `MemberInfoReportTask` | 2s | 轮询下一个成员（cursor 递增） | 只上报一个节点，gRPC 优先 |
| `UnhealthyMemberInfoReportTask` | 5s | 遍历所有不健康成员 | 对每个 SUSPICIOUS/DOWN 节点上报 |

两种上报通道：

| 通道 | 条件 | 实现 |
|------|------|------|
| gRPC | `target.grpcReportEnabled = true` | `ClusterRpcClientProxy.sendRequest(MemberReportRequest)` |
| HTTP | gRPC 不可用（旧版本） | `POST /nacos/v1/core/cluster/report` |

gRPC 上报失败时的降级逻辑（`MemberInfoReportTask.reportByGrpc()` L616-L648）：
```
NO_HANDLER 异常 → target.grpcReportEnabled = false → 下次自动切换 HTTP
```

---

## 四、数据流转全景

```
┌──────────────────── 启动阶段 ────────────────────┐
│                                                    │
│  ServerMemberManager() 构造器  L148-L151           │
│    └─ init()  L153-L175                            │
│         ├─ self = MemberUtil.singleParse(ip:port)  │
│         ├─ self.setAbilities(initMemberAbilities())│
│         ├─ serverList.put(self.address, self)      │
│         ├─ registerClusterEvent()                  │
│         │    └─ NotifyCenter.registerPublisher     │
│         │         (MembersChangeEvent, queue=128)  │
│         └─ initAndStartLookup()                    │
│              └─ LookupFactory.createLookUp(this)   │
│                   └─ Lookup.start() → doStart()    │
│                        └─ afterLookup(members)     │
│                             └─ memberManager       │
│                                  .memberChange()   │
│                                                    │
├──────────────────── 就绪阶段 ─────────────────────┤
│                                                    │
│  setSelfReady(port)  L479-L489                     │
│    ├─ self.state = UP                              │
│    ├─ schedule(MemberInfoReportTask, 2s)           │
│    └─ schedule(UnhealthyMemberInfoReportTask, 5s)  │
│                                                    │
├──────────────────── 心跳循环 ─────────────────────┤
│                                                    │
│  MemberInfoReportTask.executeBody()  L535-L560     │
│    ├─ cursor = (cursor+1) % members.size()         │
│    ├─ target = members.get(cursor)                 │
│    ├─ reportByGrpc(target)  L616-L648              │
│    │    └─ ClusterRpcClientProxy                   │
│    │         .sendRequest(MemberReportRequest)     │
│    │         → 对端 MemberReportHandler            │
│    │              .handle()  L57-L71               │
│    │              └─ memberManager.update(node)    │
│    │                   └─ 状态变更 →               │
│    │                        notifyMemberChange()   │
│    └─ after() → schedule(this, 2s)  L651-L653     │
│                                                    │
├──────────────────── 事件广播 ─────────────────────┤
│                                                    │
│  notifyMemberChange()  L278-L281                   │
│    └─ NotifyCenter.publishEvent(                   │
│         MembersChangeEvent)                        │
│         ├─ ClusterRpcClientProxy.onEvent()         │
│         │    └─ refresh() → 创建/销毁 gRPC 客户端   │
│         ├─ ProtocolManager.onEvent()               │
│         │    └─ 更新 Distro/JRaft 成员列表          │
│         ├─ DistroMapper.onEvent()                  │
│         │    └─ 重新计算服务分片映射                  │
│         └─ NacosMaintainerClientHolder.onEvent()   │
│              └─ 更新 Console 连接                   │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## 五、核心类详解

### 5.1 ServerMemberManager

**定位**：集群成员管理的唯一实现（`@Component`），管理成员列表的增删改查、心跳上报、状态转换。

**核心数据结构**（[源码 L111-L146](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L111-L146)）：

| 字段 | 类型 | 用途 |
|------|------|------|
| `serverList` | `ConcurrentSkipListMap<String, Member>` | 集群成员字典（key=address） |
| `self` | `Member` | 本地节点信息 |
| `memberAddressInfos` | `Set<String>` | 当前 UP 状态的成员地址集合 |
| `lookup` | `MemberLookup` | 当前使用的寻址策略 |
| `infoReportTask` | `MemberInfoReportTask` | 心跳上报定时任务 |

**关键方法**：

| 方法 | 行号 | 职责 |
|------|------|------|
| `init()` | L153-L175 | 初始化本地节点、注册事件、启动寻址 |
| `memberChange()` | L357-L416 | 比较新旧成员列表，有变化则发布事件+持久化 |
| `update()` | L250-L276 | 更新单个成员信息，基本信息变化则发事件 |
| `setSelfReady()` | L479-L489 | 标记本节点就绪，启动心跳定时任务 |
| `memberJoin/Leave()` | L424-L440 | 成员加入/离开（委托给 memberChange） |

### 5.2 LookupFactory

**定位**：静态工厂，根据配置和环境创建 `MemberLookup` 实例。

**选择逻辑**（[源码 L124-L136](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/LookupFactory.java#L124-L136)）：

```java
private static LookupType chooseLookup(String lookupType) {
    if (StringUtils.isNotBlank(lookupType)) {
        return LookupType.sourceOf(lookupType);  // 显式配置
    }
    if (cluster.conf 存在 || memberList 不为空) {
        return LookupType.FILE_CONFIG;           // 文件模式
    }
    return LookupType.ADDRESS_SERVER;            // 地址服务器
}
```

**运行时切换**（[源码 L85-L109](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/LookupFactory.java#L85-L109)）：

`switchLookup()` 支持运行时更换寻址模式，会先 `destroy()` 旧 Lookup 再 `start()` 新 Lookup。

### 5.3 AbstractMemberLookup

**定位**：寻址策略的模板方法基类，封装生命周期管理（`AtomicBoolean` 防重复启动/销毁）。

**模板方法骨架**（[源码 L48-L60](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/AbstractMemberLookup.java#L48-L60)）：

```java
@Override
public void start() throws NacosException {
    if (start.compareAndSet(false, true)) {   // 防重复启动
        doStart();                             // 子类实现
    }
}

@Override
public void afterLookup(Collection<Member> members) {
    this.memberManager.memberChange(members);  // 统一回调到 Manager
}
```

### 5.4 ClusterRpcClientProxy

**定位**：集群间 gRPC 通信代理，继承 `MemberChangeListener` 监听成员变更事件。

**核心机制**（[源码 L61-L88](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/ClusterRpcClientProxy.java#L61-L88)）：

```java
@Component
public class ClusterRpcClientProxy extends MemberChangeListener {
    
    @PostConstruct
    public void init() {
        NotifyCenter.registerSubscriber(this);     // 订阅 MembersChangeEvent
        refresh(serverMemberManager.allMembersWithoutSelf());
    }
}
```

**refresh() 逻辑**（[源码 L95-L120](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/ClusterRpcClientProxy.java#L95-L120)）：
- 新成员：创建 gRPC 客户端并连接（`createRpcClientAndStart()`）
- 已离开成员：销毁 gRPC 客户端（`RpcClientFactory.destroyClient()`）

### 5.5 MemberReportHandler

**定位**：gRPC 心跳接收端，处理 `MemberReportRequest`，更新本地成员列表。

**处理逻辑**（[源码 L55-L71](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/MemberReportHandler.java#L55-L71)）：

```java
@Secured(resource = "report", signType = SignType.SPECIFIED, apiType = ApiType.INNER_API)
public MemberReportResponse handle(MemberReportRequest request, RequestMeta meta) {
    Member node = request.getNode();
    node.setState(NodeState.UP);
    node.setFailAccessCnt(0);
    memberManager.update(node);                          // 更新本地成员信息
    return new MemberReportResponse(memberManager.getSelf()); // 返回自己的信息
}
```

### 5.6 ModuleHealthCheckerHolder

**定位**：模块就绪检查的注册表（单例），收集各模块的 readiness 检查结果。

**自注册机制**（[源码 AbstractModuleHealthChecker L26-L28](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/health/AbstractModuleHealthChecker.java#L26-L28)）：

```java
protected AbstractModuleHealthChecker() {
    ModuleHealthCheckerHolder.getInstance().registerChecker(this);
}
```

**检查逻辑**（[源码 L52-L66](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/health/ModuleHealthCheckerHolder.java#L52-L66)）：

遍历所有注册的 Checker，任一 `readiness()=false` 则返回失败模块名。

---

## 六、扩展点

### 6.1 MemberLookup（寻址策略扩展）

新增寻址方式只需：
1. 继承 `AbstractMemberLookup`，实现 `doStart()` 和 `doDestroy()`
2. 在 `LookupFactory.find()` 中增加新的 `LookupType`
3. 配置 `nacos.core.member.lookup.type=新类型名`

### 6.2 MemberChangeListener（事件订阅扩展）

新增集群变更监听方只需：
1. 继承 `MemberChangeListener`
2. 实现 `onEvent(MembersChangeEvent event)`
3. 注册到 `NotifyCenter`（继承即自动注册）

当前四个订阅方：

| 订阅方 | 模块 | 收到事件后做什么 |
|--------|------|----------------|
| `ClusterRpcClientProxy` | core | 创建/销毁节点间 gRPC 连接 |
| `ProtocolManager` | core | 更新 Distro / JRaft 协议的成员列表 |
| `DistroMapper` | naming | 重新计算服务分片映射 |
| `NacosMaintainerClientHolder` | console | 更新 Console 到各节点的连接 |

### 6.3 AbstractModuleHealthChecker（健康检查扩展）

新增模块就绪检查只需：
1. 继承 `AbstractModuleHealthChecker`
2. 实现 `readiness()` 和 `getModuleName()`
3. 构造器自动注册到 `ModuleHealthCheckerHolder`

---

## 七、关键设计决策

1. **LookupFactory 集中创建**：将"选哪种寻址方式"的决策集中在工厂类中，避免 ServerMemberManager 内部大量 if-else
2. **gRPC 心跳优先 + HTTP 兜底**：v2.3+ 全部走 gRPC 上报，但保留 HTTP 降级路径兼容旧版本；`NO_HANDLER` 异常时自动回退
3. **MembersChangeEvent 广播解耦**：Manager 只负责发布事件，不关心谁订阅——ProtocolManager、DistroMapper、ClusterRpcClientProxy 各自独立处理
4. **三态状态机（UP → SUSPICIOUS → DOWN）**：避免单次心跳失败导致节点被误判为 DOWN，连续失败超过阈值才降级
5. **AbstractMemberLookup 模板方法**：用 `AtomicBoolean` 封装启动/销毁的幂等性，子类只需关注业务逻辑
6. **AbstractModuleHealthChecker 自注册**：模块各自提供 readiness 检查，core 模块不需要知道有哪些业务模块

---

## 八、相关文件清单

| 文件 | 模块 | 角色 |
|------|------|------|
| `NacosMemberManager.java` | core | 成员管理接口 |
| `ServerMemberManager.java` | core | 成员管理唯一实现（@Component） |
| `Member.java` | core | 集群成员数据模型 |
| `MembersChangeEvent.java` | core | 集群变更事件 |
| `MemberChangeListener.java` | core | 集群变更监听器抽象基类 |
| `MemberUtil.java` | core | 成员工具类（解析、状态转换、持久化） |
| `MemberMetaDataConstants.java` | core | 成员元数据常量定义 |
| `Task.java` | core | 定时任务抽象基类 |
| `MemberLookup.java` | core/cluster/lookup | 寻址策略接口 |
| `AbstractMemberLookup.java` | core/cluster/lookup | 寻址策略模板方法基类 |
| `StandaloneMemberLookup.java` | core/cluster/lookup | 单机模式寻址 |
| `FileConfigMemberLookup.java` | core/cluster/lookup | cluster.conf 文件寻址 |
| `AddressServerMemberLookup.java` | core/cluster/lookup | 地址服务器寻址 |
| `LookupFactory.java` | core/cluster/lookup | 寻址策略工厂 |
| `ClusterRpcClientProxy.java` | core/cluster/remote | 集群间 gRPC 通信代理 |
| `MemberReportHandler.java` | core/cluster/remote | gRPC 心跳接收端 |
| `MemberReportRequest.java` | core/cluster/remote/request | 心跳上报请求 |
| `MemberReportResponse.java` | core/cluster/remote/response | 心跳上报响应 |
| `PluginAvailabilityRequest.java` | core/cluster/remote/request | 插件可用性查询请求 |
| `PluginAvailabilityRequestHandler.java` | core/cluster/remote/request | 插件可用性查询处理 |
| `AbstractModuleHealthChecker.java` | core/cluster/health | 模块健康检查抽象基类 |
| `ModuleHealthCheckerHolder.java` | core/cluster/health | 健康检查注册表（单例） |
| `ReadinessResult.java` | core/cluster/health | 就绪检查结果 |
| `ConfigReadinessCheckService.java` | config | Config 模块就绪检查 |
| `NamingReadinessCheckService.java` | naming | Naming 模块就绪检查 |
