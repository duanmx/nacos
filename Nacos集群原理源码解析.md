# Nacos 集群原理源码解析

> **原则声明**：本文只讲 Nacos 3.x 服务端**集群（cluster）**的工作原理——即"多个 Nacos 节点如何互相发现、如何感知彼此存活、如何通信、数据如何在节点间达成一致"。所有结论都落到具体类、方法、行号（`file:///` 可点击跳转），不臆测。核心代码在 `core` 模块的 `com.alibaba.nacos.core.cluster` 与 `com.alibaba.nacos.core.distributed` 包下。

Nacos 集群的核心可以拆成**四层能力**，本文按这个顺序展开：

1. **成员发现（MemberLookup）**：我怎么知道集群里有哪些节点？
2. **成员管理（ServerMemberManager）**：节点列表怎么维护、变更怎么传播？
3. **成员健康观测（上报任务 + 状态机）**：怎么知道某个节点还活着？
4. **一致性协议（ProtocolManager：AP=Distro / CP=Raft）**：数据怎么在节点间达成一致？

---

## 目录

1. [为什么需要集群](#1-为什么需要集群)
2. [整体架构分层图](#2-整体架构分层图)
3. [成员模型：Member 与 NodeState](#3-成员模型member-与-nodestate)
4. [成员发现：三种寻址模式](#4-成员发现三种寻址模式)
5. [成员管理：ServerMemberManager](#5-成员管理servermembermanager)
6. [成员健康观测：上报任务与状态机](#6-成员健康观测上报任务与状态机)
7. [节点间通信：ClusterRpcClientProxy](#7-节点间通信clusterrpcclientproxy)
8. [一致性协议接入：AP 与 CP](#8-一致性协议接入ap-与-cp)
9. [关键设计决策](#9-关键设计决策)
10. [完整数据流转时间线](#10-完整数据流转时间线)
11. [总结](#11-总结)
12. [文件索引表](#12-文件索引表)

---

## 1. 为什么需要集群

单机 Nacos 有两个致命问题：**单点故障**（挂了服务发现/配置全断）和**容量瓶颈**（连接数、数据量受限于一台机器）。集群通过多节点解决，但随之带来三个必须回答的问题：

- **节点怎么找到彼此**？—— 成员发现（Lookup）。
- **一个节点挂了，其他节点怎么知道**？—— 健康观测。
- **数据写到节点 A，节点 B 怎么读到**？—— 一致性协议。

Nacos 的巧妙之处在于：它把"**集群成员管理**"和"**数据一致性**"**彻底解耦**成两层。成员层（`ServerMemberManager`）只负责"有哪些节点、谁死谁活"；一致性层（Distro/Raft）负责"数据怎么同步"。成员层的变更通过 `MembersChangeEvent` 事件驱动一致性层更新拓扑——两层通过事件松耦合。

---

## 2. 整体架构分层图

```
                          ┌─────────────────────────────────────┐
                          │      成员发现层 MemberLookup           │
                          │  ┌─────────────┬──────────────┬─────┐│
   cluster.conf ─────────▶│  │FileConfig   │AddressServer │Stand ││
   地址服务器 HTTP ────────▶│  │(文件+inotify)│(HTTP轮询)     │alone ││
                          │  └──────┬──────┴──────┬───────┴─────┘│
                          └─────────┼─────────────┼──────────────┘
                                    │ afterLookup(members)
                                    ▼
                          ┌─────────────────────────────────────┐
                          │   成员管理层 ServerMemberManager       │
                          │   serverList: ConcurrentSkipListMap   │
                          │   memberChange() → syncToFile         │
                          │           │ 发 MembersChangeEvent      │
                          └───────────┼─────────────────────────┬─┘
              ┌───────────────────────┼──────────┐              │
              ▼                       ▼           ▼              ▼
   ┌──────────────────┐  ┌──────────────────┐ ┌────────────────────────┐
   │ 健康观测           │  │ 节点通信           │ │  一致性协议 ProtocolManager│
   │ MemberInfoReport  │  │ ClusterRpcClient  │ │  ┌──────────┬─────────┐ │
   │ Task（gRPC上报）   │  │ Proxy（每节点一个  │ │  │AP=Distro │CP=Raft  │ │
   │ 状态机:UP→        │  │ gRPC 长连接）      │ │  │临时实例   │配置/持久 │ │
   │ SUSPICIOUS→DOWN   │  │                   │ │  │(第06篇)  │实例/元数据│ │
   └──────────────────┘  └──────────────────┘ │  └──────────┴─────────┘ │
                                               └────────────────────────┘
```

关键：**Lookup 喂数据 → MemberManager 维护列表并发事件 → 健康观测/节点通信/一致性协议三者都订阅这个事件**。

---

## 3. 成员模型：Member 与 NodeState

一个集群节点用 [Member](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/Member.java) 表示，它 `extends NacosMember implements Comparable, Serializable`（[L41](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/Member.java#L41)）：

```java
public class Member extends NacosMember implements Comparable<Member>, Cloneable, Serializable {
    private transient int failAccessCnt = 0;               // 连续访问失败计数，健康状态机用
    ...
    @Override
    public int compareTo(Member o) {
        return getAddress().compareTo(o.getAddress());     // 按 ip:port 排序 → 决定 firstIp
    }
}
```

核心字段（父类 `NacosMember`）：`ip`、`port`、`address`（=`ip:port`，节点唯一标识）、`state`（`NodeState`）、`abilities`（节点能力，如是否支持 gRPC 上报）、`extendInfo`（扩展元数据，含 `raftPort`、`version`、`weight` 等）。

节点状态机 `NodeState` 有四态：`UP`（健康）、`SUSPICIOUS`（可疑，访问失败但未达阈值）、`DOWN`（下线）、`STARTING`。状态流转见 §6。

一个细节：`Member` 实现 `Comparable` 按 `address` 字典序排序，配合 `serverList` 用 `ConcurrentSkipListMap`（有序），使得 [isFirstIp](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L475-L477) 能取 `firstKey()` 确定性地选出"第一个节点"——JRaft 集群初始化时由它发起（见 `JRaftUtils.joinCluster` 的 `isFirstIp()` 判断）。

---

## 4. 成员发现：三种寻址模式

节点启动时得先知道"集群里有谁"。抽象接口是 [MemberLookup](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/MemberLookup.java#L30-L76)，核心方法 `afterLookup(Collection<Member>)`——寻址到成员后回调给 `ServerMemberManager`。选哪种由 [LookupFactory.chooseLookup](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/LookupFactory.java#L39-L51) 决定：

```java
private static LookupType chooseLookup(String lookupType) {
    if (StringUtils.isNotBlank(lookupType)) {              // ① 显式配置优先
        LookupType type = LookupType.sourceOf(lookupType);
        if (Objects.nonNull(type)) {
            return type;
        }
    }
    File file = new File(EnvUtil.getClusterConfFilePath());
    if (file.exists() || StringUtils.isNotBlank(EnvUtil.getMemberList())) {
        return LookupType.FILE_CONFIG;                     // ② cluster.conf 存在或配了 member-list
    }
    return LookupType.ADDRESS_SERVER;                      // ③ 兜底：地址服务器
}
```

### 4.1 FileConfigMemberLookup（文件寻址，最常用）

从 `${nacos.home}/conf/cluster.conf` 读取节点列表（每行一个 `ip:port`），见 [FileConfigMemberLookup](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/FileConfigMemberLookup.java#L55-L67)：

```java
@Override
public void doStart() throws NacosException {
    readClusterConfFromDisk();                             // 读文件 → afterLookup
    // inotify 监听 cluster.conf 变化，改了自动重载
    WatchFileCenter.registerWatcher(EnvUtil.getConfPath(), watcher);
}
```

亮点：用 `WatchFileCenter`（inotify）监控文件，**运维改了 `cluster.conf` 无需重启**，自动触发 `readClusterConfFromDisk` → `memberChange`。

### 4.2 AddressServerMemberLookup（地址服务器寻址）

从外部地址服务器 HTTP 拉取节点列表，适合大规模/容器化动态扩缩容。见 [AddressServerMemberLookup](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/AddressServerMemberLookup.java#L45-L82)：
- 启动时 HTTP GET `http://{domain}:{port}/{context}/serverlist`，失败重试（默认 5 次）。
- 之后每 5 秒（`DEFAULT_SYNC_TASK_DELAY_MS`）定时同步一次（`AddressServerSyncTask`）。
- 连续失败 12 次（`maxFailCount`）标记 `isAddressServerHealth = false`，但**不会凭空构造成员**。

### 4.3 StandaloneMemberLookup（单机）

单机模式专用，成员集合只有自己，见 [StandaloneMemberLookup.doStart](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/StandaloneMemberLookup.java#L2-L6)：只把本机地址 `afterLookup`。

---

## 5. 成员管理：ServerMemberManager

[ServerMemberManager](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java) 是集群成员的**唯一权威视图**。核心字段（[L108-146](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L108-L146)）：

```java
private volatile ConcurrentSkipListMap<String, Member> serverList;  // 全部节点(address→Member)，有序
private volatile Member self;                                       // 本机节点
private volatile Set<String> memberAddressInfos;                    // 仅 UP 状态的地址集合
private final MemberInfoReportTask infoReportTask;                  // 广播本机元数据
private final UnhealthyMemberInfoReportTask unhealthyMemberInfoReportTask; // 专攻非 UP 节点
```

### 5.1 初始化

构造器调 [init()](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L153-L175)：

```java
protected void init() throws NacosException {
    this.port = EnvUtil.getProperty(SERVER_PORT_PROPERTY, Integer.class, DEFAULT_SERVER_PORT);
    this.localAddress = InetUtils.getSelfIP() + ":" + port;
    this.self = MemberUtil.singleParse(this.localAddress);       // 构造本机 Member
    this.self.setExtendVal(MemberMetaDataConstants.VERSION, VersionUtils.version);
    this.self.setAbilities(initMemberAbilities());
    serverList.put(self.getAddress(), self);                     // 先把自己放进去
    registerClusterEvent();                                      // 注册 MembersChangeEvent 发布者
    initAndStartLookup();                                        // 启动寻址（第4节）
}
```

### 5.2 成员变更的核心：memberChange

所有寻址结果、节点增删最终都汇聚到 [memberChange](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L356-L416)（`synchronized` 保证顺序）：

```java
@Override
public synchronized boolean memberChange(Collection<Member> members) {
    if (members == null || members.isEmpty()) {
        return false;                                           // ① 空列表不替换（防误清空）
    }
    boolean isContainSelfIp = members.stream()
        .anyMatch(m -> Objects.equals(localAddress, m.getAddress()));
    if (!isContainSelfIp) {
        members.add(this.self);                                 // ② 本机必须始终在列表里
    }
    boolean hasChange = members.size() != serverList.size();
    ConcurrentSkipListMap<String, Member> tmpMap = new ConcurrentSkipListMap<>();
    Set<String> tmpAddressInfo = new ConcurrentHashSet<>();
    for (Member member : members) {
        Member existMember = serverList.get(member.getAddress());
        if (existMember == null) {
            hasChange = true;
            tmpMap.put(member.getAddress(), member);
        } else {
            tmpMap.put(member.getAddress(), existMember);       // ③ 保留已有节点的动态元数据/能力
        }
        if (NodeState.UP.equals(member.getState())) {
            tmpAddressInfo.add(member.getAddress());
        }
    }
    serverList = tmpMap;
    memberAddressInfos = tmpAddressInfo;
    if (hasChange) {
        MemberUtil.syncToFile(allMembers());                   // ④ 持久化到 cluster.conf
        NotifyCenter.publishEvent(                              // ⑤ 发事件，驱动一致性层/通信层
            MembersChangeEvent.builder().members(allMembers()).build());
    }
    return hasChange;
}
```

五个关键设计（对应注释①-⑤）：
1. **空列表短路**：地址服务器临时返回空，不清空现有成员，防止误判集群全灭。
2. **本机保底**：`self` 永远在列表里，即使寻址结果漏了自己。
3. **保留动态元数据**：已存在的节点保留其 `existMember`（含运行时上报的 abilities/extendInfo），只有新节点才用新对象。
4. **回写文件**：拓扑变化同步到 `cluster.conf`，保持与文件寻址兼容。
5. **事件驱动**：只有真变化（`hasChange`）才发 `MembersChangeEvent`，且放在 `synchronized` 块内保证事件顺序。

`memberJoin`（[L424](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L424-L428)）和 `memberLeave`（[L436](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L436-L440)）都是便捷方法，最终都转成完整列表调 `memberChange`。

---

## 6. 成员健康观测：上报任务与状态机

Nacos 的节点健康是**去中心化的尽力而为上报（gossip 式）**，没有中心裁判。本机准备好后（[setSelfReady](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L479-L489)）启动两个任务：

```java
public void setSelfReady(int port) {
    getSelf().setState(NodeState.UP);
    if (!EnvUtil.getStandaloneMode()) {
        GlobalExecutor.scheduleByCommon(this.infoReportTask, DEFAULT_TASK_DELAY_TIME);
        GlobalExecutor.scheduleByCommon(this.unhealthyMemberInfoReportTask, DEFAULT_TASK_DELAY_TIME);
    }
}
```

### 6.1 常规上报：MemberInfoReportTask

[MemberInfoReportTask](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L548-L560) 用 `cursor` **轮询**，每次只向一个 peer 上报本机元数据：

```java
this.cursor = (this.cursor + 1) % members.size();
Member target = members.get(cursor);
if (target.getAbilities().getRemoteAbility().isGrpcReportEnabled() || target.isGrpcReportEnabled()) {
    reportByGrpc(target);                                  // 2.3+ 默认 gRPC
} else {
    reportByHttp(target);                                  // 混合版本兼容
}
```

`reportByGrpc`（[L616-648](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L616-L648)）发 `MemberReportRequest(self)`，对端 [MemberReportHandler.handle](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/MemberReportHandler.java#L56-L71) 收到后把发送方标记 `UP` 并回自己的信息——**一次上报，双向刷新**。任务跑完 `after()` 里 2 秒后再排（[L650-653](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L650-L653)）。

### 6.2 状态机：UP → SUSPICIOUS → DOWN

上报成功/失败驱动状态流转，见 [MemberUtil](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/MemberUtil.java#L131-L197)：

```java
// 成功：直接 UP，失败计数清零
public static void onSuccess(ServerMemberManager manager, Member member) {
    manager.getMemberAddressInfos().add(member.getAddress());
    member.setState(NodeState.UP);
    member.setFailAccessCnt(0);
    if (状态变了) { manager.notifyMemberChange(member); }
}

// 失败：先 SUSPICIOUS，失败累加，超阈值或连接被拒 → DOWN
public static void onFail(ServerMemberManager manager, Member member, Throwable ex) {
    manager.getMemberAddressInfos().remove(member.getAddress());
    member.setState(NodeState.SUSPICIOUS);
    member.setFailAccessCnt(member.getFailAccessCnt() + 1);
    if (member.getFailAccessCnt() > maxFailAccessCnt
            || StringUtils.containsIgnoreCase(ex.getMessage(), "Connection refused")) {
        member.setState(NodeState.DOWN);
    }
    if (状态变了) { manager.notifyMemberChange(member); }
}
```

两个要点：
- **SUSPICIOUS 缓冲**：单次失败不立刻判死，先置 `SUSPICIOUS`，累计超阈值才 `DOWN`——容忍网络抖动。
- **快速失败例外**：若错误是 "Connection refused"（进程明显没了），直接 `DOWN`，不等阈值。
- 只有状态**真变化**才 `notifyMemberChange` 发事件，避免无效广播。

### 6.3 专项重试：UnhealthyMemberInfoReportTask

[UnhealthyMemberInfoReportTask](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L677-L704) 独立地、更频繁地（5 秒）只向**非 UP 节点**上报，让掉线节点恢复后能被快速重新发现：

```java
for (Member member : allMembersWithoutSelf()) {
    if (!member.getState().equals(NodeState.UP)) {        // 只管非健康节点
        reportByGrpc/Http(member);
    }
}
```

> 重要：这种 member 健康是**本地对 peer 的观测**，不能替代 AP/CP 协议自身的 membership 决策，也不等于业务实例健康（那是第 07 篇的健康检查）。

---

## 7. 节点间通信：ClusterRpcClientProxy

集群内部 RPC（如成员上报、Distro 数据同步、Raft 消息）走 gRPC。[ClusterRpcClientProxy](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/ClusterRpcClientProxy.java#L61) `extends MemberChangeListener`——它**订阅成员变更事件，动态维护到每个 peer 的长连接**。

```java
@PostConstruct
public void init() {
    NotifyCenter.registerSubscriber(this);                // 订阅 MembersChangeEvent
    refresh(serverMemberManager.allMembersWithoutSelf()); // 为每个 peer 建 gRPC client
}
```

[refresh](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/ClusterRpcClientProxy.java#L95-L120) 做增量维护：
- 新成员 → `createRpcClientAndStart`（建 gRPC 长连接，client key = `"Cluster-" + address`）。
- 离开的成员 → 找出 `Cluster-` 前缀且不在新列表里的 client，`shutdown()` 并移除。

每个 client 用**固定单 server 的 `ServerListFactory`**（[L142-158](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/ClusterRpcClientProxy.java#L142-L158)）——一个 client 只连一个固定 peer，不做负载均衡（集群内是点对点通信）。`sendRequest`（[L186](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/ClusterRpcClientProxy.java#L186-L200)）默认 3 秒超时。

所以**成员变更事件同时驱动了通信层**：节点一加入，立刻为它建连接；一离开，立刻断连接回收资源。

---

## 8. 一致性协议接入：AP 与 CP

成员层解决"有谁"，一致性层解决"数据怎么同步"。[ProtocolManager](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/ProtocolManager.java#L47) 同样 `extends MemberChangeListener`，统一管理两种协议的生命周期：

```java
public class ProtocolManager extends MemberChangeListener implements DisposableBean {
    private CPProtocol cpProtocol;    // Raft（JRaft 实现）
    private APProtocol apProtocol;    // Distro
    ...
}
```

### 8.1 双协议数据分流

| 协议 | 实现 | CAP | 承载数据 |
|------|------|-----|----------|
| **AP** | Distro（自研，见第 06 篇） | 高可用、最终一致 | Naming **临时实例**（ephemeral） |
| **CP** | Raft（JRaft） | 强一致 | Naming **持久实例**、部分元数据、**内嵌 Derby 集群的配置数据** |

选择逻辑：临时实例量大、变更频繁、可容忍短暂不一致 → AP；持久实例要求强一致、不能丢 → CP。这与第 02 篇（客户端操作服务的 AP/CP 双路径）、第 06 篇（Distro）一脉相承。

> ⚠️ 一个常见误解：**配置中心并非无条件走 CP**。配置数据是否走 Raft 取决于存储模式，详见 §8.4——只有「内嵌 Derby + 集群」才走 CP，生产默认的「外部 MySQL」模式并不走 Raft。

### 8.2 懒加载

两个协议按需初始化（[getCpProtocol/getApProtocol L84-106](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/ProtocolManager.java#L84-L106)），用双重检查锁：

```java
public CPProtocol getCpProtocol() {
    if (!cpInit) {
        synchronized (cpLock) {
            if (!cpInit) {
                initCPProtocol();
                cpInit = true;
            }
        }
    }
    return cpProtocol;
}
```

初始化时通过 SPI（`ApplicationUtils.getBeanIfExist`）找到协议实现，注入成员信息后 `init(config)`。

### 8.3 成员变更如何传给协议

核心在 [onEvent](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/ProtocolManager.java#L161-L179)——监听 `MembersChangeEvent`，把新拓扑推给两个协议：

```java
@Override
public void onEvent(MembersChangeEvent event) {
    if (Objects.nonNull(apProtocol)) {
        ProtocolExecutor.apMemberChange(
            () -> apProtocol.memberChange(toAPMembersInfo(event.getMembers())));
    }
    if (Objects.nonNull(cpProtocol)) {
        ProtocolExecutor.cpMemberChange(
            () -> cpProtocol.memberChange(toCPMembersInfo(event.getMembers())));
    }
}
```

一个关键细节：**AP 和 CP 用的地址不同**。
- `toAPMembersInfo`（[L68-72](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/ProtocolManager.java#L68-L72)）直接用 `member.getAddress()`（主端口 8848）。
- `toCPMembersInfo`（[L74-82](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/ProtocolManager.java#L74-L82)）用 `ip + calculateRaftPort`——Raft 走**独立端口**（默认主端口 + 1000，即 7848），与业务通信隔离。

用独立线程池（`ProtocolExecutor`）处理，保证不同协议的成员变更互不阻塞，且单线程保证顺序。

### 8.4 配置中心的一致性：取决于存储模式（重要澄清）

很多人以为「配置走 CP」，但准确说：**配置数据是否走 Raft，由存储模式决定**。分叉点在 [DynamicDataSource.getDataSource](file:///Users/mmhm/IdeaProjects/nacos/persistence/src/main/java/com/alibaba/nacos/persistence/datasource/DynamicDataSource.java#L42-L64)（源码注释：单机默认内嵌存储，集群默认外部数据库）：

| 部署模式 | 一致性怎么保证 | 走 CP(Raft) |
|---------|--------------|:----:|
| 集群 + **外部 MySQL**（生产默认） | 靠 MySQL 本身共享存储 + 异步通知刷新缓存 | ❌ 不走 |
| 集群 + **内嵌 Derby** | 靠 JRaft 日志复制 + 快照恢复 | ✅ 走 CP |
| 单机 | 本地 Derby，无需一致性 | ❌ |

**① Derby 集群走 CP 的铁证**——[DistributedDatabaseOperateImpl](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java)：
- `@Conditional(ConditionDistributedEmbedStorage.class)`（[L146](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java#L146)）——仅「分布式内嵌存储」生效。
- `extends RequestProcessor4CP`（[L149](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java#L149)）——它本身就是一个 CP 状态机处理器。
- 构造时 `this.protocol = protocolManager.getCpProtocol()`（[L184](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java#L184)），注释直言「Raft + Derby 模式，数据一致性依赖 Raft 的日志回放和快照恢复」（[L194-196](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java#L194-L196)）。
- 写：`update()` 把 SQL 打包成 `WriteRequest` 调 `protocol.write(request)`（[L470](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java#L470)）走 Raft 多数派提交；日志 apply 回调 `onApply()`（[L558](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java#L558)）才在各节点本地 Derby 执行 SQL——标准的 Raft 状态机复制。`group()` 返回 `CONFIG_MODEL_RAFT_GROUP`（[L608](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java#L608)）。
- 旁证：[EmbeddedDumpService.canExecute](file:///Users/mmhm/IdeaProjects/nacos/config/src/main/java/com/alibaba/nacos/config/server/service/dump/EmbeddedDumpService.java#L173-L181) 明写「if is derby + raft mode, only leader can execute」——只有 Leader 能 dump，典型 CP 特征。

**② MySQL 集群不走 CP 的证据**——[ConfigChangePublisher.notifyConfigChange](file:///Users/mmhm/IdeaProjects/nacos/config/src/main/java/com/alibaba/nacos/config/server/service/ConfigChangePublisher.java#L7-L12)：

```java
public static void notifyConfigChange(ConfigDataChangeEvent event) {
    if (DatasourceConfiguration.isEmbeddedStorage() && !EnvUtil.getStandaloneMode()) {
        return;   // Derby集群:走raft,不发这个事件
    }
    NotifyCenter.publishEvent(event);   // MySQL模式:发事件→异步通知其他节点刷新缓存
}
```

MySQL 模式下配置落到共享的 MySQL（一致性「外包」给数据库），各节点本地只有缓存；写完由 [AsyncNotifyService](file:///Users/mmhm/IdeaProjects/nacos/config/src/main/java/com/alibaba/nacos/config/server/service/notify/AsyncNotifyService.java#L95-L118) 通过 gRPC 逐个通知其他节点刷新缓存——这是**最终一致的通知机制，不是 Raft**。

**为什么 Derby 模式要用 CP**：配置数据写少读多、要求强一致、绝不能丢、更不能读到脏配置；内嵌 Derby 是「每节点一个本地库」没有共享存储，只能靠 Raft 复制日志让多副本达成强一致。这与临时实例（AP，优先可用）形成鲜明对比——**同一个 Nacos，按数据特征分别选择一致性模型**。

---

## 9. 关键设计决策

1. **成员层与一致性层解耦**：`ServerMemberManager` 只管"有谁、谁死活"，Distro/Raft 只管"数据同步"，两层通过 `MembersChangeEvent` 事件松耦合，各自独立演进。
2. **事件驱动的拓扑传播**：一次 `memberChange` 发一个事件，健康观测、节点通信（建/断连接）、AP/CP 协议（更新拓扑）三方全部被动响应，无需互相调用。
3. **空列表短路 + 本机保底**：防止寻址源抖动导致误清空集群或漏掉自己，这是分布式系统的防御性设计。
4. **有序 serverList（跳表）**：`ConcurrentSkipListMap` 按地址排序，使 `isFirstIp` 确定性选主，供 Raft 集群初始化 / 一次性任务用。
5. **SUSPICIOUS 缓冲状态**：节点健康不搞"一次失败即死"，用中间态 + 失败计数容忍网络抖动，但对 "Connection refused" 快速判死。
6. **去中心化 gossip 上报**：无中心健康裁判，每个节点轮询上报、双向刷新，配合独立的"不健康节点专项重试"加速恢复发现。
7. **AP/CP 双协议 + 端口隔离**：按数据一致性需求分流，Raft 用独立端口，避免与海量业务 gRPC 流量互相干扰。
8. **gRPC 优先、HTTP 兼容**：2.3+ 集群内通信默认 gRPC 长连接，保留 HTTP 上报仅为兼容混合版本滚动升级。

---

## 10. 完整数据流转时间线

**场景：一个新节点 D 加入已有集群 {A, B, C}（文件寻址）**

```
T0  运维把 D 的地址写入所有节点的 cluster.conf（或 D 自己的 cluster.conf 含全量）
T1  各节点 FileConfigMemberLookup 的 WatchFileCenter(inotify) 感知文件变化
     → readClusterConfFromDisk → afterLookup([A,B,C,D])
T2  ServerMemberManager.memberChange([A,B,C,D])
     ├─ hasChange=true（多了 D）
     ├─ serverList 加入 D（初始非 UP）
     ├─ syncToFile 回写 cluster.conf
     └─ 发 MembersChangeEvent
T3  三个订阅者并行响应同一事件：
     ├─ ClusterRpcClientProxy.onEvent → refresh → 为 D 建 gRPC 长连接
     ├─ ProtocolManager.onEvent
     │    ├─ apProtocol.memberChange（Distro 拓扑纳入 D:8848）
     │    └─ cpProtocol.memberChange（Raft 拓扑纳入 D:7848，若 D 是首节点则 addPeer）
     └─ （其他 MemberChangeListener）
T4  MemberInfoReportTask 轮询到 D → reportByGrpc(D)
     → D 的 MemberReportHandler 收到 → 把上报方标记 UP，回自己信息
     → onSuccess(D)：D 状态置 UP，加入 memberAddressInfos，再发一次 MembersChangeEvent
T5  D 完成 Distro 快照拉取（第06篇 processSnapshot）+ Raft 数据同步
     → D 的 setSelfReady → 对外提供服务
     至此 D 成为集群健康成员，可承接读写，数据与 A/B/C 最终一致
```

**场景：节点 C 进程崩溃**

```
T0  C 进程挂掉
T1  其他节点 MemberInfoReportTask 轮询上报 C 失败
     → MemberUtil.onFail(C)：state=SUSPICIOUS，failAccessCnt++
     （"Connection refused" 则直接 DOWN）
T2  连续失败超 maxFailAccessCnt → C.state=DOWN
     → 从 memberAddressInfos 移除 → notifyMemberChange 发 MembersChangeEvent
T3  ├─ ClusterRpcClientProxy：C 仍在列表(DOWN)，连接保留；若真移除才 shutdown client
     ├─ Distro：C 负责的临时实例数据由其他责任节点接管/客户端重连到存活节点
     └─ Raft：若 C 是 Leader，触发重新选举；Follower 挂了不影响多数派写入
T4  UnhealthyMemberInfoReportTask 每 5s 持续尝试联系 C
     → C 恢复后上报成功 → onSuccess → 重新置 UP → 发事件 → 重新接入
```

---

## 11. 总结

1. **四层能力**：成员发现（Lookup）→ 成员管理（ServerMemberManager）→ 健康观测（上报+状态机）→ 一致性协议（AP/CP），层层递进。
2. **成员发现三选一**：文件（cluster.conf + inotify 热更新）、地址服务器（HTTP 轮询）、单机；由 `LookupFactory.chooseLookup` 决定。
3. **ServerMemberManager 是权威视图**：`serverList`（有序跳表）+ `memberChange`（空列表短路、本机保底、保留元数据、回写文件、发事件）。
4. **健康观测去中心化**：`MemberInfoReportTask` 轮询 gRPC 上报 + 双向刷新，`UP→SUSPICIOUS→DOWN` 状态机容忍抖动，专项任务加速恢复。
5. **通信层随拓扑自适应**：`ClusterRpcClientProxy` 订阅成员事件，为每个 peer 维护固定的 gRPC 长连接。
6. **一致性层双协议分流**：`ProtocolManager` 管理 AP（Distro，临时实例）和 CP（Raft，配置/持久实例），Raft 用独立端口，成员变更经事件推送给两协议。
7. **一根主线串起全部**：`MembersChangeEvent` 事件是集群的"神经中枢"——成员一变，健康观测、节点通信、一致性协议同时响应，实现松耦合的自适应集群。

如果把本文与前面的 Naming 系列结合：本文的**成员层**是地基，第 06 篇的 **Distro** 是跑在其上的 AP 协议，第 02 篇的 **CP 路径**跑在 Raft 上——集群原理 = 成员管理 + 这两套一致性协议的合奏。

---

## 12. 文件索引表

| 文件 | 关键内容 | 关键行号 |
|------|----------|----------|
| [ServerMemberManager.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java) | 集群成员权威视图 | 字段 L108-146；init L153-175；initAndStartLookup L226-230；update L250-276；allMembers L337-343；memberChange L356-416；memberJoin L424-428；memberLeave L436-440；isFirstIp L475-477；setSelfReady L479-489；MemberInfoReportTask L548-560；reportByGrpc L616-648；UnhealthyMemberInfoReportTask L677-704 |
| [Member.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/Member.java) | 节点模型 | 类定义 L41；failAccessCnt L45；check L90-92；compareTo L115-117；Builder L140-189 |
| [MemberUtil.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/MemberUtil.java) | 状态机与工具 | onSuccess L131-159；onFail L167-197；syncToFile L200+ |
| [MemberLookup.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/MemberLookup.java) | 寻址接口 | afterLookup L58；接口全貌 L30-76 |
| [LookupFactory.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/LookupFactory.java) | 寻址模式选择 | chooseLookup L39-51；LookupType L61-71 |
| [FileConfigMemberLookup.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/FileConfigMemberLookup.java) | 文件寻址 + inotify | doStart L55-67；watcher L42-53 |
| [AddressServerMemberLookup.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/AddressServerMemberLookup.java) | 地址服务器寻址 | 字段/重试/健康 L45-84 |
| [StandaloneMemberLookup.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/lookup/StandaloneMemberLookup.java) | 单机寻址 | doStart L2-6 |
| [ClusterRpcClientProxy.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/ClusterRpcClientProxy.java) | 节点间 gRPC 通信 | init L74-88；refresh L95-120；createRpcClientAndStart L126-162；sendRequest L186-200 |
| [MemberReportHandler.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/remote/MemberReportHandler.java) | 接收成员上报 | handle L56-71 |
| [ProtocolManager.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/ProtocolManager.java) | AP/CP 协议管理 | 字段 L49-51；toAPMembersInfo L68-72；toCPMembersInfo L74-82；getCpProtocol L84-94；onEvent L161-179 |
| [DistributedDatabaseOperateImpl.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/persistence/DistributedDatabaseOperateImpl.java) | Derby 集群配置走 CP | @Conditional L146；extends RequestProcessor4CP L149；getCpProtocol L184；init 注释 L194-196；write L470；onApply L558；group L608 |
| [DynamicDataSource.java](file:///Users/mmhm/IdeaProjects/nacos/persistence/src/main/java/com/alibaba/nacos/persistence/datasource/DynamicDataSource.java) | 内嵌/外部存储分叉 | getDataSource L42-64 |
| [ConfigChangePublisher.java](file:///Users/mmhm/IdeaProjects/nacos/config/src/main/java/com/alibaba/nacos/config/server/service/ConfigChangePublisher.java) | MySQL 模式发通知事件 | notifyConfigChange L7-12 |
| [AsyncNotifyService.java](file:///Users/mmhm/IdeaProjects/nacos/config/src/main/java/com/alibaba/nacos/config/server/service/notify/AsyncNotifyService.java) | 外部存储 gRPC 异步通知 | handleConfigDataChangeEvent L95-118 |

---

> 关联阅读：
> - [Naming 服务端 06：Distro AP 一致性协议](./Naming服务端-06-Distro一致性协议.md)（本文 AP 协议的展开）
> - [Naming 服务端 02：客户端操作服务](./Naming服务端-02-客户端操作服务.md)（AP/CP 双写路径）
> - [Naming 服务端 07：健康检查机制](./Naming服务端-07-健康检查机制.md)（业务实例健康，区别于本文的节点健康）
