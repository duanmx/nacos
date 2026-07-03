# Naming 服务端 - 06 - Distro AP 一致性协议

> **原则声明**：本文所有结论均来自 Nacos 服务端源码，标注了精确的文件路径与行号，不含任何主观猜测。阅读时可对照源码逐行验证。
>
> 版本：`3.2.1-SNAPSHOT` ｜ 主分支：`develop`

---

## 目录

1. [为什么临时实例用 Distro 而非 Raft](#1-为什么临时实例用-distro-而非-raft)
2. [Distro 三大机制总览](#2-distro-三大机制总览)
3. [责任划分：谁负责谁的数据](#3-责任划分谁负责谁的数据)
4. [DistroClientDataProcessor 的三重身份](#4-distroclientdataprocessor-的三重身份)
5. [写扩散：变更同步到所有节点](#5-写扩散变更同步到所有节点)
6. [接收侧：processData 与 upgradeClient](#6-接收侧processdata-与-upgradeclient)
7. [反熵校验：verify 机制](#7-反熵校验verify-机制)
8. [新节点全量拉取：snapshot](#8-新节点全量拉取snapshot)
9. [关键设计决策](#9-关键设计决策)
10. [线程安全与异常边界](#10-线程安全与异常边界)
11. [完整数据流转时间线](#11-完整数据流转时间线)
12. [总结](#12-总结)
13. [文件索引表](#13-文件索引表)

---

## 1. 为什么临时实例用 Distro 而非 Raft

Nacos 的两类数据走两套一致性协议：

| 数据类型 | 协议 | 一致性 | 原因 |
|----------|------|--------|------|
| 持久实例（第 02 篇 Persistent） | Raft | CP（强一致） | 关键数据，宁可牺牲可用性 |
| **临时实例**（第 02 篇 Ephemeral） | **Distro** | **AP（最终一致）** | 量大、变更频繁（心跳），追求高可用低延迟 |

**Distro 是 Nacos 自研的 AP 协议**，核心思想：

- **数据分片 + 责任制**：每个节点只**负责**（权威持有）一部分 Client 的数据，通过一致性 hash 划分。
- **写扩散**：负责节点收到写操作后，把数据**异步复制**到集群所有其他节点（每个节点都有全量副本，供读）。
- **反熵校验**：负责节点周期性向其他节点发送「数据指纹（revision）」校验，发现不一致就补发。

结果：任何节点都能提供读服务（全量副本），写只由责任节点扩散，达成**最终一致**。

本篇聚焦 naming 侧对接 Distro 框架的核心类 `DistroClientDataProcessor`。

---

## 2. Distro 三大机制总览

```
                  ┌──────────── 责任节点 A ────────────┐
   Client 写操作   │  第03篇 addServiceInstance          │
   ──────────────►│    → publishEvent(ClientChangedEvent)│
                  │         │                            │
                  │         ▼                            │
                  │  DistroClientDataProcessor.onEvent   │
                  │    syncToAllServer                   │
                  │    distroProtocol.sync(key, CHANGE)  │
                  └─────────┬────────────────────────────┘
                            │ ①写扩散（异步复制全量数据）
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
         节点 B         节点 C        节点 D          （非责任节点，持副本）
         processData    processData   processData
         upgradeClient  upgradeClient upgradeClient

                  ┌──── 责任节点 A 周期任务 ────┐
                  │ ②反熵校验 getVerifyData     │──► 向 B/C/D 发 revision 指纹
                  │   B/C/D processVerifyData   │    不一致 → 返回 false → A 补发全量
                  └────────────────────────────┘

                  ┌──── 新节点 E 加入 ────┐
                  │ ③全量拉取 getDatumSnapshot ──► E processSnapshot（一次性灌入全量）
                  └───────────────────────┘
```

三大机制：**①写扩散**（增量、实时）、**②反熵校验**（补偿、周期）、**③快照拉取**（初始化、全量）。三者互补，保证最终一致。

---

## 3. 责任划分：谁负责谁的数据

文件：[DistroClientDataProcessor.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java)

Distro 的基石是「**责任判定**」。`isInvalidClient`（L137-141）定义了「哪些 Client 数据本节点该主动同步出去」：

```java
private boolean isInvalidClient(Client client) {
    // Only ephemeral data sync by Distro, persist client should sync by raft.
    return null == client
            || !client.isEphemeral()                          // 非临时 → Raft 管，不归 Distro
            || !clientManager.isResponsibleClient(client);    // 非本节点负责 → 不主动扩散
}
```

三个条件任一满足即「无效（不该由本节点同步）」：
1. client 为 null。
2. **不是临时的**——持久数据走 Raft（第 02 篇），Distro 不碰。
3. **本节点不负责它**——`isResponsibleClient` 基于一致性 hash 判断。一个 Client 的数据由集群中某一个确定的节点负责，只有责任节点才主动向外扩散，避免重复扩散。

**责任制的意义**：每份数据只有一个「权威源」（责任节点），写扩散和反熵校验都由它发起，其他节点只被动接收，避免了多写冲突。

---

## 4. DistroClientDataProcessor 的三重身份

```java
public class DistroClientDataProcessor extends SmartSubscriber
        implements DistroDataStorage, DistroDataProcessor {

    public static final String TYPE = "Nacos:Naming:v2:ClientData";
}
```

（L58-61）这个类同时扮演三个角色，正是它成为 naming↔Distro 桥梁的原因：

| 身份 | 职责 | 关键方法 |
|------|------|----------|
| `SmartSubscriber` | 监听本地 Client 变更事件，触发写扩散 | `onEvent`（发送侧） |
| `DistroDataStorage` | 向 Distro 框架**提供**本节点数据 | `getDistroData`、`getDatumSnapshot`、`getVerifyData` |
| `DistroDataProcessor` | **处理**从其他节点收到的数据 | `processData`、`processSnapshot`、`processVerifyData`（接收侧） |

`TYPE`（L61）是这份数据在 Distro 框架里的注册类型标识——Distro 框架按 type 把不同业务数据路由到对应处理器。

`isFinishInitial`（L82-90）标识本节点是否已完成初始数据拉取（新节点启动时需先拉全量才能对外服务）。

---

## 5. 写扩散：变更同步到所有节点

### 5.1 订阅本地变更事件

```java
@Override
public List<Class<? extends Event>> subscribeTypes() {
    List<Class<? extends Event>> result = new LinkedList<>();
    result.add(ClientEvent.ClientChangedEvent.class);        // 数据变更
    result.add(ClientEvent.ClientDisconnectEvent.class);     // 连接断开
    result.add(ClientEvent.ClientVerifyFailedEvent.class);   // 校验失败（对端要补发）
    return result;
}
```

（L92-99）这里订阅的 `ClientChangedEvent` 正是第 03 篇 `AbstractClient.addServiceInstance` / `removeServiceInstance` 发出的那个事件——**模型层一改，Distro 就感知**。

### 5.2 onEvent —— 单机模式短路

```java
@Override
public void onEvent(Event event) {
    if (EnvUtil.getStandaloneMode()) {   // 单机模式无需同步
        return;
    }
    if (event instanceof ClientEvent.ClientVerifyFailedEvent) {
        syncToVerifyFailedServer((ClientEvent.ClientVerifyFailedEvent) event);  // 定向补发
    } else {
        syncToAllServer((ClientEvent) event);                                   // 广播扩散
    }
}
```

（L101-111）单机模式直接 return（没有其他节点）。集群模式分两路：校验失败 → 定向补发；其他 → 广播。

### 5.3 syncToAllServer —— 广播扩散

```java
private void syncToAllServer(ClientEvent event) {
    Client client = event.getClient();
    if (isInvalidClient(client)) { return; }                 // 责任判定：不归我管就不扩散
    if (event instanceof ClientEvent.ClientDisconnectEvent) {
        DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
        distroProtocol.sync(distroKey, DataOperation.DELETE); // 删除扩散
    } else if (event instanceof ClientEvent.ClientChangedEvent) {
        DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
        distroProtocol.sync(distroKey, DataOperation.CHANGE); // 变更扩散
    }
}
```

（L123-135）关键点：
- 先 `isInvalidClient` 过滤——**只有责任节点才扩散**。
- `DistroKey = clientId + TYPE`：以 clientId 为资源 key。
- `distroProtocol.sync(...)`：交给 Distro 框架层做异步扩散。框架内部用**延迟任务 + 失败重试**（类似第 05 篇的延迟引擎思路）把数据发往所有其他节点，`sync` 本身立即返回，不阻塞业务。

### 5.4 syncToVerifyFailedServer —— 定向补发

```java
private void syncToVerifyFailedServer(ClientEvent.ClientVerifyFailedEvent event) {
    Client client = clientManager.getClient(event.getClientId());
    if (isInvalidClient(client)) { return; }
    DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
    // Verify failed data should be sync directly.
    distroProtocol.syncToTarget(distroKey, DataOperation.ADD, event.getTargetServer(), 0L);
}
```

（L113-121）当反熵校验发现某节点数据不一致时，**只向那个节点**（`event.getTargetServer()`）补发全量数据，延迟参数 `0L` 表示立即发送（不走延迟合并）。这是反熵机制的「修复动作」。

---

## 6. 接收侧：processData 与 upgradeClient

其他节点扩散来的数据，由 `processData`（L148-166）接收：

```java
@Override
public boolean processData(DistroData distroData) {
    switch (distroData.getType()) {
        case ADD:
        case CHANGE:
            ClientSyncData clientSyncData = serializer.deserialize(distroData.getContent(), ClientSyncData.class);
            handlerClientSyncData(clientSyncData);       // 落地新增/变更
            return true;
        case DELETE:
            String deleteClientId = distroData.getDistroKey().getResourceKey();
            clientManager.clientDisconnected(deleteClientId);  // 落地删除
            return true;
        default:
            return false;
    }
}
```

### 6.1 handlerClientSyncData —— 建/更新同步客户端

```java
private void handlerClientSyncData(ClientSyncData clientSyncData) {
    clientManager.syncClientConnected(clientSyncData.getClientId(), clientSyncData.getAttributes()); // 建同步客户端
    Client client = clientManager.getClient(clientSyncData.getClientId());
    upgradeClient(client, clientSyncData);   // diff 更新
}
```

（L168-177）`syncClientConnected` 创建的是「**非 native** 的同步客户端」（第 03 篇 `ConnectionBasedClient.isNative=false`）——表示这不是本机直连，而是从别处同步来的副本。

### 6.2 upgradeClient —— diff 式增量更新

`upgradeClient`（L179-214）是接收侧的核心，用 **diff** 对齐本地副本与同步数据：

```java
private void upgradeClient(Client client, ClientSyncData clientSyncData) {
    Set<Service> syncedService = new HashSet<>();
    processBatchInstanceDistroData(syncedService, client, clientSyncData);  // 批量实例先处理

    // ① 遍历同步来的服务：新增或变更
    for (int i = 0; i < namespaces.size(); i++) {
        Service singleton = ServiceManager.getInstance().getSingleton(...);  // 享元单例（第03篇）
        syncedService.add(singleton);
        InstancePublishInfo instancePublishInfo = instances.get(i);
        if (!instancePublishInfo.equals(client.getInstancePublishInfo(singleton))) { // 只处理真正变化的
            client.addServiceInstance(singleton, instancePublishInfo);
            NotifyCenter.publishEvent(new ClientRegisterServiceEvent(singleton, client.getClientId())); // 驱动本地索引/推送
            NotifyCenter.publishEvent(new InstanceMetadataEvent(singleton, ...));
        }
    }
    // ② 遍历本地已有但同步数据里没有的服务：删除
    for (Service each : client.getAllPublishedService()) {
        if (!syncedService.contains(each)) {
            client.removeServiceInstance(each);
            NotifyCenter.publishEvent(new ClientDeregisterServiceEvent(each, client.getClientId()));
        }
    }
    // ③ 对齐 revision
    client.setRevision(clientSyncData.getAttributes().getClientAttribute(REVISION, 0));
}
```

三个关键点：

1. **只处理真正变化的**（L194）：`!instancePublishInfo.equals(...)` 才更新，相同则跳过，避免无谓事件。
2. **发的是业务事件**：`ClientRegisterServiceEvent`（第 02 篇同款）→ 触发本节点的倒排索引（第 04 篇）和推送（第 05 篇）。**即：同步来的数据也会让本节点通知它的订阅者**，这正是 AP 副本能对外提供读/推送的原因。
3. **对齐 revision**（L212-213）：把本地副本的 revision 设成和源节点一致，供下次反熵校验比对。

**注意**：`upgradeClient` 直接 `setRevision`（用同步数据的值），而非 `recalculateRevision`——因为副本要与源保持相同指纹，不能自己重算。

---

## 7. 反熵校验：verify 机制

写扩散是异步的，可能丢消息。Distro 用**周期性反熵校验**兜底。

### 7.1 责任节点提供校验数据 getVerifyData

```java
@Override
public List<DistroData> getVerifyData() {
    List<DistroData> result = null;
    for (String each : clientManager.allClientId()) {
        Client client = clientManager.getClient(each);
        if (null == client || !client.isEphemeral()) { continue; }
        if (clientManager.isResponsibleClient(client)) {     // 只校验本节点负责的
            DistroClientVerifyInfo verifyData =
                    new DistroClientVerifyInfo(client.getClientId(), client.getRevision()); // 只带 revision 指纹！
            ...
            data.setType(DataOperation.VERIFY);
            result.add(data);
        }
    }
    return result;
}
```

（L297-319）关键：校验数据 `DistroClientVerifyInfo` **只包含 clientId + revision**（一个数字指纹），不含完整实例列表——**校验是轻量的**，只传指纹不传全量。责任节点周期性把这些指纹发往其他节点。

### 7.2 接收节点校验 processVerifyData

```java
@Override
public boolean processVerifyData(DistroData distroData, String sourceAddress) {
    DistroClientVerifyInfo verifyData = serializer.deserialize(distroData.getContent(), DistroClientVerifyInfo.class);
    if (clientManager.verifyClient(verifyData)) {    // 本地 revision 与指纹一致？
        return true;                                  // 一致 → 校验通过
    }
    Loggers.DISTRO.info("client {} is invalid, get new client from {}", ...);
    return false;                                     // 不一致 → 校验失败
}
```

（L248-258）`verifyClient` 比对本地副本的 revision 与收到的指纹：
- **一致**：数据同步无误，返回 true。
- **不一致**：本地副本过期/缺失，返回 false。Distro 框架会据此让**责任节点**（源）重新扩散全量——这就闭环到第 5.4 节的 `syncToVerifyFailedServer`（`ClientVerifyFailedEvent` → 定向补发）。

**revision 在这里兑现价值**：第 03 篇讲过 revision 是「内容指纹」，反熵校验用它以极小代价（一个 long）判断两节点数据是否一致，无需传输和逐条比对完整列表。

---

## 8. 新节点全量拉取：snapshot

新节点加入集群时，需要一次性获取全量数据。

### 8.1 提供快照 getDatumSnapshot

```java
@Override
public DistroData getDatumSnapshot() {
    List<ClientSyncData> datum = new LinkedList<>();
    for (String each : clientManager.allClientId()) {
        Client client = clientManager.getClient(each);
        if (null == client || !client.isEphemeral()) { continue; } // 只快照临时数据
        datum.add(client.generateSyncData());                       // 第03篇的打包方法
    }
    ClientSyncDatumSnapshot snapshot = new ClientSyncDatumSnapshot();
    snapshot.setClientSyncDataList(datum);
    ...
    return new DistroData(new DistroKey(DataOperation.SNAPSHOT.name(), TYPE), data);
}
```

（L281-295）把本节点所有临时 Client 的数据（`generateSyncData`，第 03 篇）打包成一个大快照。注意这里**不限于责任节点的数据**——快照给新节点用的是本节点持有的全部副本。

### 8.2 处理快照 processSnapshot

```java
@Override
public boolean processSnapshot(DistroData distroData) {
    ClientSyncDatumSnapshot snapshot = serializer.deserialize(distroData.getContent(), ClientSyncDatumSnapshot.class);
    for (ClientSyncData each : snapshot.getClientSyncDataList()) {
        handlerClientSyncData(each);    // 复用单条处理逻辑
    }
    return true;
}
```

（L260-268）逐条复用 `handlerClientSyncData`（第 6 节）灌入。新节点拉完快照后调 `finishInitial()` 标记就绪。

### 8.3 按需单条 getDistroData

```java
@Override
public DistroData getDistroData(DistroKey distroKey) {
    Client client = clientManager.getClient(distroKey.getResourceKey());
    if (null == client) { return null; }
    byte[] data = serializer.serialize(client.generateSyncData());
    return new DistroData(distroKey, data);
}
```

（L270-279）写扩散和定向补发时，框架用它按 clientId 取单个 Client 的当前数据。

---

## 9. 关键设计决策

### 9.1 为什么临时用 Distro、持久用 Raft？

临时数据（心跳）变更极频繁，Raft 的多数派写会成为瓶颈，且临时数据丢了可由客户端重新注册恢复——用 AP 换取高可用低延迟。持久数据需强一致，用 CP。

### 9.2 为什么要责任制（isResponsibleClient）？

每份数据一个权威源，写扩散和反熵都由责任节点单向发起，天然避免多写冲突和重复扩散。一致性 hash 让责任划分在节点增减时只迁移少量数据。

### 9.3 为什么校验只传 revision 而非全量？

反熵校验是周期性、覆盖全量数据的高频操作。若每次传完整实例列表，网络开销巨大。用 revision（内容哈希）做指纹，一个 long 就能判断一致性，只在不一致时才补发全量——**校验轻、修复重**，符合「大多数时候是一致的」这一现实。

### 9.4 为什么同步数据也发业务事件？

接收节点 `upgradeClient` 发 `ClientRegisterServiceEvent`，让本节点的倒排索引、推送照常工作。这样**任何节点（无论是否责任节点）都能对本地订阅者提供推送**，副本真正「可用」，而非只是冷备份。

### 9.5 diff 更新为什么只处理变化的？

`!instancePublishInfo.equals(...)` 过滤，避免相同数据触发无意义的事件风暴（进而触发无谓推送）。

---

## 10. 线程安全与异常边界

| 环节 | 机制 | 说明 |
|------|------|------|
| 写扩散 | `distroProtocol.sync` 异步 + 延迟任务 + 失败重试 | 不阻塞业务，丢失由反熵兜底 |
| 责任判定 | `isResponsibleClient`（一致性 hash） | 每份数据单一权威源，无多写冲突 |
| 接收处理 | `ServiceManager.getSingleton` 享元 | 同步来的 Service 也收敛为单例（第03篇） |
| diff 更新 | `equals` 比对 + `setRevision` 对齐 | 副本与源指纹一致，供反熵比对 |
| 单机模式 | `onEvent` 直接 return | 无集群，跳过同步 |
| 数据丢失 | 周期反熵 verify + 校验失败补发 | 最终一致的保证 |
| 新节点 | snapshot 全量 + `finishInitial` | 就绪前不对外服务 |

---

## 11. 完整数据流转时间线

以「客户端在责任节点 A 注册临时实例」为例：

```
T0   客户端 → A：注册（第01/02篇）→ addServiceInstance（第03篇）
       publishEvent(ClientChangedEvent)
       │
T1   A: DistroClientDataProcessor.onEvent
       standalone? 否 → syncToAllServer
       isInvalidClient? 否（A 负责该 client）
       distroProtocol.sync(DistroKey(clientId), CHANGE)   ← 异步扩散
       │
T2   A: Distro 框架 → getDistroData(clientId) → client.generateSyncData() → 发往 B、C、D
       │
T3   B/C/D: processData(CHANGE)
       → handlerClientSyncData → syncClientConnected（建非native副本）
       → upgradeClient：diff → addServiceInstance + publishEvent(ClientRegisterServiceEvent)
       → 本地倒排索引更新（第04篇）→ 本地推送订阅者（第05篇）
       → setRevision（对齐 A 的指纹）
       │
Tn   A: 周期任务 getVerifyData → 把 {clientId, revision} 发往 B/C/D
       │
Tn+1 B: processVerifyData → verifyClient(revision 一致?) 
       ├─ 一致 → return true（无需动作）
       └─ 不一致 → return false → 触发 A 的 ClientVerifyFailedEvent
                    → A: syncToVerifyFailedServer → syncToTarget(B, ADD, 立即) 补发全量
```

---

## 12. 总结

1. **协议选型**：临时实例用自研 Distro（AP，最终一致），持久实例用 Raft（CP）——`isInvalidClient` 里 `!isEphemeral` 就是这条分界线。
2. **责任制**：一致性 hash 让每份数据有唯一责任节点，写扩散与反熵校验由它单向发起，避免冲突。
3. **三重身份**：`DistroClientDataProcessor` 同时是 `SmartSubscriber`（听变更）、`DistroDataStorage`（供数据）、`DistroDataProcessor`（收数据）。
4. **写扩散**：`ClientChangedEvent` → `syncToAllServer` → `distroProtocol.sync`，异步复制全量数据到所有节点。
5. **接收 diff**：`upgradeClient` 只处理真正变化的实例，发业务事件驱动本地索引/推送，并 `setRevision` 对齐指纹。
6. **反熵校验**：`getVerifyData` 只传 revision 指纹（轻量），不一致才补发全量（`syncToVerifyFailedServer`）——校验轻、修复重。
7. **快照拉取**：新节点 `getDatumSnapshot`/`processSnapshot` 一次性灌入全量，`finishInitial` 标记就绪。
8. **副本可用**：同步数据同样触发本地推送，任何节点都能服务订阅者，而非冷备。

下一篇（第 07 篇）将讲 **健康检查机制**：第 03 篇 `IpPortBasedClient.init` 启动的 `ClientBeatCheckTaskV2`（临时心跳）与 `HealthCheckTaskV2`（持久主动探测）如何工作，以及不健康/过期实例如何被摘除。

---

## 13. 文件索引表

| 文件 | 关键内容 | 关键行号 |
|------|----------|----------|
| [DistroClientDataProcessor.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java) | Distro naming 侧核心处理器 | TYPE L61；三重身份 L58-59；subscribeTypes L92-99；onEvent L101-111；syncToVerifyFailedServer L113-121；syncToAllServer L123-135；isInvalidClient L137-141；processData L148-166；handlerClientSyncData L168-177；upgradeClient L179-214；processBatchInstance L216-246；processVerifyData L248-258；processSnapshot L260-268；getDistroData L270-279；getDatumSnapshot L281-295；getVerifyData L297-319 |
| [DistroClientVerifyInfo.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientVerifyInfo.java) | 校验指纹（clientId + revision） | 全文 |
| [DistroClientTransportAgent.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientTransportAgent.java) | 节点间数据传输代理 | 全文 |
| [DistroClientComponentRegistry.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientComponentRegistry.java) | 向 Distro 框架注册各组件 | 全文 |

关联文件（前序篇章）：
- 事件来源：[AbstractClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/AbstractClient.java)（第 03 篇，发 `ClientChangedEvent`、`generateSyncData`）

---

> 系列导航：
> - [第 01 篇：gRPC 请求处理链路](./Naming服务端-01-gRPC请求处理链路.md)
> - [第 02 篇：客户端操作服务](./Naming服务端-02-客户端操作服务.md)
> - [第 03 篇：数据模型（ServiceManager / Client 抽象）](./Naming服务端-03-数据模型.md)
> - [第 04 篇：事件驱动倒排索引](./Naming服务端-04-事件驱动倒排索引.md)
> - [第 05 篇：服务变更推送机制](./Naming服务端-05-服务变更推送机制.md)
> - **第 06 篇：Distro AP 一致性协议（本篇）**
> - [第 07 篇：健康检查机制](./Naming服务端-07-健康检查机制.md)
