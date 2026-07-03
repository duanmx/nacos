# Naming 服务端 - 04 - 事件驱动的倒排索引（ClientServiceIndexesManager）

> **原则声明**：本文所有结论均来自 Nacos 服务端源码，标注了精确的文件路径与行号，不含任何主观猜测。阅读时可对照源码逐行验证。
>
> 版本：`3.2.1-SNAPSHOT` ｜ 主分支：`develop`

---

## 目录

1. [为什么需要倒排索引](#1-为什么需要倒排索引)
2. [两张倒排表](#2-两张倒排表)
3. [SmartSubscriber：订阅哪些事件](#3-smartsubscriber订阅哪些事件)
4. [事件分发入口 onEvent](#4-事件分发入口-onevent)
5. [维护发布者索引](#5-维护发布者索引)
6. [维护订阅者索引](#6-维护订阅者索引)
7. [连接断开的批量清理](#7-连接断开的批量清理)
8. [对外查询接口](#8-对外查询接口)
9. [关键设计决策](#9-关键设计决策)
10. [线程安全分析](#10-线程安全分析)
11. [完整数据流转时间线](#11-完整数据流转时间线)
12. [总结](#12-总结)
13. [文件索引表](#13-文件索引表)

---

## 1. 为什么需要倒排索引

第 03 篇讲到，2.x 把实例挂在 **Client** 上：`Client.publishers` 是 `Service → 实例`。这个方向的映射叫「正排」——**已知 Client，查它发布了哪些服务**。

但服务发现的核心查询恰恰相反：**已知 Service，查有哪些 Client 注册了它 / 订阅了它**。例如：

- 客户端订阅 `order-service`，服务端要推送「谁提供了 `order-service`」——需要 `Service → 提供者 clientId 集合`。
- `order-service` 实例发生变化，服务端要通知「谁订阅了 `order-service`」——需要 `Service → 订阅者 clientId 集合`。

如果没有倒排索引，每次都要遍历**所有 Client** 的 `publishers` / `subscribers` 去筛，代价是 O(N)。`ClientServiceIndexesManager` 就是为此维护两张 **倒排表**，把查询降到 O(1)。

它的巧妙之处在于：**自己不主动扫描，而是订阅第 02 篇发出的领域事件，被动增量维护索引**。

---

## 2. 两张倒排表

文件：[ClientServiceIndexesManager.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/index/ClientServiceIndexesManager.java)

```java
@Component
public class ClientServiceIndexesManager extends SmartSubscriber {

    private final ConcurrentMap<Service, Set<String>> publisherIndexes = new ConcurrentHashMap<>();   // 发布者倒排
    private final ConcurrentMap<Service, Set<String>> subscriberIndexes = new ConcurrentHashMap<>();  // 订阅者倒排
}
```

- `publisherIndexes`（L49）：`Service → 注册了它的 clientId 集合`。
- `subscriberIndexes`（L51）：`Service → 订阅了它的 clientId 集合`。

value 用 `Set<String>`（`ConcurrentHashSet`）而非直接存 Client 对象——**只存 clientId**，避免持有 Client 强引用，也让索引与 Client 生命周期解耦。

方向对照：

```
正排（Client 内部，第03篇）        倒排（本篇）
  Client.publishers                publisherIndexes
  clientId ──► {Service...}         Service ──► {clientId...}
                       互 为 反 向
```

---

## 3. SmartSubscriber：订阅哪些事件

`ClientServiceIndexesManager extends SmartSubscriber`（L47）。`SmartSubscriber` 是 Nacos 事件框架里「**一个订阅者监听多种事件类型**」的基类。

### 3.1 构造时注册自己

```java
public ClientServiceIndexesManager() {
    NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
}
```

（L53-55）构造时把自己注册进 `NotifyCenter`，并指定用 naming 专属的事件发布器（`NamingEventPublisherFactory`）。

### 3.2 声明关心的事件类型

```java
@Override
public List<Class<? extends Event>> subscribeTypes() {
    List<Class<? extends Event>> result = new LinkedList<>();
    result.add(ClientOperationEvent.ClientRegisterServiceEvent.class);     // 注册
    result.add(ClientOperationEvent.ClientDeregisterServiceEvent.class);   // 注销
    result.add(ClientOperationEvent.ClientSubscribeServiceEvent.class);    // 订阅
    result.add(ClientOperationEvent.ClientUnsubscribeServiceEvent.class);  // 取消订阅
    result.add(ClientOperationEvent.ClientReleaseEvent.class);             // 连接断开
    return result;
}
```

（L83-92）正是第 02 篇 `ClientOperationService` 发出的那五种事件（第 03 篇的事件家族）。**上游发事件、这里收事件，形成闭环**——上游注册/注销/订阅时压根不知道倒排索引的存在，完全解耦。

---

## 4. 事件分发入口 onEvent

```java
@Override
public void onEvent(Event event) {
    if (event instanceof ClientOperationEvent.ClientReleaseEvent) {
        handleClientDisconnect((ClientOperationEvent.ClientReleaseEvent) event); // 连接断开：批量清理
    } else if (event instanceof ClientOperationEvent) {
        handleClientOperation((ClientOperationEvent) event);                     // 普通操作：单条增删
    }
}
```

（L94-101）分两条路：
- **`ClientReleaseEvent`**（连接断开）：一次连接可能注册/订阅了多个服务，需要**批量**清理 → `handleClientDisconnect`。
- **其他 4 种**：单个服务的增删 → `handleClientOperation`。

`handleClientOperation`（L122-134）再按事件子类型分派到四个私有方法：

```java
private void handleClientOperation(ClientOperationEvent event) {
    Service service = event.getService();
    String clientId = event.getClientId();
    if (event instanceof ClientRegisterServiceEvent) {        addPublisherIndexes(service, clientId);
    } else if (event instanceof ClientDeregisterServiceEvent) { removePublisherIndexes(service, clientId);
    } else if (event instanceof ClientSubscribeServiceEvent) {  addSubscriberIndexes(service, clientId);
    } else if (event instanceof ClientUnsubscribeServiceEvent) {removeSubscriberIndexes(service, clientId);
    }
}
```

---

## 5. 维护发布者索引

### 5.1 addPublisherIndexes —— 加索引并触发推送

```java
private void addPublisherIndexes(Service service, String clientId) {
    String serviceChangedType = Constants.ServiceChangedType.INSTANCE_CHANGED;
    if (!publisherIndexes.containsKey(service)) {
        // 索引里还没有这个服务 → 说明服务是「首次创建」
        serviceChangedType = Constants.ServiceChangedType.ADD_SERVICE;
    }
    NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, serviceChangedType, true)); // 触发推送
    publisherIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>()).add(clientId);          // 加索引
}
```

（L136-145）两个动作，**顺序很关键**：

1. **先判断变更类型**：索引里没有该服务 → `ADD_SERVICE`（新服务上线）；已有 → `INSTANCE_CHANGED`（服务已存在，只是多了个实例）。
2. **发 `ServiceChangedEvent`**（L142-143）：这是驱动**服务变更推送**的信号（第 05 篇详解）。第三个参数 `true` 表示要通知订阅者。
3. **再把 clientId 加进索引**（L144）：`computeIfAbsent` 保证 Set 存在后再 add。

**为什么先发事件再改索引？** 无论顺序，最终索引都会更新；这里先算 `serviceChangedType` 依赖「改之前」的状态（是否已存在），所以判断必须在 `computeIfAbsent` 之前。发事件放中间即可。

### 5.2 removePublisherIndexes —— 删索引，空则删服务

```java
private void removePublisherIndexes(Service service, String clientId) {
    publisherIndexes.computeIfPresent(service, (s, ids) -> {
        ids.remove(clientId);
        String serviceChangedType = ids.isEmpty() ? Constants.ServiceChangedType.DELETE_SERVICE
                : Constants.ServiceChangedType.INSTANCE_CHANGED;
        NotifyCenter.publishEvent(new ServiceEvent.ServiceChangedEvent(service, serviceChangedType, true));
        return ids.isEmpty() ? null : ids;      // 返回 null → 从 Map 中移除该 key
    });
}
```

（L147-156）用 `computeIfPresent` 原子地完成「删 clientId + 判空 + 决定去留」：
- 移除 clientId 后，若集合空了 → 变更类型 `DELETE_SERVICE`（服务下线），并 `return null` 让 `ConcurrentHashMap` 删掉整个 key。
- 否则 → `INSTANCE_CHANGED`，保留集合。
- 无论哪种，都发 `ServiceChangedEvent` 通知订阅者。

**用 `computeIfPresent` 而非「get 再改」的意义**：整个「读-改-判空-删除」在一个原子操作里完成，避免并发下的竞态（比如刚判空又被别的线程加了 clientId）。

---

## 6. 维护订阅者索引

### 6.1 addSubscriberIndexes —— 只有首次订阅才发事件

```java
private void addSubscriberIndexes(Service service, String clientId) {
    Set<String> clientIds = subscriberIndexes.computeIfAbsent(service, key -> new ConcurrentHashSet<>());
    // Fix #5404, Only first time add need notify event.
    if (clientIds.add(clientId)) {
        NotifyCenter.publishEvent(new ServiceEvent.ServiceSubscribedEvent(service, clientId));
    }
}
```

（L158-165）关键在 `if (clientIds.add(clientId))`：`Set.add` 返回 `true` 表示「这个 clientId 之前不在集合里」，即**首次订阅**，才发 `ServiceSubscribedEvent`。

**为什么？（Fix #5404）** 客户端可能重复发起订阅（重连、重复调用），若每次都发 `ServiceSubscribedEvent` 会导致重复推送。用 Set 的「是否真的新增」做幂等门闩，只在首次订阅时触发一次推送。

### 6.2 removeSubscriberIndexes —— 空则删 key

```java
private void removeSubscriberIndexes(Service service, String clientId) {
    subscriberIndexes.computeIfPresent(service, (s, clientIds) -> {
        clientIds.remove(clientId);
        return clientIds.isEmpty() ? null : clientIds;   // 空则移除 key
    });
}
```

（L167-172）与发布者删除对称，但**不发任何事件**——取消订阅只是不再接收推送，无需通知其他人。

---

## 7. 连接断开的批量清理

```java
private void handleClientDisconnect(ClientOperationEvent.ClientReleaseEvent event) {
    Client client = event.getClient();
    for (Service each : client.getAllSubscribeService()) {        // ① 清所有订阅索引
        removeSubscriberIndexes(each, client.getClientId());
    }
    DeregisterInstanceReason reason = event.isNative()            // ② 区分断开原因
            ? DeregisterInstanceReason.NATIVE_DISCONNECTED
            : DeregisterInstanceReason.SYNCED_DISCONNECTED;
    long currentTimeMillis = System.currentTimeMillis();
    for (Service each : client.getAllPublishedService()) {        // ③ 清所有发布索引
        removePublisherIndexes(each, client.getClientId());
        InstancePublishInfo instance = client.getInstancePublishInfo(each);
        NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(...)); // ④ 发注销追踪事件
    }
}
```

（L103-120）连接断开时，`ClientReleaseEvent` 携带整个 `Client` 对象（第 03 篇提到），因此这里能：

1. **遍历该 Client 的所有订阅服务**（`getAllSubscribeService`），逐个从订阅索引移除（L105-107）。
2. **区分断开原因**（L108-110）：`isNative` 为 true 是本机连接断开（`NATIVE_DISCONNECTED`），false 是 Distro 同步过来的连接失效（`SYNCED_DISCONNECTED`）。
3. **遍历该 Client 的所有发布服务**（`getAllPublishedService`），逐个从发布索引移除（会触发 `ServiceChangedEvent` → 推送）（L112-113）。
4. 为每个被摘除的实例发 `DeregisterInstanceTraceEvent`（L115-118），用于可观测/审计。

**这正体现了第 03 篇「实例挂 Client」的收益**：连接断开只需拿到一个 Client 对象，遍历它自己的两个集合即可完成全部清理，无需扫描全局。

---

## 8. 对外查询接口

倒排索引维护好后，对外提供 O(1) 查询：

```java
public Collection<String> getAllClientsRegisteredService(Service service) {   // 谁注册了该服务
    Set<String> publishers = publisherIndexes.get(service);
    return publishers != null ? publishers : new ConcurrentHashSet<>();
}

public Collection<String> getAllClientsSubscribeService(Service service) {    // 谁订阅了该服务
    Set<String> subscribers = subscriberIndexes.get(service);
    return subscribers != null ? subscribers : new ConcurrentHashSet<>();
}

public Collection<Service> getSubscribedService() {                          // 所有被订阅的服务
    return subscriberIndexes.keySet();
}
```

| 方法 | 行号 | 用途 |
|------|------|------|
| `getAllClientsRegisteredService` | L57-60 | 组装服务实例列表时，查所有提供者 |
| `getAllClientsSubscribeService` | L62-65 | 推送时，查所有订阅者 → 挨个推送（第 05 篇） |
| `getSubscribedService` | L67-69 | 遍历所有被订阅服务 |
| `removePublisherIndexesByEmptyService` | L76-81 | 清理无实例的空服务索引 |

**空值处理细节**：查不到时返回**新建的空集合**而非 `null`，让调用方无需判空，直接遍历——防御式编程的小优雅。

---

## 9. 关键设计决策

### 9.1 为什么用事件驱动被动维护，而非主动扫描？

- **解耦**：上游（`ClientOperationService`）发事件即可，完全不知道有倒排索引这个消费者。将来新增其他消费者（如统计、审计）零侵入。
- **增量**：每次只处理一条变更（加/删一个 clientId），而非全量重建，开销极小。
- **一致性**：索引更新与业务操作通过同一事件流串联，天然对齐。

### 9.2 为什么索引更新时顺带发 ServiceChangedEvent？

倒排索引是「服务实例是否变化」的**最权威、最集中**的判定点：只有它知道「加了 clientId 导致服务首次出现（ADD_SERVICE）」还是「删光了导致服务消失（DELETE_SERVICE）」。因此把「发推送信号」的职责放在这里最合适——推送模块（第 05 篇）只需订阅 `ServiceChangedEvent`。

### 9.3 为什么订阅索引首次才发事件（Fix #5404）？

幂等。重复订阅不应触发重复推送。用 `Set.add` 的布尔返回值天然实现「首次」判定，简洁可靠。

### 9.4 为什么 value 存 clientId 而非 Client 对象？

避免倒排索引持有 Client 强引用，防止 Client 已释放但因被索引引用而无法回收。存 clientId，需要时再回 `ClientManager` 查，生命周期清晰。

---

## 10. 线程安全分析

| 组件 | 机制 | 说明 |
|------|------|------|
| `publisherIndexes`/`subscriberIndexes` | `ConcurrentHashMap` | 顶层 Map 并发安全 |
| value 集合 | `ConcurrentHashSet` | 集合内部增删并发安全 |
| 加索引 | `computeIfAbsent(...).add(...)` | 原子建集合 + 安全 add |
| 删索引 | `computeIfPresent(..., (k,v)->...)` | 「读-改-判空-删 key」在一个原子块内 |
| 事件处理 | `SmartSubscriber` 单订阅者串行 | 同类事件按发布顺序处理 |

**核心技巧**：删除路径统一用 `computeIfPresent` 并在 lambda 里 `return 空?null:集合`，把「集合空了就删掉 key」这一复合判断做成原子操作，杜绝了「判空后又被并发加入」的竞态。

---

## 11. 完整数据流转时间线

以「客户端 A 注册 order-service（该服务此前不存在）」为例：

```
T0  第02篇：EphemeralClientOperationServiceImpl.registerInstance()
      publishEvent(ClientRegisterServiceEvent(order-service, clientA))
      │
T1  NotifyCenter 异步分发 → ClientServiceIndexesManager.onEvent()
      │  event instanceof ClientOperationEvent → handleClientOperation()
      │
T2  handleClientOperation:
      │  instanceof ClientRegisterServiceEvent → addPublisherIndexes(order-service, clientA)
      │
T3  addPublisherIndexes:
      │  publisherIndexes.containsKey(order-service)? 否 → serviceChangedType = ADD_SERVICE
      │  publishEvent(ServiceChangedEvent(order-service, ADD_SERVICE, true))  ──┐
      │  publisherIndexes: {order-service → {clientA}}                          │
      │                                                                         ▼
T4                                                            第05篇：推送模块收到 ServiceChangedEvent
      │                                                       查 getAllClientsSubscribeService(order-service)
      │                                                       → 挨个推送给订阅者
```

若之后客户端 B 也注册 order-service：T3 时 `containsKey` 为 true → `INSTANCE_CHANGED`，索引变为 `{order-service → {clientA, clientB}}`。

---

## 12. 总结

1. **定位**：`ClientServiceIndexesManager` 把「Client→Service」的正排反转为「Service→clientId 集合」的倒排，支撑 O(1) 的服务发现查询。
2. **两张表**：`publisherIndexes`（谁注册）与 `subscriberIndexes`（谁订阅），value 只存 clientId。
3. **事件驱动**：作为 `SmartSubscriber` 订阅第 02 篇的 5 种 `ClientOperationEvent`，被动增量维护，与上游彻底解耦。
4. **顺带发推送信号**：加/删发布索引时计算 `ADD_SERVICE`/`INSTANCE_CHANGED`/`DELETE_SERVICE`，发 `ServiceChangedEvent` 驱动第 05 篇推送。
5. **订阅幂等**：靠 `Set.add` 返回值实现「首次订阅才发 `ServiceSubscribedEvent`」（Fix #5404）。
6. **断开批量清理**：`ClientReleaseEvent` 携带 Client，遍历其发布/订阅集合一次性清理，印证「实例挂 Client」的收益。
7. **并发技巧**：删除统一用 `computeIfPresent` + 「空则返回 null」，把复合判断做成原子操作。

下一篇（第 05 篇）将讲 **服务变更推送机制**：推送模块如何订阅本篇发出的 `ServiceChangedEvent`，通过「延迟合并 + 异步执行」把短时间内的多次变更聚合，再推送给 `getAllClientsSubscribeService` 查出的订阅者。

---

## 13. 文件索引表

| 文件 | 关键内容 | 关键行号 |
|------|----------|----------|
| [ClientServiceIndexesManager.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/index/ClientServiceIndexesManager.java) | 事件驱动倒排索引 | 两张倒排表 L49-51；构造注册 L53-55；查询接口 L57-69；removeEmpty L76-81；subscribeTypes L83-92；onEvent L94-101；断开清理 L103-120；操作分派 L122-134；addPublisher L136-145；removePublisher L147-156；addSubscriber L158-165；removeSubscriber L167-172 |

关联文件（前序篇章）：
- 事件来源：[ClientOperationEvent.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/event/client/ClientOperationEvent.java)（第 02/03 篇）
- 事件发出方：[EphemeralClientOperationServiceImpl.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/impl/EphemeralClientOperationServiceImpl.java)（第 02 篇）

---

> 系列导航：
> - [第 01 篇：gRPC 请求处理链路](./Naming服务端-01-gRPC请求处理链路.md)
> - [第 02 篇：客户端操作服务](./Naming服务端-02-客户端操作服务.md)
> - [第 03 篇：数据模型（ServiceManager / Client 抽象）](./Naming服务端-03-数据模型.md)
> - **第 04 篇：事件驱动倒排索引（本篇）**
> - 第 05 篇：服务变更推送机制*（待续）*
