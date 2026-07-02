# Nacos 客户端重试机制源代码分析

> **原则声明**：本文档所有结论均来源于 Nacos 3.2.1-SNAPSHOT 源代码的实际引用，不含任何猜测性描述。每一段分析都附有源码文件路径和行号。

---

## 目录

1. [为什么需要重试机制](#1-为什么需要重试机制)
2. [哪些操作需要重做，哪些不需要](#2-哪些操作需要重做哪些不需要)
3. [RedoData 状态机](#3-redodata-状态机)
4. [NamingGrpcRedoService——客户端重做核心](#4-naminggrpcredoservice客户端重做核心)
5. [RedoScheduledTask——定时重做执行器](#5-redoscheduledtask定时重做执行器)
6. [完整操作流程](#6-完整操作流程)
7. [服务端证据：为什么断连后状态会丢失](#7-服务端证据为什么断连后状态会丢失)
8. [总结](#8-总结)

---

## 1. 为什么需要重试机制

### 1.1 核心原因：临时实例和订阅是连接级状态

Nacos 2.x/3.x 使用 gRPC 双向流通信。在服务端，"一个存活 gRPC 连接对应一个 connection-based Client，该连接上的所有发布和订阅请求都会更新同一个 Client 对象"。

> **规范证据**：
> [naming-consistency-client-spec.md](file:///Users/mmhm/IdeaProjects/nacos/specs/zh-cn/naming/naming-consistency-client-spec.md) L27-29：
> "对于 gRPC 通信，一个存活连接对应一个 connection-based Client。该连接上的所有发布和订阅请求都会更新同一个 Client 对象。"

这意味着服务端的 Client 对象（包含 registered instances 和 subscribers）的生命周期与 gRPC 连接绑定。一旦连接断开，服务端就释放整个 Client 对象，其上挂载的所有**注册实例和订阅关系**全部丢失。

### 1.2 服务端断连时的实际行为

从 [ConnectionBasedClientManager.clientDisconnected()](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/manager/impl/ConnectionBasedClientManager.java) L105-117：

```java
public boolean clientDisconnected(String clientId) {
    Loggers.SRV_LOG.info("Client connection {} disconnect, remove instances and subscribers",
        clientId);
    ConnectionBasedClient client = clients.remove(clientId);  // 从 Map 中移除 Client
    if (null == client) {
        return true;
    }
    client.release();  // 清理 publishers 和 subscribers
    boolean isResponsible = isResponsibleClient(client);
    NotifyCenter
        .publishEvent(new ClientOperationEvent.ClientReleaseEvent(client, isResponsible));
    NotifyCenter.publishEvent(new ClientEvent.ClientDisconnectEvent(client, isResponsible));
    return true;
}
```

日志明确写的是 `"remove instances and subscribers"`——**实例和订户一起删除**。

从 [AbstractClient](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/AbstractClient.java) L46-50 可以看到 Client 中维护的两个核心 Map：

```java
protected final ConcurrentHashMap<Service, InstancePublishInfo> publishers =
    new ConcurrentHashMap<>(16, 0.75f, 1);

protected final ConcurrentHashMap<Service, Subscriber> subscribers =
    new ConcurrentHashMap<>(16, 0.75f, 1);
```

`client.release()` 会清空这两个 Map。因此：

| 操作类型 | 服务端存储位置 | 断连后 | 不重做的后果 |
|----------|-------------|--------|------------|
| 临时实例注册 | Client.publishers | 清除 | 实例从服务发现消失 |
| 服务订阅 | Client.subscribers | 清除 | 收不到服务变更推送 |

### 1.3 一个具体的时间线

如果客户端注册了 5 个临时实例、订阅了 3 个服务，然后 gRPC 连接断开又重连：

```
t0  客户端注册实例 A,B,C,D,E + 订阅服务 X,Y,Z
    → 服务端 Client 对象中存在 5 个 publishers + 3 个 subscribers

t1  gRPC 连接断开
    → 服务端 clientDisconnected()
    → clients.remove(connectionId) → Client 对象被移除
    → 5 个实例从服务发现消失
    → 3 个订阅失效

t2  gRPC 重连成功
    → 新的 connectionId，新的 Client 对象（空的 publishers + subscribers）
    → 如果不做 redo：永远丢失 5 个实例 + 3 个订阅
```

RedoService 的作用就是在 t2 之后自动把 t0 的状态恢复回来。

---

## 2. 哪些操作需要重做，哪些不需要

### 2.1 需要重做的操作

从 `NamingGrpcClientProxy` 的源码可以看到明确的分类。每当执行这些操作时，先缓存到 RedoService，再发送 gRPC 请求：

| 操作 | 缓存方法 | 代码位置 |
|------|---------|---------|
| 临时实例注册 | `cacheInstanceForRedo()` | [NamingGrpcClientProxy.registerServiceForEphemeral()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L337-344 |
| 批量临时实例注册 | `cacheInstanceForRedo(instances)` | [NamingGrpcClientProxy.batchRegisterService()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L349-354 |
| 临时实例注销 | `instanceDeregister()` | [NamingGrpcClientProxy.deregisterServiceForEphemeral()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L527 |
| 服务订阅 | `cacheSubscriberForRedo()` | [NamingGrpcClientProxy.subscribe()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L631-634 |
| 取消订阅 | `subscriberDeregister()` | [NamingGrpcClientProxy.unsubscribe()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L665-669 |

> **规范证据**：
> [client-local-cache-redo-spec.md](file:///Users/mmhm/IdeaProjects/nacos/specs/zh-cn/client/client-local-cache-redo-spec.md) L108-113：
> "Naming redo 覆盖：临时实例注册；批量临时实例注册；服务订阅；fuzzy watch 一致性状态。"

### 2.2 不需要重做的操作

#### 2.2.1 持久实例（Persistent Instance）—— 依赖 CP 协议，状态已持久化

从 [NamingGrpcClientProxy.registerService()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L317-330：

```java
@Override
public void registerService(String serviceName, String groupName, Instance instance)
    throws NacosException {
    if (instance.isEphemeral()) {
        registerServiceForEphemeral(serviceName, groupName, instance);  // 走 redo
    } else {
        doRegisterServiceForPersistent(serviceName, groupName, instance);  // 不走 redo
    }
}
```

持久实例的 `doRegisterServiceForPersistent()`（L478-485）直接发送 `PersistentInstanceRequest`，不经过任何 redo 缓存：

```java
public void doRegisterServiceForPersistent(String serviceName, String groupName,
    Instance instance) throws NacosException {
    PersistentInstanceRequest request =
        new PersistentInstanceRequest(namespaceId, serviceName, groupName,
            NamingRemoteConstants.REGISTER_INSTANCE, instance);
    requestToServer(request, Response.class);  // 仅发送请求，不操作 redo
}
```

**为什么不需要？**

> **规范证据**：
> [naming-consistency-client-spec.md](file:///Users/mmhm/IdeaProjects/nacos/specs/zh-cn/naming/naming-consistency-client-spec.md) L72-77：
> "持久服务拥有偏 CP 的实例状态。注册、注销和更新操作会写入 persistent service group，并由 persistent client operation service apply。"

持久实例的注册/注销通过 Raft 共识协议写入日志，一旦多数节点确认，状态就持久化了。连接断开不会丢失，重连后持久实例依然存在。

**规范也明确指出持久 client 不支持 subscriber state**（L79："持久 client 不支持 subscriber state。订阅属于临时 client 行为。"）。

#### 2.2.2 查询操作 —— 无状态，幂等

[NamingGrpcClientProxy](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) 中的以下查询方法不涉及任何 RedoService 调用：

| 方法 | 行号 | 原因 |
|------|------|------|
| `queryInstancesOfService()` | L571-579 | 只读查询，不改变服务端状态 |
| `getServiceList()` | L600-615 | 只读查询 |
| `serverHealthy()` | L694-697 | 只检查连接状态 |
| `isSubscribed()` | L671-675 | 仅读取 redo 缓存判断，不发请求 |

查询操作本质上是无状态的——即使断连期间查询失败，重连后再查一次即可，不存在"状态丢失"问题。

#### 2.2.3 空实现方法 —— 不产生任何操作

[NamingGrpcClientProxy](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) 中以下方法是空实现：

```java
// L565-568
@Override
public void updateInstance(String serviceName, String groupName, Instance instance)
    throws NacosException { }

// L581-584
@Override
public Service queryService(String serviceName, String groupName) throws NacosException {
    return null;
}

// L586-588
@Override
public void createService(Service service, AbstractSelector selector) throws NacosException { }

// L590-593
@Override
public boolean deleteService(String serviceName, String groupName) throws NacosException {
    return false;
}

// L595-597
@Override
public void updateService(Service service, AbstractSelector selector) throws NacosException { }
```

这些操作在 gRPC 代理中根本不会发出任何请求，自然不需要 redo。

---

## 3. RedoData 状态机

RedoData 是客户端重做机制的数据载体，位于 [RedoData](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/redo/data/RedoData.java) L61-196。

### 3.1 三个核心字段

```java
// L67: 期望的最终状态 —— true=期望服务端有这条数据（已注册），false=期望服务端没有（已注销）
private volatile boolean expectedRegistered;

// L72: 是否已在服务端注册成功
private volatile boolean registered;

// L77: 是否正在注销
private volatile boolean unregistering;
```

### 3.2 状态转换方法

```java
// L117-120: 注册成功时调用 —— 设置 registered=true, unregistering=false
public void registered() {
    this.registered = true;
    this.unregistering = false;
}

// L122-125: 注销完成时调用 —— 设置 registered=false, unregistering=true
public void unregistered() {
    this.registered = false;
    this.unregistering = true;
}
```

### 3.3 RedoType 决策逻辑

`getRedoType()` 方法（L143-153）根据三个字段的组合返回四种 RedoType：

```java
public RedoType getRedoType() {
    if (isRegistered() && !isUnregistering()) {
        return expectedRegistered ? RedoType.NONE : RedoType.UNREGISTER;
    } else if (isRegistered() && isUnregistering()) {
        return RedoType.UNREGISTER;
    } else if (!isRegistered() && !isUnregistering()) {
        return RedoType.REGISTER;
    } else {
        return expectedRegistered ? RedoType.REGISTER : RedoType.REMOVE;
    }
}
```

完整状态表：

| registered | unregistering | expectedRegistered | RedoType | 含义 |
|:---:|:---:|:---:|:---:|---|
| true | false | true | **NONE** | 正常状态，无需重做 |
| true | false | false | **UNREGISTER** | 已注册但期望注销，需重做注销 |
| true | true | - | **UNREGISTER** | 正在注销中，需继续重做注销 |
| false | false | true | **REGISTER** | 未注册但期望注册（断连丢失），需重做注册 |
| false | true | true | **REGISTER** | 期望注册，需重做注册 |
| false | true | false | **REMOVE** | 期望注销且已完成，可移除缓存 |

### 3.4 RedoType 枚举

```java
// L155-175
public enum RedoType {
    REGISTER,    // 需要重做注册操作
    UNREGISTER,  // 需要重做注销操作
    NONE,        // 无需任何操作
    REMOVE;      // 从 redoDataMap 中移除此数据
}
```

---

## 4. NamingGrpcRedoService——客户端重做核心

[NamingGrpcRedoService](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/redo/NamingGrpcRedoService.java) 是 Naming 模块的重做服务实现。它承担两个角色：

1. **ConnectionEventListener**：监听 gRPC 连接的建立与断开
2. **Redo 数据仓库**：维护 instances 和 subscribers 两份缓存

### 4.1 数据结构

```java
// L155-156: 实例缓存，key = groupedServiceName (group@@service)
private final ConcurrentMap<String, InstanceRedoData> registeredInstances =
    new ConcurrentHashMap<>();

// L162: 订阅者缓存，key = ServiceInfo.getKey(groupedName, cluster)
private final ConcurrentMap<String, SubscriberRedoData> subscribes = new ConcurrentHashMap<>();
```

### 4.2 构造与初始化

```java
// L186-200
public NamingGrpcRedoService(NamingGrpcClientProxy clientProxy,
    NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder,
    NacosClientProperties properties) {
    setProperties(properties);
    this.namingFuzzyWatchServiceListHolder = namingFuzzyWatchServiceListHolder;
    this.redoExecutor = new ScheduledThreadPoolExecutor(redoThreadCount,
        new NameThreadFactory(REDO_THREAD_NAME));
    // 启动定时重做任务，固定延迟 redoDelayTime（默认 3000ms）
    this.redoExecutor.scheduleWithFixedDelay(new RedoScheduledTask(clientProxy, this),
        redoDelayTime, redoDelayTime,
        TimeUnit.MILLISECONDS);
}
```

关键参数（L202-207）：

```java
redoDelayTime = properties.getLong(PropertyKeyConst.REDO_DELAY_TIME,
    Constants.DEFAULT_REDO_DELAY_TIME);         // 默认 3000ms
redoThreadCount = properties.getInteger(PropertyKeyConst.REDO_DELAY_THREAD_COUNT,
    Constants.DEFAULT_REDO_THREAD_COUNT);        // 默认 1
```

### 4.3 断连回调：onDisConnect——重做机制的触发器

```java
// L239-258
@Override
public void onDisConnect(Connection connection) {
    connected = false;
    LogUtils.NAMING_LOGGER.warn("Grpc connection disconnect, mark to redo");
    // Step 1: 所有已注册实例标记为未注册
    synchronized (registeredInstances) {
        registeredInstances.values()
            .forEach(instanceRedoData -> instanceRedoData.setRegistered(false));
    }
    // Step 2: 所有订阅标记为未注册
    synchronized (subscribes) {
        subscribes.values()
            .forEach(subscriberRedoData -> subscriberRedoData.setRegistered(false));
    }
    // Step 3: 重置模糊监听一致性状态
    synchronized (namingFuzzyWatchServiceListHolder) {
        namingFuzzyWatchServiceListHolder.resetConsistenceStatus();
    }
    LogUtils.NAMING_LOGGER.warn("mark to redo completed");
}
```

**关键动作**：将所有缓存数据的 `registered` 设为 `false`。这使得 `getRedoType()` 返回值从 `NONE` 变为 `REGISTER`，定时任务在下一次轮询时会发现这些数据需要重做。

### 4.4 连接恢复回调：onConnected

```java
// L221-225
@Override
public void onConnected(Connection connection) {
    connected = true;
    LogUtils.NAMING_LOGGER.info("Grpc connection connect");
}
```

仅设置 `connected` 标志为 `true`。定时任务在下一次执行时会检查此标志。

### 4.5 实例注册缓存流程

**缓存**（L270-276）：

```java
public void cacheInstanceForRedo(String serviceName, String groupName, Instance instance) {
    String key = NamingUtils.getGroupedName(serviceName, groupName);
    InstanceRedoData redoData = InstanceRedoData.build(serviceName, groupName, instance);
    synchronized (registeredInstances) {
        registeredInstances.put(key, redoData);
    }
}
```

**标记注册成功**（L315-323）：

```java
public void instanceRegistered(String serviceName, String groupName) {
    String key = NamingUtils.getGroupedName(serviceName, groupName);
    synchronized (registeredInstances) {
        InstanceRedoData redoData = registeredInstances.get(key);
        if (null != redoData) {
            redoData.registered();  // → registered=true, unregistering=false
        }
    }
}
```

**标记开始注销**（L336-345）：

```java
public void instanceDeregister(String serviceName, String groupName) {
    String key = NamingUtils.getGroupedName(serviceName, groupName);
    synchronized (registeredInstances) {
        InstanceRedoData redoData = registeredInstances.get(key);
        if (null != redoData) {
            redoData.setUnregistering(true);
            redoData.setExpectedRegistered(false);
        }
    }
}
```

**标记注销完成**（L358-366）：

```java
public void instanceDeregistered(String serviceName, String groupName) {
    String key = NamingUtils.getGroupedName(serviceName, groupName);
    synchronized (registeredInstances) {
        InstanceRedoData redoData = registeredInstances.get(key);
        if (null != redoData) {
            redoData.unregistered();  // → registered=false, unregistering=true
        }
    }
}
```

### 4.6 查找需要重做的数据

`findInstanceRedoData()`（L400-410）和 `findSubscriberRedoData()`（L521-531）遍历缓存，通过 `isNeedRedo()`（即 `getRedoType() != NONE`）过滤出需要重做的条目：

```java
public Set<InstanceRedoData> findInstanceRedoData() {
    Set<InstanceRedoData> result = new HashSet<>();
    synchronized (registeredInstances) {
        for (InstanceRedoData each : registeredInstances.values()) {
            if (each.isNeedRedo()) {       // ← getRedoType() != NONE
                result.add(each);
            }
        }
    }
    return result;
}
```

---

## 5. RedoScheduledTask——定时重做执行器

[RedoScheduledTask](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/redo/RedoScheduledTask.java) 是定时任务，周期 3 秒（默认）。

### 5.1 主循环

```java
// L45-57
@Override
public void run() {
    if (!redoService.isConnected()) {
        LogUtils.NAMING_LOGGER.warn("Grpc Connection is disconnect, skip current redo task");
        return;                          // 连接未恢复时跳过
    }
    try {
        redoForInstances();             // Step 1: 重做实例操作
        redoForSubscribes();            // Step 2: 重做订阅操作
    } catch (Exception e) {
        LogUtils.NAMING_LOGGER.warn("Redo task run with unexpected exception: ", e);
    }
}
```

关键设计：执行前先检查 `redoService.isConnected()`。连接未恢复时跳过所有重做操作，避免无意义的重试。

### 5.2 实例重做

```java
// L71-96
private void redoForInstance(InstanceRedoData redoData) throws NacosException {
    NamingRedoData.RedoType redoType = redoData.getRedoType();
    switch (redoType) {
        case REGISTER:
            if (isClientDisabled()) { return; }
            processRegisterRedoType(redoData, serviceName, groupName);
            break;
        case UNREGISTER:
            if (isClientDisabled()) { return; }
            clientProxy.doDeregisterService(serviceName, groupName, redoData.get());  // 重做注销
            break;
        case REMOVE:
            redoService.removeInstanceForRedo(serviceName, groupName);  // 从缓存移除
            break;
        default:          // NONE → 不执行任何操作
    }
}
```

批量注册的分支处理（L98-108）：

```java
private void processRegisterRedoType(InstanceRedoData redoData, String serviceName,
    String groupName) throws NacosException {
    if (redoData instanceof BatchInstanceRedoData) {
        BatchInstanceRedoData batchInstanceRedoData = (BatchInstanceRedoData) redoData;
        clientProxy.doBatchRegisterService(serviceName, groupName,
            batchInstanceRedoData.getInstances());
        return;
    }
    clientProxy.doRegisterService(serviceName, groupName, redoData.get());
}
```

### 5.3 订阅重做

```java
// L122-148
private void redoForSubscribe(SubscriberRedoData redoData) throws NacosException {
    switch (redoData.getRedoType()) {
        case REGISTER:
            if (isClientDisabled()) { return; }
            clientProxy.doSubscribe(serviceName, groupName, cluster);
            break;
        case UNREGISTER:
            if (isClientDisabled()) { return; }
            clientProxy.doUnsubscribe(serviceName, groupName, cluster);
            break;
        case REMOVE:
            redoService.removeSubscriberForRedo(redoData.getServiceName(),
                redoData.getGroupName(), redoData.get());
            break;
        default:          // NONE → 不执行任何操作
    }
}
```

---

## 6. 完整操作流程

### 6.1 临时实例注册的完整生命周期

以 `registerService()` 为例，展示从注册到重做的完整链路。

**NamingGrpcClientProxy**（L337-344）：

```java
private void registerServiceForEphemeral(String serviceName, String groupName,
    Instance instance) throws NacosException {
    // Step 1: 先缓存 redo（缓存在前是关键设计）
    redoService.cacheInstanceForRedo(serviceName, groupName, instance);
    // Step 2: 再发送 gRPC 请求
    doRegisterService(serviceName, groupName, instance);
}
```

`doRegisterService()`（L462-468）：

```java
public void doRegisterService(String serviceName, String groupName, Instance instance)
    throws NacosException {
    InstanceRequest request = new InstanceRequest(namespaceId, serviceName, groupName,
        NamingRemoteConstants.REGISTER_INSTANCE, instance);
    requestToServer(request, Response.class);
    redoService.instanceRegistered(serviceName, groupName);  // 标记 registered=true
}
```

**完整时间线**：

```
1. cacheInstanceForRedo()
   → RedoData{registered=false, unregistering=false, expectedRegistered=true}
   → getRedoType() = REGISTER (因为 registered=false)

2. doRegisterService() 成功
   → instanceRegistered() → registered.registered()
   → RedoData{registered=true, unregistering=false, expectedRegistered=true}
   → getRedoType() = NONE（无需重做）

3. 如果 gRPC 连接断开
   → onDisConnect() → setRegistered(false)
   → RedoData{registered=false, unregistering=false, expectedRegistered=true}
   → getRedoType() = REGISTER

4. gRPC 重连后 RedoScheduledTask 检测到 REGISTER
   → doRegisterService() 重新注册
   → instanceRegistered() → getRedoType() = NONE
```

### 6.2 临时实例注销的完整生命周期

**NamingGrpcClientProxy.deregisterServiceForEphemeral()**（L513-529）：

```java
private void deregisterServiceForEphemeral(String serviceName, String groupName,
    Instance instance) throws NacosException {
    String key = NamingUtils.getGroupedName(serviceName, groupName);
    InstanceRedoData instanceRedoData = redoService.getRegisteredInstancesByKey(key);
    if (instanceRedoData instanceof BatchInstanceRedoData) {
        // 批量模式：计算差集后重新批量注册
        batchDeregisterService(serviceName, groupName, instances);
    } else {
        redoService.instanceDeregister(serviceName, groupName);  // 标记注销中
        doDeregisterService(serviceName, groupName, instance);
    }
}
```

`doDeregisterService()`（L540-546）：

```java
public void doDeregisterService(String serviceName, String groupName, Instance instance)
    throws NacosException {
    InstanceRequest request = new InstanceRequest(namespaceId, serviceName, groupName,
        NamingRemoteConstants.DE_REGISTER_INSTANCE, instance);
    requestToServer(request, Response.class);
    redoService.instanceDeregistered(serviceName, groupName);  // 标记注销完成
}
```

**完整时间线**：

```
1. instanceDeregister()
   → setUnregistering(true), setExpectedRegistered(false)
   → RedoData{registered=true, unregistering=true, expectedRegistered=false}
   → getRedoType() = UNREGISTER

2. doDeregisterService() 成功
   → instanceDeregistered() → unregistered()
   → RedoData{registered=false, unregistering=true, expectedRegistered=false}
   → getRedoType() = REMOVE

3. 下一次 RedoScheduledTask 扫描
   → REMOVE → removeInstanceForRedo() → 从缓存中删除
```

### 6.3 服务订阅的完整生命周期

**NamingGrpcClientProxy.subscribe()**（L626-635）：

```java
@Override
public ServiceInfo subscribe(String serviceName, String groupName, String clusters)
    throws NacosException {
    redoService.cacheSubscriberForRedo(serviceName, groupName, clusters);   // 先缓存
    return doSubscribe(serviceName, groupName, clusters);                    // 再发请求
}
```

`doSubscribe()`（L646-655）：

```java
public ServiceInfo doSubscribe(String serviceName, String groupName, String clusters)
    throws NacosException {
    SubscribeServiceRequest request =
        new SubscribeServiceRequest(namespaceId, groupName, serviceName, clusters, true);
    SubscribeServiceResponse response =
        requestToServer(request, SubscribeServiceResponse.class);
    redoService.subscriberRegistered(serviceName, groupName, clusters);  // 标记 subscribed
    return response.getServiceInfo();
}
```

**取消订阅**（L660-669）：

```java
@Override
public void unsubscribe(String serviceName, String groupName, String clusters)
    throws NacosException {
    redoService.subscriberDeregister(serviceName, groupName, clusters);  // 标记注销中
    doUnsubscribe(serviceName, groupName, clusters);
}
```

### 6.4 批量注册与注销的特殊处理

**批量注册**（L349-354）：

```java
@Override
public void batchRegisterService(String serviceName, String groupName, List<Instance> instances)
    throws NacosException {
    redoService.cacheInstanceForRedo(serviceName, groupName, instances);
    doBatchRegisterService(serviceName, groupName, instances);
}
```

批量注册使用 `BatchInstanceRedoData` 而非 `InstanceRedoData`，重做时调用 `doBatchRegisterService()`。

**批量注销**采用"差集计算"策略（L362-372）：从已注册列表中减去待注销实例，将剩余实例重新批量注册。

---

## 7. 服务端证据：为什么断连后状态会丢失

### 7.1 订阅绑定 connectionId

[SubscribeServiceRequestHandler](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/remote/rpc/handler/SubscribeServiceRequestHandler.java) L96-104：

```java
Subscriber subscriber =
    new Subscriber(meta.getClientIp(), meta.getClientVersion(), app, meta.getClientIp(),
        namespaceId, groupedServiceName, 0, request.getClusters());
if (request.isSubscribe()) {
    clientOperationService.subscribeService(service, subscriber, meta.getConnectionId());
    //                                                            ^^^^^^^^^^^^^^^^^^^^
    //                                                    订阅挂在 connectionId 对应的 Client 上
}
```

### 7.2 订阅存储在 Client 对象的 subscribers Map 中

[EphemeralClientOperationServiceImpl](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/impl/EphemeralClientOperationServiceImpl.java) L130-138：

```java
@Override
public void subscribeService(Service service, Subscriber subscriber, String clientId) {
    Service singleton =
        ServiceManager.getInstance().getSingletonIfExist(service).orElse(service);
    Client client = clientManager.getClient(clientId);               // clientId = connectionId
    checkClientIsLegal(client, clientId);
    client.addServiceSubscriber(singleton, subscriber);              // 存入 Client.subscribers
    client.setLastUpdatedTime();
    NotifyCenter.publishEvent(
        new ClientOperationEvent.ClientSubscribeServiceEvent(singleton, clientId));
}
```

### 7.3 服务端断连时 Client 被移除

`ConnectionBasedClientManager.clientDisconnected()`（L105-117）：
- `clients.remove(clientId)` —— 从 Map 中移除整个 Client
- `client.release()` —— 清空 publishers 和 subscribers
- 发布 `ClientReleaseEvent`，触发 `ClientServiceIndexesManager.handleClientDisconnect()` 清除订阅索引

### 7.4 持久 client 不支持 subscriber state

> **规范证据**：
> [naming-consistency-client-spec.md](file:///Users/mmhm/IdeaProjects/nacos/specs/zh-cn/naming/naming-consistency-client-spec.md) L79：
> "持久 client 不支持 subscriber state。订阅属于临时 client 行为。"

---

## 8. 总结

### 8.1 重做机制决策树

```
NamingGrpcClientProxy 上的操作
    │
    ├── 临时实例注册/注销          → ✅ 缓存到 RedoService
    │   └── 原因：连接级状态，断连丢失
    │
    ├── 批量临时实例注册/注销      → ✅ 缓存到 RedoService
    │   └── 原因：同上
    │
    ├── 服务订阅/取消订阅          → ✅ 缓存到 RedoService
    │   └── 原因：订阅也是连接级状态，断连后服务端不再推送
    │
    ├── 持久实例注册/注销          → ❌ 不缓存
    │   └── 原因：CP/Raft 协议持久化，断连不影响
    │
    ├── 查询操作                   → ❌ 不缓存
    │   └── 原因：无状态，幂等
    │
    └── 空实现方法                 → ❌ 不缓存
        └── 原因：不产生任何请求
```

### 8.2 核心设计原则

1. **"缓存在前，请求在后"**：`registerServiceForEphemeral()` 先调用 `cacheInstanceForRedo()` 再调用 `doRegisterService()`，确保即使请求发出后立即断连，redo 缓存中也有记录

2. **断连时批量标记**：`onDisConnect()` 将所有缓存数据的 `registered` 设为 `false`，触发状态机从 `NONE` 转为 `REGISTER`

3. **定时扫描 + 状态机决策**：`RedoScheduledTask` 每 3 秒扫描一次，根据 `getRedoType()` 决定 `REGISTER` / `UNREGISTER` / `REMOVE` / `NONE`

4. **连接感知**：`connected` 标志确保仅在连接恢复后才执行重做，避免无意义的重试

### 8.3 关键文件索引

| 文件 | 相对路径 | 角色 |
|------|---------|------|
| NamingGrpcClientProxy | `client/src/main/java/.../gprc/NamingGrpcClientProxy.java` | 主代理，所有分流逻辑 |
| NamingGrpcRedoService | `client/src/main/java/.../gprc/redo/NamingGrpcRedoService.java` | Redo 核心，连接监听 + 数据仓库 |
| RedoScheduledTask | `client/src/main/java/.../gprc/redo/RedoScheduledTask.java` | 定时重做任务 |
| RedoData | `client/src/main/java/.../redo/data/RedoData.java` | 状态机基类 |
| ConnectionBasedClientManager | `naming/src/main/java/.../client/manager/impl/ConnectionBasedClientManager.java` | 服务端 Client 管理，断连清除 |
| EphemeralClientOperationServiceImpl | `naming/src/main/java/.../service/impl/EphemeralClientOperationServiceImpl.java` | 服务端临时操作实现 |
| SubscribeServiceRequestHandler | `naming/src/main/java/.../remote/rpc/handler/SubscribeServiceRequestHandler.java` | 服务端订阅处理 |
| client-local-cache-redo-spec.md | `specs/zh-cn/client/client-local-cache-redo-spec.md` | 客户端 Redo 规范 |
| naming-consistency-client-spec.md | `specs/zh-cn/naming/naming-consistency-client-spec.md` | Naming 一致性规范 |
