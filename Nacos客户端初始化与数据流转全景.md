# Nacos Naming 客户端完整初始化与数据流转全景

> **原则声明**：本文档所有结论均来源于 Nacos 3.2.1-SNAPSHOT 源代码的实际引用，不含任何猜测性描述。每一段分析都附有源码文件路径和行号。

---

## 目录

1. [为什么需要这张全景图](#1-为什么需要这张全景图)
2. [架构总览：四大组件层次](#2-架构总览四大组件层次)
3. [初始化全流程](#3-初始化全流程)
4. [数据流转全景](#4-数据流转全景)
5. [关键设计决策](#5-关键设计决策)
6. [关闭全流程](#6-关闭全流程)
7. [关键文件索引](#7-关键文件索引)

---

## 1. 为什么需要这张全景图

Nacos Naming 客户端的初始化涉及 **8 个类、6 步顺序、4 种异步线程**。如果不理解初始化顺序和各组件之间的依赖关系，阅读任何单一组件的源码都会产生"这个对象从哪传进来的？""为什么必须先初始化 A 才能初始化 B？"等困惑。

本章作为"阅读地图"，回答以下问题：

- `NacosNamingService.init()` 到底做了什么？按什么顺序？
- `NamingClientProxyDelegate` 构造函数里的 6 步为什么是这个顺序？
- 实例数据从服务端到用户 EventListener 经过了哪些组件？
- 为什么订阅操作需要同时启动 `ServiceInfoUpdateService`（定时拉取）和 gRPC 推送？

---

## 2. 架构总览：四大组件层次

从调用链路看，Nacos Naming 客户端可分为四层：

```
┌────────────────────────────────────────────────────────────────────┐
│  第 1 层：用户 API 层                                              │
│  NacosNamingService                                               │
│  registerInstance / deregisterInstance / subscribe / selectInstances│
├────────────────────────────────────────────────────────────────────┤
│  第 2 层：路由调度层                                               │
│  NamingClientProxyDelegate                                        │
│  决定每次请求走 gRPC 还是 HTTP                                     │
├──────────────────────────┬─────────────────────────────────────────┤
│  第 3 层：通信代理层      │  第 3 层：缓存与事件层                  │
│  NamingGrpcClientProxy   │  ServiceInfoHolder    (中央缓存)        │
│  NamingHttpClientProxy   │  ServiceInfoUpdateService (定时拉取)    │
│                          │  InstancesChangeNotifier (事件路由)      │
│                          │  FailoverReactor       (故障转移)       │
│                          │  NamingGrpcRedoService  (重做)          │
├──────────────────────────┴─────────────────────────────────────────┤
│  第 4 层：基础设施层                                               │
│  NamingServerListManager  SecurityProxy  RpcClient  NotifyCenter   │
└────────────────────────────────────────────────────────────────────┘
```

**为什么分四层？** 这是典型的"关注点分离"设计：
- 第 1 层关注 API 契约（NamingService 接口）
- 第 2 层关注路由决策（临时 vs 持久，gRPC vs HTTP）
- 第 3 层关注通信实现（gRPC 双向流 / HTTP REST）和缓存一致性
- 第 4 层关注通用基础设施，被所有上层共享

---

## 3. 初始化全流程

### 3.1 入口：NacosNamingService.init()

> 源码：[NacosNamingService.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/NacosNamingService.java) L172-198

```java
private void init(Properties properties) throws NacosException {
    // Step 0: 异步预加载耗性能组件（JsonUtils SPI 扫描、ObjectMapper 初始化）
    PreInitUtils.asyncPreLoadCostComponent();

    // Step 1: 配置处理
    final NacosClientProperties nacosClientProperties =
        NacosClientProperties.PROTOTYPE.derive(properties);
    ValidatorUtils.checkInitParam(nacosClientProperties);
    this.namespace = InitUtils.initNamespaceForNaming(nacosClientProperties);
    InitUtils.initSerialization();
    InitUtils.initWebRootContext(nacosClientProperties);

    // Step 2: 创建事件体系
    this.notifierEventScope = UUID.randomUUID().toString();        // ← 事件隔离 ID
    this.changeNotifier = new InstancesChangeNotifier(this.notifierEventScope);
    NotifyCenter.registerToPublisher(InstancesChangeEvent.class, 16384);
    NotifyCenter.registerSubscriber(changeNotifier);

    // Step 3: 创建缓存中心（内含 FailoverReactor + DiskCacheRefresher）
    this.serviceInfoHolder =
        new ServiceInfoHolder(namespace, this.notifierEventScope, nacosClientProperties);

    // Step 4: 创建模糊监听管理器
    NotifyCenter.registerToPublisher(NamingFuzzyWatchNotifyEvent.class, 16384);
    this.namingFuzzyWatchServiceListHolder =
        new NamingFuzzyWatchServiceListHolder(this.notifierEventScope);

    // Step 5: 创建通信代理（核心，内含 6 步子初始化）
    this.clientProxy = new NamingClientProxyDelegate(this.namespace, serviceInfoHolder,
        nacosClientProperties, changeNotifier, namingFuzzyWatchServiceListHolder);
}
```

### 3.2 关键设计决策：为什么 notifierEventScope 用 UUID？

> 源码：[NacosNamingService.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/NacosNamingService.java) L184

```java
this.notifierEventScope = UUID.randomUUID().toString();
```

**决策理由**：同一个 JVM 中可以创建多个 `NacosNamingService` 实例（例如连接不同的 Nacos 集群）。如果没有 scope 隔离，实例 A 的 `InstancesChangeEvent` 会被实例 B 的 `InstancesChangeNotifier` 消费，导致误通知。

在 `InstancesChangeNotifier.scopeMatches()` 中通过比对 scope 来过滤：

```java
// InstancesChangeNotifier 内部逻辑（从 NotifyCenter 事件分发角度）：
// 只有 event.scope() == this.eventScope 的事件才会被处理
```

### 3.3 核心：NamingClientProxyDelegate 的 6 步初始化

> 源码：[NamingClientProxyDelegate.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/NamingClientProxyDelegate.java) L165-212

这是整个客户端最关键的初始化逻辑：

```java
public NamingClientProxyDelegate(String namespace, ServiceInfoHolder serviceInfoHolder,
    NacosClientProperties properties, InstancesChangeNotifier changeNotifier,
    NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder)
    throws NacosException {

    // Step 1: 定时拉取兜底服务
    // 构造器接收 this（NamingClientProxy 接口），
    // 后续 ServiceInfoUpdateService 通过 this 回调 queryInstancesOfService
    this.serviceInfoUpdateService =
        new ServiceInfoUpdateService(properties, serviceInfoHolder, this, changeNotifier);

    // Step 2: 服务端地址管理
    // start() → SPI 加载 ServerListProvider → getServerList()
    // genNextServer() 为 RpcClient 提供轮询取地址的能力
    this.serverListManager = new NamingServerListManager(properties, namespace);
    this.serverListManager.start();

    // Step 3: 缓存中央仓库（外部传入）
    this.serviceInfoHolder = serviceInfoHolder;

    // Step 4: 安全代理
    // SPI 加载鉴权插件 → 立即登录 → 定时刷新 token
    this.securityProxy = new SecurityProxy(this.serverListManager,
        NamingHttpClientManager.getInstance().getNacosRestTemplate());
    initSecurityProxy(properties);

    // Step 5: HTTP 兼容代理（旧版服务端）
    this.httpClientProxy =
        new NamingHttpClientProxy(namespace, securityProxy, serverListManager, properties);

    // Step 6: gRPC 主力代理
    // 建连 + 注册 push handler + 创建 RedoService
    this.grpcClientProxy =
        new NamingGrpcClientProxy(namespace, securityProxy, serverListManager, properties,
            serviceInfoHolder, namingFuzzyWatchServiceListHolder);
}
```

### 3.4 关键设计决策：为什么 Step 1 要先于 Step 6？

**原因：循环依赖。**

`ServiceInfoUpdateService` 的构造器接收 `NamingClientProxy` 接口（this），用于定时拉取时回调 `queryInstancesOfService()`。它只依赖接口，不依赖 gRPC 连接是否已建立。

`NamingGrpcClientProxy` 的构造器（Step 6）内部调用了 `start()`：
- `start()` → `rpcClient.start()` → 建立 gRPC 连接
- gRPC 连接建立后，服务端可以立即推送数据

如果 Step 6 在 Step 1 之前，且 gRPC 连接立即建立、服务端立即推送数据 → `NamingPushRequestHandler` → `serviceInfoHolder.processServiceInfo()` → 发布 `InstancesChangeEvent`。此时 `InstancesChangeNotifier` 可能尚未准备好。

实际上 `InstancesChangeNotifier` 是由外部传入的，在 `NacosNamingService.init()` 中先创建（L185），安全。

### 3.5 NamingGrpcClientProxy.start() 的 6 步内部初始化

> 源码：[NamingGrpcClientProxy.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L281-298

```java
private void start(ServerListFactory serverListFactory, ServiceInfoHolder serviceInfoHolder,
    NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder)
    throws NacosException {
    // Step 1: 注入服务端地址工厂（RpcClient 通过 ServerListFactory 轮询选 server）
    rpcClient.serverListFactory(serverListFactory);

    // Step 2: 注册连接事件监听器（RedoService 同时作为 ConnectionEventListener）
    // 断连时标记所有 redo 数据为未注册，重连后自动重做
    rpcClient.registerConnectionListener(redoService);

    // Step 3: 注册两个服务端推送处理器
    //   - NamingPushRequestHandler：处理服务端主动推送的实例变更
    //   - NamingFuzzyWatchNotifyRequestHandler：处理模糊监听通知
    rpcClient.registerServerRequestHandler(new NamingPushRequestHandler(serviceInfoHolder));
    rpcClient.registerServerRequestHandler(
        new NamingFuzzyWatchNotifyRequestHandler(namingFuzzyWatchServiceListHolder));

    // Step 4: 启动 gRPC 连接（与 Nacos 服务端建立双向流）
    rpcClient.start();

    // Step 5: 启动模糊监听管理器
    namingFuzzyWatchServiceListHolder.start();

    // Step 6: 注册 this 为 NotifyCenter 订阅者（监听服务端地址变更）
    NotifyCenter.registerSubscriber(this);
}
```

### 3.6 初始化顺序总图

```
NacosNamingService.init()
│
├─ Step 0: PreInitUtils.asyncPreLoadCostComponent()  ← 异步预加载
├─ Step 1: 配置处理 + 校验
├─ Step 2: 创建事件体系
│   ├─ notifierEventScope = UUID
│   ├─ InstancesChangeNotifier
│   └─ NotifyCenter 注册
├─ Step 3: 创建 ServiceInfoHolder
│   ├─ 计算 cacheDir
│   ├─ 可选：loadCacheAtStart（从磁盘加载历史缓存）
│   ├─ new FailoverReactor(this, scope)    ← 启动 5s 轮询线程
│   └─ new ServiceInfoDiskCacheRefresher() ← 启动 100ms 刷盘线程
├─ Step 4: 创建模糊监听管理器
└─ Step 5: 创建 NamingClientProxyDelegate
    ├─ Step 5.1: ServiceInfoUpdateService（接收 this）
    ├─ Step 5.2: NamingServerListManager.start()（SPI 加载地址）
    ├─ Step 5.3: serviceInfoHolder = 外部传入
    ├─ Step 5.4: SecurityProxy + initSecurityProxy（登录 + 定时刷新）
    ├─ Step 5.5: NamingHttpClientProxy（HTTP 兼容通道）
    └─ Step 5.6: NamingGrpcClientProxy（gRPC 主力通道）
        ├─ 缓存 namespace / UUID
        ├─ 设置 gRPC labels
        ├─ 注册反向引用到 FuzzyWatchServiceListHolder
        ├─ 创建 RpcClient（gRPC 底层客户端）
        ├─ 创建 NamingGrpcRedoService
        └─ start()
            ├─ serverListFactory 注入
            ├─ registerConnectionListener(redoService)
            ├─ registerServerRequestHandler × 2
            ├─ rpcClient.start()  ← gRPC 建连
            ├─ FuzzyWatchServiceListHolder.start()
            └─ NotifyCenter.registerSubscriber(this)
```

---

## 4. 数据流转全景

### 4.1 注册流程

```
NacosNamingService.registerInstance()
  → clientProxy.registerService()                          [NamingClientProxyDelegate L234]
    → getExecuteClientProxy(instance)                      [L476-482]
      ├─ ephemeral=true              → grpcClientProxy
      └─ ephemeral=false
          ├─ server支持gRPC持久化     → grpcClientProxy
          └─ server不支持             → httpClientProxy
    → NamingGrpcClientProxy.registerService()              [L318-330]
      ├─ ephemeral → registerServiceForEphemeral()         [L337-344]
      │   ├─ redoService.cacheInstanceForRedo()  ← 先缓存
      │   └─ doRegisterService()                 ← 再请求
      │       ├─ requestToServer(InstanceRequest)
      │       └─ redoService.instanceRegistered() ← 标记成功
      └─ persistent → doRegisterServiceForPersistent()    [L478-485]
          └─ requestToServer(PersistentInstanceRequest)     ← 无 redo
```

### 4.2 订阅流程 —— 最复杂的数据流

```
NacosNamingService.doSubscribe()
  → changeNotifier.registerListener(wrapper)               [NacosNamingService L606]
  → clientProxy.subscribe()                                [L608]
    → NamingClientProxyDelegate.subscribe()                [L387-404]
      ├─ Step 1: scheduleUpdateIfAbsent()                  [L392]
      │   启动定时拉取任务（仅当 asyncQuerySubscribeService=true）
      ├─ Step 2: 查本地缓存 serviceInfoMap.get(key)        [L396]
      ├─ Step 3: 缓存未命中或未订阅 → gRPC 订阅             [L398-400]
      │   → grpcClientProxy.subscribe()
      │       ├─ cacheSubscriberForRedo()  ← 先缓存 redo
      │       └─ doSubscribe()
      │           ├─ requestToServer(SubscribeServiceRequest)
      │           └─ subscriberRegistered()  ← 标记成功
      └─ Step 4: processServiceInfo(result)               [L402]
          ├─ serviceInfoMap.put(key, result)  ← 更新内存
          ├─ InstancesDiffer.doDiff(old, new) ← 计算差异
          ├─ NotifyCenter.publishEvent(InstancesChangeEvent) ← 通知用户
          └─ publishDiskCacheRefreshEvent()  ← 异步刷盘
```

### 4.3 查询流程 —— failover 优先

```
NacosNamingService.selectInstances()
  → getServiceInfo(serviceName, groupName, clusters, subscribe)  [L434-451]
    ├─ failover 开启？
    │   └─ getServiceInfoByFailover()
    │       └─ serviceInfoHolder.getFailoverServiceInfo()  ← 读磁盘数据
    │           └─ doSelectInstance(clusterSelector)  ← 按集群过滤
    └─ failover 关闭：
        └─ getServiceInfoBySubscribe()
            ├─ subscribe=true → tryToSubscribe()
            │   ├─ serviceInfoHolder.getServiceInfo()  ← 查内存缓存
            │   ├─ 缓存有且已订阅 → 返回缓存
            │   ├─ 缓存有但未订阅 → clientProxy.subscribe()
            │   └─ 缓存无 → clientProxy.subscribe()
            └─ subscribe=false → clientProxy.queryInstancesOfService()  ← 直接查服务端
```

### 4.4 关键设计决策：为什么 subscribe 内部先查缓存再决定是否发网络请求？

> 源码：[NamingClientProxyDelegate](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/NamingClientProxyDelegate.java) L387-404

```java
// Step 2: 先查本地缓存，避免不必要的网络请求
ServiceInfo result = serviceInfoHolder.getServiceInfoMap().get(serviceKey);

// Step 3: 缓存未命中 或 尚未订阅 → 向服务端发起 gRPC 订阅
if (null == result || !isSubscribed(serviceName, groupName, clusters)) {
    result = grpcClientProxy.subscribe(serviceName, groupName, clusters);
}
// Step 4: 无论数据来自缓存还是服务端，都经过 processServiceInfo 统一处理
serviceInfoHolder.processServiceInfo(result);
```

**决策理由**：

1. **减少网络开销**：如果本地缓存已有数据（比如启动时从磁盘加载），且已订阅，就没有必要再请求服务端。gRPC 推送会保证后续的实时更新。

2. **`tryToSubscribe()` 的兜底逻辑**：即使缓存未命中导致订阅失败（网络异常），也会返回已有缓存数据：
   ```java
   // [NacosNamingService L475-495]
   if (null == cachedServiceInfo) {
       return clientProxy.subscribe(...);  // 首次订阅，必须成功
   }
   if (clientProxy.isSubscribed(...)) {
       return cachedServiceInfo;           // 已订阅，用缓存
   }
   // 有缓存但未订阅（如启动加载磁盘缓存），尝试订阅
   try {
       result = clientProxy.subscribe(...);
   } catch (NacosException e) {
       // 订阅失败不抛异常，降级使用本地缓存
   }
   return result;
   ```

   这就是 **"fail-fast vs fail-safe"** 权衡：首次订阅必须成功；后续调用可以降级使用缓存。

### 4.5 数据三源汇聚到 processServiceInfo

> 源码：[ServiceInfoHolder.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L279-372

三条数据来源最终都汇聚到同一个方法：

```
数据来源 1: gRPC 服务端推送
  NamingPushRequestHandler.requestReply()
    → serviceInfoHolder.processServiceInfo(ServiceInfo)

数据来源 2: 定时拉取兜底
  ServiceInfoUpdateService.UpdateTask.run()
    → namingClientProxy.queryInstancesOfService()
    → serviceInfoHolder.processServiceInfo(ServiceInfo)

数据来源 3: 主动订阅
  NamingClientProxyDelegate.subscribe()
    → grpcClientProxy.subscribe()
    → serviceInfoHolder.processServiceInfo(ServiceInfo)
```

**统一入口的好处**：
- 所有缓存更新具有相同的一致性保证
- 差异计算（InstancesDiffer）和事件发布逻辑只写一次
- 可以在这个单一入口处插入空推送保护、failover 开关检查等横切逻辑

---

## 5. 关键设计决策

### 5.1 "缓存在前，请求在后"模式的普遍性

在 Nacos 客户端中，不仅是 redo，多个关键操作都采用此模式：

| 操作 | 先缓存 | 后请求 | 源码位置 |
|------|--------|--------|----------|
| 临时实例注册 | `cacheInstanceForRedo()` | `doRegisterService()` | [NamingGrpcClientProxy L337-344](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) |
| 批量注册 | `cacheInstanceForRedo(instances)` | `doBatchRegisterService()` | [L349-354](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) |
| 服务订阅 | `cacheSubscriberForRedo()` | `doSubscribe()` | [L631-634](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) |
| 取消订阅 | `subscriberDeregister()` | `doUnsubscribe()` | [L665-669](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) |

**设计意图**：确保即使请求发送"瞬间"后连接断开，redo 缓存中也已有记录，断连时 `onDisConnect()` 将 `registered` 设为 `false`，重连后由 `RedoScheduledTask` 重做。

### 5.2 为什么需要定时拉取（ServiceInfoUpdateService）作为 gRPC 推送的兜底？

> 源码：[ServiceInfoUpdateService.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/core/ServiceInfoUpdateService.java) L96-109

```java
private boolean isAsyncQueryForSubscribeService(NacosClientProperties properties) {
    // 默认 false —— Nacos 2.x+ 主要靠 gRPC 服务端主动推送
    if (...) {
        return false;
    }
    return ConvertUtils.toBoolean(
        properties.getProperty(PropertyKeyConst.NAMING_ASYNC_QUERY_SUBSCRIBE_SERVICE), false);
}
```

**决策理由**：

1. **推送可能丢失**：gRPC 网络抖动时，`NotifySubscriberRequest` 可能因网络分区而丢失，服务端认为推送成功（因为 gRPC buffer 已写出），但客户端未收到。

2. **兜底机制的必要性**：`UpdateTask` 通过比较 `lastRefTime` 来判断：
   ```java
   // [ServiceInfoUpdateService L283-288]
   if (serviceObj.getLastRefTime() <= lastRefTime) {
       // 服务端 lastRefTime 没变 → 可能推送丢失 → 主动查询
       serviceObj = namingClientProxy.queryInstancesOfService(...);
       serviceInfoHolder.processServiceInfo(serviceObj);
   }
   ```
   如果 `lastRefTime` 长时间未增长，说明推送可能丢失，触发主动拉取。

3. **默认关闭**：因为 Nacos 2.x+ 的 gRPC 双向流已经足够可靠，定时拉取只在特定场景（网络不稳定、长连接频繁断开）下才需要开启。

### 5.3 指数退避策略

> 源码：[ServiceInfoUpdateService.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/core/ServiceInfoUpdateService.java) L340-346

```java
private void incFailCount() {
    int limit = 6;          // 失败计数上限 6 次
    if (failCount == limit) {
        return;
    }
    failCount++;
}
```

配合重调度逻辑：

```java
// [L309]: 指数退避：delayTime << failCount
executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60), ...);
```

| failCount | 延迟（delayTime=1000ms） |
|:---:|:---|
| 0 | 1s（正常） |
| 1 | 2s |
| 2 | 4s |
| 3 | 8s |
| 4 | 16s |
| 5 | 32s |
| 6+ | 60s（上限） |

**设计意图**：连接不稳定时避免频繁请求加重服务端压力；上限 60 秒确保最终一定会重试。

### 5.4 初始化顺序的依赖传递链

`NamingClientProxyDelegate` 构造器中的 6 步顺序不是随意的，而是由依赖关系决定的：

```
Step 2 (serverListManager) 必须先于 Step 4 (securityProxy)
  ∵ SecurityProxy 构造器接收 serverListManager，调用 getServerList() 初始化插件

Step 4 (securityProxy) 必须先于 Step 5/6 (httpClientProxy / grpcClientProxy)
  ∵ 两个代理的父类 AbstractNamingClientProxy 接收 securityProxy，每次请求注入 token

Step 1 (serviceInfoUpdateService) 可以放在任何位置
  ∵ 它只依赖 NamingClientProxy 接口（this），不依赖任何已初始化的组件

Step 3 (serviceInfoHolder) 只是赋值，无依赖
```

---

## 6. 关闭全流程

> 源码：[NamingClientProxyDelegate.shutdown()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/NamingClientProxyDelegate.java) L498-510
> + [NacosNamingService.shutDown()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/NacosNamingService.java) L769-774
> + [ServiceInfoHolder.shutdown()](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L465-471

```
NacosNamingService.shutDown()
│
├─ serviceInfoHolder.shutdown()
│   ├─ failoverReactor.shutdown()            ← 关闭 5s 轮询线程
│   └─ serviceInfoDiskCacheRefresher.shutdown()
│       └─ 执行最后一次 flush（pendingEvents 中剩余数据写入磁盘）
│
├─ clientProxy.shutdown()                    ← NamingClientProxyDelegate
│   ├─ serviceInfoUpdateService.shutdown()   ← 停止定时拉取线程池
│   ├─ serverListManager.shutdown()          ← 停止地址管理
│   ├─ httpClientProxy.shutdown()            ← 关闭 HTTP 代理
│   ├─ grpcClientProxy.shutdown()            ← 关闭 gRPC 连接（最关键）
│   │   ├─ redoService.shutdown()
│   │   ├─ RpcClientFactory.destroyClient()
│   │   └─ NotifyCenter.deregisterSubscriber(this)
│   ├─ securityProxy.shutdown()              ← 关闭安全代理插件
│   └─ executorService.shutdown()            ← 关闭 token 刷新线程池
│
├─ namingFuzzyWatchServiceListHolder.shutdown()
└─ NotifyCenter.deregisterSubscriber(changeNotifier)
```

### 关键设计决策：关闭顺序为什么是逆初始化的顺序？

**原因**：后初始化的组件可能依赖先初始化的组件。

例如：gRPC 连接（Step 6）可能正在执行 RedoScheduledTask 的重做操作，重做操作会调用 `NamingGrpcClientProxy.doRegisterService()` → `requestToServer()` → 需要 `rpcClient`。如果先关闭 `rpcClient`，重做线程会出现错误。

因此关闭顺序必须**逆序**：先关闭最上层（定时任务、gRPC 连接），再关闭基础设施（安全代理、线程池）。

---

## 7. 关键文件索引

| 文件 | 相对路径 | 角色 |
|------|---------|------|
| NacosNamingService | `client/src/main/java/.../naming/NacosNamingService.java` | 用户 API 入口，init() + shutDown() |
| NamingClientProxyDelegate | `client/src/main/java/.../naming/remote/NamingClientProxyDelegate.java` | 委托/路由调度，6 步初始化核心 |
| NamingGrpcClientProxy | `client/src/main/java/.../remote/gprc/NamingGrpcClientProxy.java` | gRPC 通信代理，start() 6 步 |
| ServiceInfoHolder | `client/src/main/java/.../naming/cache/ServiceInfoHolder.java` | 中央缓存，processServiceInfo() 统一入口 |
| ServiceInfoUpdateService | `client/src/main/java/.../naming/core/ServiceInfoUpdateService.java` | 定时拉取兜底 + 指数退避 |
| NamingServerListManager | `client/src/main/java/.../naming/core/NamingServerListManager.java` | 服务端地址管理（SPI） |
| SecurityProxy | `client/src/main/java/.../security/SecurityProxy.java` | 鉴权门面（SPI） |
| NamingGrpcRedoService | `client/src/main/java/.../gprc/redo/NamingGrpcRedoService.java` | 重做服务（断连恢复） |
| FailoverReactor | `client/src/main/java/.../naming/backups/FailoverReactor.java` | 故障转移反应器 |
| ServiceInfoDiskCacheRefresher | `client/src/main/java/.../naming/cache/ServiceInfoDiskCacheRefresher.java` | 异步磁盘刷新（100ms） |
