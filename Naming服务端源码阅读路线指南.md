# Naming 服务端源码阅读路线指南

> **原则声明**：本文档所有路径和引用均来源于 Nacos 3.2.1-SNAPSHOT 源代码的实际结构，不含任何猜测性描述。

---

## 目录

1. [为什么从客户端过渡到服务端](#1-为什么从客户端过渡到服务端)
2. [服务端架构总览](#2-服务端架构总览)
3. [第一层：入口锚点 — gRPC Handler（建议优先读）](#3-第一层入口锚点--grpc-handler建议优先读)
4. [第二层：核心逻辑 — v2 Client 模型](#4-第二层核心逻辑--v2-client-模型)
5. [第三层：Distro 一致性协议（AP 模式核心）](#5-第三层distro-一致性协议ap-模式核心)
6. [第四层：推送引擎](#6-第四层推送引擎)
7. [第五层：健康检查](#7-第五层健康检查)
8. [第六层：HTTP Controller（V3 Admin API）](#8-第六层http-controllerv3-admin-api)
9. [建议的阅读顺序（按天）](#9-建议的阅读顺序按天)
10. [从客户端到服务端的完整调用链示例](#10-从客户端到服务端的完整调用链示例)
11. [关键目录索引](#11-关键目录索引)

---

## 1. 为什么从客户端过渡到服务端

在已经吃透 6 篇客户端文档的前提下，读服务端有一条黄金捷径：

> **服务端代码的每一个核心类，都是客户端某个行为的"镜像"**。

| 客户端操作 | 服务端对应 Handler | 服务端核心处理类 |
|-----------|-------------------|----------------|
| `NamingGrpcClientProxy.doRegisterService()` | `InstanceRequestHandler` | `EphemeralClientOperationServiceImpl` |
| `NamingGrpcClientProxy.doSubscribe()` | `SubscribeServiceRequestHandler` | `EphemeralClientOperationServiceImpl` |
| `NamingGrpcClientProxy.doUnsubscribe()` | `SubscribeServiceRequestHandler`（同一 Handler） | `EphemeralClientOperationServiceImpl` |
| `NamingGrpcClientProxy.queryInstancesOfService()` | `ServiceQueryRequestHandler` | `ServiceStorage` |
| 心跳 `HealthCheckRequest` | 内嵌在 `RpcClient` 层 | `ClientBeatProcessorV2` |

**因此**：从 gRPC Handler 层开始读，每读一个 Handler 都能直接对接你已有的客户端知识，不会产生"这个请求从哪来的"困惑。

---

## 2. 服务端架构总览

Naming 服务端的包结构分为两层设计：

```
naming/src/main/java/com/alibaba/nacos/naming/
│
├── remote/rpc/handler/        ← 第 1 层：gRPC 入口（客户端请求的第一站）
│   ├── InstanceRequestHandler.java          注册/注销实例
│   ├── SubscribeServiceRequestHandler.java  订阅/取消订阅
│   ├── ServiceQueryRequestHandler.java      查询实例列表
│   ├── ServiceListRequestHandler.java       查询服务列表
│   ├── BatchInstanceRequestHandler.java     批量注册
│   ├── PersistentInstanceRequestHandler.java 持久实例注册
│   ├── NamingFuzzyWatchRequestHandler.java  模糊监听
│   └── DistroDataRequestHandler.java        集群间 Distro 同步
│
├── core/v2/                   ← 第 2 层：核心模型（以 Client 为中心）
│   ├── service/impl/
│   │   ├── EphemeralClientOperationServiceImpl.java   ★ 第一核心类（170行）
│   │   └── PersistentClientOperationServiceImpl.java
│   ├── client/
│   │   ├── Client.java                    Client 抽象
│   │   ├── IpPortBasedClient.java         HTTP 客户端模型
│   │   ├── ConnectionBasedClient.java     gRPC 客户端模型
│   │   └── manager/                       ClientManager 接口+实现
│   ├── index/
│   │   └── ServiceStorage.java            ★ 从 Client 汇总实例 → ServiceInfo
│   ├── metadata/
│   │   └── NamingMetadataManager.java     服务/实例元数据
│   └── ServiceManager.java               ★ 全局 Service 单例注册表
│
├── consistency/               ← 第 3 层：一致性协议
│   ├── ephemeral/distro/v2/                Distro AP 协议
│   └── persistent/                         持久实例 CP 协议（JRaft）
│
├── push/                      ← 第 4 层：推送引擎
│   ├── NamingSubscriberService.java        订阅者管理（gRPC 双向流推送）
│   └── v2/                                 推送任务执行器
│
├── healthcheck/               ← 第 5 层：健康检查
│   ├── heartbeat/
│   │   └── ClientBeatProcessorV2.java      客户端心跳处理
│   └── HealthCheckReactor.java
│
├── core/                      ← 兼容层：V1 API + HTTP 的桥接适配
│   ├── InstanceOperator.java              接口定义
│   ├── InstanceOperatorClientImpl.java      ★ 桥接到 v2 的实现
│   ├── ServiceOperator.java               服务操作接口
│   └── ServiceOperatorV2Impl.java          桥接到 v2 的实现
│
└── controllers/v3/            ← 第 6 层：HTTP API（控制台/OpenAPI）
    ├── ServiceControllerV3.java
    ├── InstanceControllerV3.java
    └── ...
```

---

## 3. 第一层：入口锚点 — gRPC Handler（建议优先读）

> 目录：`naming/src/main/java/com/alibaba/nacos/naming/remote/rpc/handler/`

这 8 个 Handler 是客户端 gRPC 请求到达服务端的第一站。每个 Handler 都很短（50-120 行），职责单一：**解析请求 → 调用 Service → 返回响应**。

### 3.1 InstanceRequestHandler — 注册/注销实例

> 源码：[InstanceRequestHandler.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/remote/rpc/handler/InstanceRequestHandler.java) (106 行)

```java
public InstanceResponse handle(InstanceRequest request, RequestMeta meta) {
    Service service = Service.newService(namespace, group, serviceName, true);
    switch (request.getType()) {
        case REGISTER_INSTANCE:
            return registerInstance(...);   // → EphemeralClientOperationServiceImpl
        case DE_REGISTER_INSTANCE:
            return deregisterInstance(...); // → EphemeralClientOperationServiceImpl
    }
}
```

**关键点**：`request.getType()` 区分注册/注销，同一个 Handler 处理两种操作。

### 3.2 SubscribeServiceRequestHandler — 订阅/取消订阅

> 源码：[SubscribeServiceRequestHandler.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/remote/rpc/handler/SubscribeServiceRequestHandler.java) (119 行)

```java
public SubscribeServiceResponse handle(SubscribeServiceRequest request, RequestMeta meta) {
    // 1. 先从 ServiceStorage 查询当前实例列表，作为首次返回
    ServiceInfo serviceInfo = ServiceUtil.selectInstancesWithHealthyProtection(
        serviceStorage.getData(service), ...);
    // 2. 根据 isSubscribe 标志执行订阅或取消订阅
    if (request.isSubscribe()) {
        clientOperationService.subscribeService(service, subscriber, connectionId);
    } else {
        clientOperationService.unsubscribeService(service, subscriber, connectionId);
    }
    // 3. 返回当前实例列表
    return new SubscribeServiceResponse(SUCCESS, "success", serviceInfo);
}
```

**关键设计**：订阅请求**先返回当前数据，再注册订阅关系**。这确保客户端在等待推送期间不会出现"空白期"。

### 3.3 其他 Handler 速览

| Handler | 客户端对应操作 | 行数 | 特点 |
|---------|-------------|:---:|------|
| `ServiceQueryRequestHandler` | `queryInstancesOfService` | ~70 | 纯查询，无副作用 |
| `ServiceListRequestHandler` | `getServicesOfServer` | ~60 | 返回某 namespace 下的所有服务名 |
| `BatchInstanceRequestHandler` | `batchRegisterInstance` | ~50 | 批量注册 |
| `PersistentInstanceRequestHandler` | 持久实例注册 | ~50 | 走 CP 协议 |
| `NamingFuzzyWatchRequestHandler` | 模糊监听 | ~50 | 实验功能 |
| `DistroDataRequestHandler` | 集群间同步 | ~40 | 接收其他 Nacos 节点的 Distro 数据 |

### 3.4 Handler 的共同特征

每个 Handler 都遵循相同的模式：

```java
@Component
public class XxxHandler extends RequestHandler<XxxRequest, XxxResponse> {

    @NamespaceValidation                          // namespace 合法性校验
    @TpsControl(pointName = "...")               // 流量控制
    @Secured(action = ActionTypes.READ/WRITE)    // 权限校验
    @ExtractorManager.Extractor(...)             // 参数提取
    public XxxResponse handle(XxxRequest request, RequestMeta meta) {
        // 1. 构建 Service 对象
        // 2. 调用 clientOperationService / serviceStorage
        // 3. 发布 Trace 事件
        // 4. 返回 Response
    }
}
```

这些注解（`@TpsControl`、`@Secured`、`@NamespaceValidation`）是横切关注点，通过 AOP 织入，不在 Handler 方法体内执行。阅读时可以暂时忽略它们的实现细节。

---

## 4. 第二层：核心逻辑 — v2 Client 模型

> 目录：`naming/src/main/java/com/alibaba/nacos/naming/core/v2/`

### 4.1 核心设计思想：以 Client 为中心

在 Nacos 2.x 之前，服务端以"服务"维度存储实例（Service → List\<Instance\>）。这导致无法区分实例是哪个客户端注册的，也无法在客户端断开时自动清理。

**2.x 的改进**：每个连接（gRPC connectionId 或 HTTP IP+Port）对应一个 `Client` 对象。实例挂在 Client 上，而不是 Service 上。

```
旧模型 (1.x):                  新模型 (2.x):
ServiceA                        ClientA (connectionId=123)
  ├── Instance1 (IP1)             ├── Instance1 (ServiceA)
  ├── Instance2 (IP2)             └── Instance2 (ServiceB)
  └── Instance3 (IP3)
                                ClientB (connectionId=456)
                                  └── Instance3 (ServiceA)

问题时：断开连接后              优势：ClientA 断开后
不知道哪些实例该摘除            直接清理 ClientA 上的所有实例
```

### 4.2 EphemeralClientOperationServiceImpl — 第一核心类

> 源码：[EphemeralClientOperationServiceImpl.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/impl/EphemeralClientOperationServiceImpl.java) (170 行)

**为什么只有 170 行？** 因为它只做编排，不存储数据：

```java
// 注册实例的核心流程
public void registerInstance(Service service, Instance instance, String clientId) {
    Service singleton = ServiceManager.getInstance().getSingleton(service);
    Client client = clientManager.getClient(clientId);
    // 1. 把实例挂在 Client 上
    client.addServiceInstance(singleton, instanceInfo);
    // 2. 更新时间戳
    client.setLastUpdatedTime();
    // 3. 重新计算 revision
    client.recalculateRevision();
    // 4. 发布事件（触发 Distro 同步 + 推送）
    NotifyCenter.publishEvent(
        new ClientRegisterServiceEvent(singleton, clientId));
}
```

**数据不在这里**，它通过 `NotifyCenter.publishEvent()` 发布事件，由其他组件异步响应：
- `DistroClientDataProcessor` 收到事件 → 同步到其他节点
- `NamingSubscriberService` 收到事件 → 推送给订阅客户端

### 4.3 Client 体系

| 类 | 标识方式 | 适用场景 |
|----|---------|---------|
| `Client.java` | 抽象基类 | 定义 `addServiceInstance`、`addServiceSubscriber` 等接口 |
| `ConnectionBasedClient.java` | gRPC `connectionId` | gRPC 客户端（Nacos 2.x+ SDK） |
| `IpPortBasedClient.java` | `IP#PORT#ephemeral/persist` | HTTP 客户端（Nacos 1.x SDK、OpenAPI） |

**为什么分两种？**

- `ConnectionBasedClient`：客户端与连接一一对应，断开时立即清理所有实例和订阅。这是首选模型。
- `IpPortBasedClient`：HTTP 请求没有长连接，无法感知客户端存活。用于兼容旧版 SDK 和 OpenAPI 调用。

### 4.4 ServiceStorage — 实例汇总引擎

> 源码：`naming/src/main/java/.../naming/core/v2/index/ServiceStorage.java`

**核心问题**：实例挂在 Client 上，但查询时要按 Service 维度返回。怎么办？

`ServiceStorage` 监听 `ClientRegisterServiceEvent` / `ClientDeregisterServiceEvent`，实时维护一个 `Map<Service, ServiceInfo>`。当收到注册事件时，从 `ClientManager` 获取所有注册了该服务的 Client，汇总它们的实例 → 生成 `ServiceInfo`。

```
ServiceStorage.getData(service):
  → 找到所有注册了 service 的 Client
    → 逐个收集 addServiceInstance 的 instanceInfo
      → 合并 → ServiceInfo
```

### 4.5 ServiceManager — 全局 Service 单例注册表

> 源码：`naming/src/main/java/.../naming/core/v2/ServiceManager.java`

每个 `Service(namespace, group, name)` 对象在服务端全局只存在一个实例（单例模式）。`ServiceManager.getSingleton(service)` 保证同一服务名的多次请求拿到同一个对象引用。

**为什么需要单例？** 因为 `Client.addServiceInstance(service, info)` 中，`service` 对象是 Map 的 key。如果同一个服务有多个不同的 `Service` 对象实例，会导致 `HashMap` 无法正确匹配 key，实例列表错乱。

### 4.6 ClientManager — 连接生命周期管理

> 源码：`naming/src/main/java/.../naming/core/v2/client/manager/ClientManager.java`

```
ClientManager 接口:
  ├── clientConnected(clientId, attributes)   ← RpcClient 建连时调用
  ├── clientDisconnected(clientId)             ← RpcClient 断连时调用
  ├── getClient(clientId)                      ← 查询 Client 对象
  └── contains(clientId)                       ← 判断是否存在
```

- 建连时：创建 `Client` 对象，放入 `clients` Map
- 断连时：从 `clients` 移除 → 发布 `ClientDisconnectEvent` → Distro 广播 → 其他节点摘除该 Client 的实例

---

## 5. 第三层：Distro 一致性协议（AP 模式核心）

> 目录：`naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/`

### 5.1 为什么需要 Distro？

Nacos 集群中的多个节点各自独立接收客户端注册。需要一种机制把注册数据同步到其他节点：

```
场景：3 节点集群（A、B、C）
  客户端连接节点 A → 注册 service-X
  → 节点 A 通过 Distro 同步到 B 和 C
  → 其他客户端连接节点 B → 查询 service-X → 可以查到
```

### 5.2 核心组件

| 文件 | 作用 |
|------|------|
| `DistroClientDataProcessor.java` | 监听 `ClientEvent`，触发 Distro 同步任务 |
| `DistroClientVerifyInfo.java` | 数据校验信息 |
| `DistroClientTaskFailedHandler.java` | 同步失败重试 |

### 5.3 触发链路

```
EphemeralClientOperationServiceImpl.registerInstance()
  → NotifyCenter.publishEvent(ClientRegisterServiceEvent)
    → DistroClientDataProcessor.onEvent()
      → 创建 Distro 同步任务
      → 选择目标节点（由 DistroMapper 计算，按 serviceName 哈希）
      → 发送 Client 数据到目标节点
```

### 5.4 持久实例的 CP 一致性

> 目录：`naming/src/main/java/.../naming/consistency/persistent/impl/`

持久实例走 JRaft（CP 协议），数据持久化到磁盘。在集群中通过 Raft 日志复制保证强一致性。与临时实例的 Distro 协议不同，持久实例的注册需要多数节点确认才能返回成功。

---

## 6. 第四层：推送引擎

> 目录：`naming/src/main/java/com/alibaba/nacos/naming/push/`

### 6.1 NamingSubscriberService

当实例变更后，服务端需要告知所有订阅了该服务的客户端。

```
实例变更事件 → NamingSubscriberService
  → 查询订阅了该服务的所有 clientId
    → 逐个通过 gRPC 双向流发送 NotifySubscriberRequest
      → 客户端 NamingPushRequestHandler 收到
        → serviceInfoHolder.processServiceInfo()
```

### 6.2 推送与 Distro 的关系

```
客户端注册 → ClientRegisterServiceEvent
  ├─ DistroClientDataProcessor → 同步到其他节点
  └─ NamingSubscriberService  → 推送到订阅客户端
```

两者并行执行，互不影响。即使 Distro 同步尚未完成，同节点上的订阅者也能立即收到推送。

---

## 7. 第五层：健康检查

> 目录：`naming/src/main/java/com/alibaba/nacos/naming/healthcheck/`

### 7.1 客户端心跳处理

> 源码：`naming/src/main/java/.../naming/healthcheck/heartbeat/ClientBeatProcessorV2.java`

gRPC 客户端的心跳由 `RpcClient` 层自动处理（gRPC 长连接本身有心跳）。到达 `ClientBeatProcessorV2` 的心跳属于 v1 HTTP 客户端兼容逻辑——v1 HTTP 客户端需要显式发送 `HTTP PUT /v1/ns/instance/beat`。

```java
// InstanceOperatorClientImpl.handleBeat()
ClientBeatProcessorV2 beatProcessor = new ClientBeatProcessorV2(namespaceId, clientBeat, client);
HealthCheckReactor.scheduleNow(beatProcessor);
client.setLastUpdatedTime();  // 更新时间戳，防止被判定为过期
```

### 7.2 客户端过期清理

`ConnectionBasedClient` 在 `RpcClient` 连接断开时主动调用 `clientDisconnected()` → 发布 `ClientDisconnectEvent` → Distro 广播摘除实例。

`IpPortBasedClient` 没有长连接，通过定时任务检查 `lastUpdatedTime` 是否超过超时阈值来判定过期。

---

## 8. 第六层：HTTP Controller（V3 Admin API）

> 目录：`naming/src/main/java/com/alibaba/nacos/naming/controllers/v3/`

这一层是控制台和 OpenAPI 的入口，面向运维操作而非客户端 SDK。每个 Controller 方法都带有 `@Secured` 注解进行权限校验。

| Controller | HTTP 路径 | 对应功能 |
|-----------|----------|---------|
| `ServiceControllerV3` | `POST /v3/admin/ns/service` | 创建/查询服务 |
| `InstanceControllerV3` | `POST /v3/admin/ns/instance` | 管理实例（上下线） |
| `ClusterControllerV3` | `/v3/admin/ns/cluster` | 集群管理 |
| `HealthControllerV3` | `/v3/admin/ns/health` | 健康检查查询 |
| `ClientControllerV3` | `/v3/admin/ns/client` | 客户端连接查询 |

**为什么最后读？** 因为 HTTP Controller 依赖所有前面层的组件（`InstanceOperator`、`ServiceOperator`、`ClientManager` 等）。只有理解了核心模型后再读 Controller，才不会陷入"这个方法到底在调什么"的困惑。

---

## 9. 建议的阅读顺序（按天）

| 天数 | 内容 | 预计行数 | 阅读重点 |
|:---:|------|:---:|------|
| **D1** | 8 个 gRPC Handler | ~800 行 | 建立"客户端请求→服务端处理"的映射，不追深层调用 |
| **D2** | `EphemeralClientOperationServiceImpl` + `Client` 体系 | ~500 行 | 理解"以 Client 为中心"的模型，实例如何挂在 Client 上 |
| **D3** | `ServiceStorage` + `ServiceManager` + `NamingMetadataManager` | ~600 行 | 理解数据汇总和元数据管理 |
| **D4** | Distro 一致性 + 推送引擎 | ~800 行 | 理解集群同步和推送通知 |
| **D5** | 健康检查 + HTTP Controller | ~600 行 | 理解心跳和运维 API |

### 建议方法

1. **以 IDE 的 "Go to Implementation" 为导航**：从 `InstanceRequestHandler.registerInstance()` 开始，一路 Ctrl+Alt+B 追下去
2. **先读完整个调用链再回头看细节**：不要在第一轮就纠结 `NamingMetadataManager` 的内部实现，先跑通主链路
3. **关注事件驱动**：注意 `NotifyCenter.publishEvent()` 的地方，理解事件触发了哪些后续行为
4. **对比客户端文档**：每读一个服务端类，回头看看客户端 DOC 1-6 中对应的部分，形成端到端的闭环理解

---

## 10. 从客户端到服务端的完整调用链示例

### 10.1 注册实例完整链路

```
客户端侧（DOC 1）:
  NacosNamingService.registerInstance()
    → NamingClientProxyDelegate.registerService()
      → NamingGrpcClientProxy.doRegisterService()
        → requestToServer(InstanceRequest)                    ← gRPC 请求发送

服务端侧:
  InstanceRequestHandler.handle(InstanceRequest)              [L63-78]
    → registerInstance(service, request, meta)                [L80-90]
      → EphemeralClientOperationServiceImpl.registerInstance() [L56-78]
        → ServiceManager.getSingleton(service)                ← 获取 Service 单例
        → ClientManager.getClient(clientId)                   ← 获取 Client 对象
        → client.addServiceInstance(singleton, instanceInfo)  ← 实例挂在 Client 上
        → client.recalculateRevision()                        ← 重新计算 revision
        → NotifyCenter.publishEvent(ClientRegisterServiceEvent)
          ├─ DistroClientDataProcessor.onEvent()              ← 同步到其他节点
          └─ NamingSubscriberService.onEvent()                ← 推送到订阅客户端
            → gRPC NotifySubscriberRequest                    ← 服务端推送

客户端侧（DOC 1）:
  NamingPushRequestHandler.requestReply()                     ← 收到推送
    → serviceInfoHolder.processServiceInfo(ServiceInfo)       ← 更新缓存
      → NotifyCenter.publishEvent(InstancesChangeEvent)       ← 通知用户
```

### 10.2 订阅完整链路

```
客户端侧（DOC 1）:
  NacosNamingService.subscribe()
    → NamingClientProxyDelegate.subscribe()
      → NamingGrpcClientProxy.doSubscribe()
        → requestToServer(SubscribeServiceRequest)

服务端侧:
  SubscribeServiceRequestHandler.handle(SubscribeServiceRequest)  [L79-117]
    → serviceStorage.getData(service)                          ← 查询当前实例列表
    → clientOperationService.subscribeService(
        service, subscriber, connectionId)                     ← 注册订阅关系
      → ClientManager.getClient(clientId)
      → client.addServiceSubscriber(singleton, subscriber)    ← 订阅挂在 Client 上
      → NotifyCenter.publishEvent(ClientSubscribeServiceEvent)
    → return SubscribeServiceResponse(serviceInfo)             ← 返回当前数据
```

---

## 11. 关键目录索引

| 目录 | 绝对路径 | 角色 |
|------|---------|------|
| gRPC Handler | `naming/src/main/java/.../naming/remote/rpc/handler/` | 客户端请求入口 |
| v2 核心 | `naming/src/main/java/.../naming/core/v2/` | Client 模型 + Service 管理 |
| v2 事件 | `naming/src/main/java/.../naming/core/v2/event/` | ClientEvent / MetadataEvent |
| Distro | `naming/src/main/java/.../naming/consistency/ephemeral/distro/v2/` | AP 一致性协议 |
| 推送引擎 | `naming/src/main/java/.../naming/push/` | gRPC 双向流推送 |
| 健康检查 | `naming/src/main/java/.../naming/healthcheck/` | 心跳处理 |
| 兼容层 | `naming/src/main/java/.../naming/core/` | InstanceOperator + 桥接 |
| HTTP API | `naming/src/main/java/.../naming/controllers/v3/` | Admin API / OpenAPI |
| 工具类 | `naming/src/main/java/.../naming/misc/` | 常量、工具 |
