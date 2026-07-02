# Nacos gRPC/HTTP 双通道通信与路由决策模型

> **原则声明**：本文档所有结论均来源于 Nacos 3.2.1-SNAPSHOT 源代码的实际引用，不含任何猜测性描述。

---

## 目录

1. [为什么需要双通道](#1-为什么需要双通道)
2. [NamingClientProxyDelegate：路由决策中心](#2-namingclientproxydelegate路由决策中心)
3. [NamingGrpcClientProxy：gRPC 主力通道](#3-naminggrpcclientproxygrpc-主力通道)
4. [NamingHttpClientProxy：HTTP 兼容通道](#4-naminghttpclientproxyhttp-兼容通道)
5. [Config 模块的传输层选择](#5-config-模块的传输层选择)
6. [路由决策矩阵](#6-路由决策矩阵)
7. [关键设计决策](#7-关键设计决策)
8. [总结](#8-总结)

---

## 1. 为什么需要双通道

Nacos 同时支持 gRPC 和 HTTP 两种通信协议，核心原因：

1. **向后兼容**：Nacos 1.x 只用 HTTP，2.x 引入 gRPC。旧版服务端不支持 gRPC
2. **持久实例的特殊性**：持久实例在旧版服务端只能通过 HTTP 操作
3. **渐进迁移**：客户端可以同时连接 gRPC 和 HTTP，根据操作类型和服务端能力自动选择

**gRPC 是主力通道**，提供双向流、服务端推送、更低的延迟和更高的吞吐。**HTTP 是兼容通道**，用于旧版服务端和不支持 gRPC 的操作。

---

## 2. NamingClientProxyDelegate：路由决策中心

### 2.1 整体架构

> 源码：[NamingClientProxyDelegate.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/NamingClientProxyDelegate.java) L126-131

```java
public class NamingClientProxyDelegate implements NamingClientProxy {
    private final NamingGrpcClientProxy grpcClientProxy;   // gRPC 主力通道
    private final NamingHttpClientProxy httpClientProxy;   // HTTP 兼容通道
    private final ServiceInfoUpdateService serviceInfoUpdateService;
    private final NamingServerListManager serverListManager;
    private final ServiceInfoHolder serviceInfoHolder;
    private final SecurityProxy securityProxy;
}
```

### 2.2 路由决策核心：getExecuteClientProxy()

> 源码：[NamingClientProxyDelegate.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/NamingClientProxyDelegate.java) L476-482

```java
private NamingClientProxy getExecuteClientProxy(Instance instance) {
    if (instance.isEphemeral() || grpcClientProxy.isAbilitySupportedByServer(
        AbilityKey.SERVER_PERSISTENT_INSTANCE_BY_GRPC)) {
        return grpcClientProxy;
    }
    return httpClientProxy;
}
```

**决策逻辑**：

```
                        ┌─ instance.isEphemeral() == true
                        │     → gRPC（临时实例必须走 gRPC）
registerService() ──────┤
                        └─ instance.isEphemeral() == false
                              ├─ 服务端支持 gRPC 持久化 → gRPC
                              └─ 服务端不支持 → HTTP（兼容旧版）
```

**为什么临时实例必须走 gRPC？** 临时实例的健康状态通过 gRPC 连接的心跳维持。HTTP 是无状态协议，不具备长连接心跳能力。如果临时实例走 HTTP，服务端无法感知客户端是否存活。

### 2.3 各操作的路由策略

> 源码：[NamingClientProxyDelegate.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/NamingClientProxyDelegate.java)

| 操作 | 路由策略 | 源码行号 | 原因 |
|------|---------|----------|------|
| `registerService` | `getExecuteClientProxy()` | L234 | 临时→gRPC，持久→取决于能力 |
| `deregisterService` | `getExecuteClientProxy()` | L281 | 同上 |
| `batchRegisterService` | **始终 gRPC** | L251 | HTTP 不支持批量 |
| `batchDeregisterService` | **始终 gRPC** | L269 | HTTP 不支持批量 |
| `subscribe` | **始终 gRPC** | L399 | 依赖 gRPC 双向流推送 |
| `unsubscribe` | **始终 gRPC** | L423 | 同上 |
| `queryInstancesOfService` | **始终 gRPC** | L311 | gRPC 查询更快 |
| `getServiceList` | **始终 gRPC** | L361 | gRPC 查询更快 |
| `isSubscribed` | **始终 gRPC** | L437 | 查询 redo 缓存 |
| `updateInstance` | **空实现** | L291 | 不支持的操作 |
| `queryService` | **返回 null** | L320 | 不支持的操作 |
| `createService` | **空实现** | L327 | 不支持的操作 |
| `deleteService` | **返回 false** | L335 | 不支持的操作 |
| `updateService` | **空实现** | L343 | 不支持的操作 |

### 2.4 服务端能力协商机制

> gRPC 能力协商发生在 `RpcClient.connectToServer()` 时，通过 `ConnectionSetupRequest` 交换能力表。

```java
grpcClientProxy.isAbilitySupportedByServer(
    AbilityKey.SERVER_PERSISTENT_INSTANCE_BY_GRPC)
```

客户端建连时向服务端发送 `ConnectionSetupRequest`（含客户端能力列表），服务端响应时携带服务端能力列表。能力表存储在 `Connection.abilityTable` 中。只有新版服务端（2.x+）才会返回 `SERVER_PERSISTENT_INSTANCE_BY_GRPC=true`。

### 2.5 serverHealthy() 的双通道检查

> 源码：[NamingClientProxyDelegate.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/NamingClientProxyDelegate.java) L448-450

```java
public boolean serverHealthy() {
    return grpcClientProxy.serverHealthy() || httpClientProxy.serverHealthy();
}
```

**任一通道可用即认为健康**——这是典型的"宽进严出"策略，避免因单个通道故障导致整体不可用。

---

## 3. NamingGrpcClientProxy：gRPC 主力通道

### 3.1 8 步初始化

> 源码：[NamingGrpcClientProxy.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L236-268

```java
public NamingGrpcClientProxy(...) {
    super(securityProxy);
    this.namespaceId = namespaceId;
    this.uuid = UUID.randomUUID().toString();

    // Step 2: 设置 gRPC 标签（source=SDK, module=naming）
    Map<String, String> labels = new HashMap<>();
    labels.put(RemoteConstants.LABEL_SOURCE, RemoteConstants.LABEL_SOURCE_SDK);
    labels.put(RemoteConstants.LABEL_MODULE, RemoteConstants.LABEL_MODULE_NAMING);

    // Step 4: 创建 gRPC 客户端配置
    GrpcClientConfig grpcClientConfig = RpcClientConfigFactory.getInstance()
        .createGrpcClientConfig(properties.asProperties(), labels);

    // Step 5: 创建 RpcClient（ConnectionType.GRPC）
    this.rpcClient = RpcClientFactory.createClient(uuid, ConnectionType.GRPC, grpcClientConfig);

    // Step 6: 创建 Redo 服务
    this.redoService = new NamingGrpcRedoService(this, ...);

    // Step 8: 启动
    start(serverListFactory, serviceInfoHolder, namingFuzzyWatchServiceListHolder);
}
```

### 3.2 start() 的 6 步建立连接

> 源码：[NamingGrpcClientProxy.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/gprc/NamingGrpcClientProxy.java) L281-298

```java
private void start(...) {
    rpcClient.serverListFactory(serverListFactory);                   // 1. 地址来源
    rpcClient.registerConnectionListener(redoService);                // 2. 连接监听
    rpcClient.registerServerRequestHandler(new NamingPushRequestHandler(...));  // 3a. 推送处理器
    rpcClient.registerServerRequestHandler(new NamingFuzzyWatchNotifyRequestHandler(...)); // 3b.
    rpcClient.start();                                                // 4. 建立 gRPC 连接
    namingFuzzyWatchServiceListHolder.start();                        // 5. 模糊监听
    NotifyCenter.registerSubscriber(this);                            // 6. 监听地址变更
}
```

### 3.3 requestToServer()：通用的请求发送方法

每次请求都会注入鉴权信息：

```java
private <T extends Response> T requestToServer(Request request, Class<T> responseType)
    throws NacosException {
    // 注入鉴权 headers（accessToken + app）
    request.putAllHeader(getSecurityHeaders(namespaceId, groupName, serviceName));
    // 注入通用 headers（client version, etc.）
    request.putAllHeader(super.getCommonHeader());
    return (T) rpcClient.request(request, requestTimeout);
}
```

---

## 4. NamingHttpClientProxy：HTTP 兼容通道

### 4.1 设计定位

> 源码：[NamingHttpClientProxy.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/remote/http/NamingHttpClientProxy.java) L137

HTTP 代理仅用于**持久实例**在旧版服务端（不支持 gRPC 持久化）上的注册/注销操作。

### 4.2 callServer()：HTTP 请求发送

```java
// 每次 HTTP 请求前注入鉴权 headers
Map<String, String> headers = getSecurityHeaders(namespace, groupName, serviceName);
// 请求失败时重试下一个 server（最多 REQUEST_DOMAIN_RETRY_COUNT=3 次）
```

### 4.3 HTTP 重试机制

HTTP 代理内置了简单的重试逻辑：请求失败后自动切换到下一个服务端地址，最多重试 3 次。

**为什么 gRPC 不需要这个重试？** gRPC 的长连接本身有自动重连机制（RpcClient 内部的 ReconnectTask）。HTTP 是无状态协议，每次请求都需要重新建立连接，所以需要应用层的重试。

---

## 5. Config 模块的传输层选择

### 5.1 Config 只用 gRPC

> 源码：[ClientWorker.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/config/impl/ClientWorker.java) L674

Config 客户端的 `ConfigRpcTransportClient` 继承 `ConfigTransportClient`，只实现了 gRPC 通信：

```java
public class ConfigRpcTransportClient extends ConfigTransportClient {
    private ConnectionType getConnectionType() {
        return ConnectionType.GRPC;
    }
}
```

**但 Config 有多 RpcClient 实例**（按 taskId 分片），每个监听任务组对应一个独立的 gRPC 连接。

### 5.2 Config 与 Naming 传输层对比

| 维度 | Naming | Config |
|------|--------|--------|
| gRPC | ✅ 主力通道 | ✅ 唯一通道 |
| HTTP | ✅ 兼容通道（持久实例旧版） | ❌ 不支持 |
| RpcClient 数量 | 1 个 | N 个（按 taskId 分片） |
| 服务端推送 | ✅（ServiceInfo 推送） | ✅（dataId 变更通知） |
| 重连恢复 | RedoService | 标记 isConsistentWithServer=false |

---

## 6. 路由决策矩阵

```
NamingClientProxyDelegate 的路由决策：

registerService / deregisterService
  │
  ├─ instance.isEphemeral() == true
  │   └─ grpcClientProxy                     ← 临时实例必须走 gRPC（心跳依赖长连接）
  │
  └─ instance.isEphemeral() == false
      ├─ server 支持 gRPC 持久化
      │   └─ grpcClientProxy                 ← 新版服务端，走 gRPC
      └─ server 不支持
          └─ httpClientProxy                 ← 旧版服务端，走 HTTP

subscribe / unsubscribe / query / batch
  │
  └─ grpcClientProxy                         ← 始终走 gRPC（HTTP 不支持这些操作）

updateInstance / queryService / createService / deleteService / updateService
  │
  └─ 空实现 / 返回 null/false                 ← 不支持的操作
```

---

## 7. 关键设计决策

### 7.1 为什么只有 register/deregister 需要路由决策？

**因为只有这两个操作同时存在于 gRPC 和 HTTP 两个通道中。** 其他操作（订阅、查询、批量）只在 gRPC 中有实现，HTTP 通道不支持。

从源码可以看到，`NamingClientProxyDelegate` 中 `subscribe`、`queryInstancesOfService`、`getServiceList` 等方法都**直接调用 `grpcClientProxy`**，不经过 `getExecuteClientProxy()`。

### 7.2 为什么临时实例必须走 gRPC？

临时实例的生命周期管理依赖 gRPC 长连接的心跳。服务端通过 `ConnectionBasedClient` 管理临时实例，连接断开时自动摘除。HTTP 是无状态协议，无法实现这种"连接级状态管理"。

### 7.3 为什么 Config 不用 HTTP 通道？

Config 模块从 2.x 开始就全面转向 gRPC。配置的监听、查询、发布都通过 gRPC 完成。Config 的 HTTP API（Nacos 1.x 的长轮询方式）在新版客户端中已废弃。

### 7.4 为什么需要能力协商？

**新旧服务端并存**。当 Nacos 集群从 1.x 升级到 2.x 时，可能存在混部场景。客户端不知道连上的服务端是否支持 gRPC 持久化，通过能力协商动态判断。如果服务端不支持，自动降级到 HTTP。

---

## 8. 总结

### 8.1 通道选择决策树

```
操作类型
  │
  ├── 订阅/取消订阅/查询/批量 → 始终 gRPC
  │
  ├── 临时实例注册/注销 → 始终 gRPC
  │
  ├── 持久实例注册/注销
  │   ├── 服务端支持 gRPC 持久化 → gRPC
  │   └── 服务端不支持 → HTTP
  │
  └── update/create/delete service → 不支持（空实现）
```

### 8.2 关键文件索引

| 文件 | 相对路径 | 角色 |
|------|---------|------|
| NamingClientProxyDelegate | `client/src/main/java/.../naming/remote/NamingClientProxyDelegate.java` | 路由决策中心 |
| NamingGrpcClientProxy | `client/src/main/java/.../naming/remote/gprc/NamingGrpcClientProxy.java` | gRPC 主力通道 |
| NamingHttpClientProxy | `client/src/main/java/.../naming/remote/http/NamingHttpClientProxy.java` | HTTP 兼容通道 |
| AbstractNamingClientProxy | `client/src/main/java/.../naming/remote/AbstractNamingClientProxy.java` | 代理基类（鉴权注入） |
| ConfigRpcTransportClient | `client/src/main/java/.../config/impl/ClientWorker.java` (内部类) | Config gRPC 通道 |
| ConfigTransportClient | `client/src/main/java/.../config/impl/ConfigTransportClient.java` | Config 传输层基类 |
