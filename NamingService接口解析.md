# NamingService 接口解析

## 一、接口的作用与定位

`NamingService` 是 Nacos **客户端 SDK 的核心服务发现接口**，定义在 `api` 模块中（Java 8+ 兼容）。

| 维度 | 说明 |
|------|------|
| **所在层** | `api` 模块 — 纯接口定义，不含实现 |
| **实现类** | `NacosNamingService`（`client` 模块） |
| **创建方式** | 通过 `NamingFactory.createNamingService()` 或 `NacosFactory.createNamingService()` 工厂方法反射创建 |
| **核心职责** | 为应用提供**服务注册、服务注销、实例查询、服务订阅、模糊监听、服务列表查询**等能力 |

### 在 Nacos 架构中的角色

它是服务消费者和服务提供者与 Nacos 服务端交互的**统一客户端门面**。服务提供者通过它注册自己，服务消费者通过它发现服务实例并订阅变更。所有通信底层走 gRPC（v2+）或 HTTP（兼容）。

```
应用代码  →  NamingService（接口）  →  NacosNamingService（实现）  →  gRPC/HTTP  →  Nacos Server
```

### 架构分层

```
┌─────────────────────────────────────────────────────────┐
│                    应用层（用户代码）                      │
├─────────────────────────────────────────────────────────┤
│  NamingService（接口）  ← api 模块                        │
│  - 定义服务发现的所有操作契约                              │
│  - Java 8+ 兼容，纯接口无依赖                             │
├─────────────────────────────────────────────────────────┤
│  NacosNamingService（实现）  ← client 模块               │
│  - 内部委托 NamingClientProxy（gRPC/HTTP 代理）          │
│  - 维护本地缓存、订阅表、推送回调                          │
├─────────────────────────────────────────────────────────┤
│  gRPC / HTTP 通信层                                      │
├─────────────────────────────────────────────────────────┤
│  Nacos Server（naming 模块）                             │
│  - 服务注册表、健康检查、Distro/JRaft 一致性              │
└─────────────────────────────────────────────────────────┘
```

---

## 二、方法分组详解

该接口共定义了 **40+ 个方法**，按功能分为以下七大类。

---

### 1. 服务注册（6个重载 + 2个批量）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `registerInstance(serviceName, ip, port)` | 0.2.0 | 将实例注册到默认分组（DEFAULT_GROUP）下的服务 |
| `registerInstance(serviceName, groupName, ip, port)` | 0.2.0 | 指定分组注册 |
| `registerInstance(serviceName, ip, port, clusterName)` | 0.2.0 | 指定集群注册 |
| `registerInstance(serviceName, groupName, ip, port, clusterName)` | 1.0.0 | 指定分组+集群注册 |
| `registerInstance(serviceName, instance)` | 0.2.0 | 用完整 Instance 对象注册（默认分组），可携带元数据、权重等 |
| `registerInstance(serviceName, groupName, instance)` | 0.2.0 | 用完整 Instance 对象注册（指定分组） |
| `batchRegisterInstance(serviceName, groupName, instances)` | 2.1.1 | 批量注册实例，一次请求注册多个实例 |
| `batchDeregisterInstance(serviceName, groupName, instances)` | 2.2.0 | 批量注销实例 |

**作用**：服务提供者启动时调用这些方法，将自己的 IP:Port 注册到 Nacos Server，使服务可被发现。`Instance` 对象可携带权重、健康状态、元数据、集群名等丰富信息。

---

### 2. 服务注销（6个重载）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `deregisterInstance(serviceName, ip, port)` | 0.2.0 | 从默认分组注销 |
| `deregisterInstance(serviceName, groupName, ip, port)` | 0.2.0 | 指定分组注销 |
| `deregisterInstance(serviceName, ip, port, clusterName)` | 0.2.0 | 指定集群注销 |
| `deregisterInstance(serviceName, groupName, ip, port, clusterName)` | 1.0.0 | 指定分组+集群注销 |
| `deregisterInstance(serviceName, instance)` | 1.1.0 | 用完整 Instance 对象注销（默认分组） |
| `deregisterInstance(serviceName, groupName, instance)` | 0.2.0 | 用完整 Instance 对象注销（指定分组） |

**作用**：服务提供者下线时调用，从 Nacos Server 移除自己的注册信息。通常在应用优雅停机时调用。

---

### 3. 实例查询（8个重载）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `getAllInstances(serviceName)` | 0.2.0 | 获取服务的所有实例 |
| `getAllInstances(serviceName, groupName)` | 0.2.0 | 指定分组获取所有实例 |
| `getAllInstances(serviceName, subscribe)` | 0.2.0 | 控制是否走订阅模式获取 |
| `getAllInstances(serviceName, groupName, subscribe)` | 0.8.0 | 指定分组 + 是否订阅 |
| `getAllInstances(serviceName, clusters)` | 0.2.0 | 指定集群列表过滤 |
| `getAllInstances(serviceName, groupName, clusters)` | 0.8.0 | 指定分组 + 集群过滤 |
| `getAllInstances(serviceName, clusters, subscribe)` | 0.8.0 | 集群过滤 + 是否订阅 |
| `getAllInstances(serviceName, groupName, clusters, subscribe)` | 1.0.0 | 全参数版本 |

**作用**：服务消费者获取目标服务的实例列表。`subscribe=true` 时，客户端会自动订阅该服务并维护本地缓存，后续实例变更会通过推送通知；`subscribe=false` 时，直接向服务端发起一次查询请求。

---

### 4. 筛选实例（8个重载）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `selectInstances(serviceName, healthy)` | 0.2.0 | 按健康状态过滤实例 |
| `selectInstances(serviceName, groupName, healthy)` | 0.2.0 | 指定分组 + 健康状态 |
| `selectInstances(serviceName, healthy, subscribe)` | 0.2.0 | + 是否走订阅 |
| `selectInstances(serviceName, groupName, healthy, subscribe)` | 0.8.0 | 全参数 |
| `selectInstances(serviceName, clusters, healthy)` | 0.2.0 | 集群 + 健康过滤 |
| `selectInstances(serviceName, groupName, clusters, healthy)` | 0.8.0 | + 分组 |
| `selectInstances(serviceName, clusters, healthy, subscribe)` | 0.8.0 | + 订阅控制 |
| `selectInstances(serviceName, groupName, clusters, healthy, subscribe)` | 1.0.0 | 全参数版本 |

**作用**：在 `getAllInstances` 基础上增加**健康状态过滤**，只返回健康（`healthy=true`）或不健康（`healthy=false`）的实例。适合服务消费者做主动筛选。

---

### 5. 负载均衡选一个实例（8个重载）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `selectOneHealthyInstance(serviceName)` | 0.2.0 | 从健康实例中选一个 |
| `selectOneHealthyInstance(serviceName, groupName)` | 0.2.0 | 指定分组 |
| `selectOneHealthyInstance(serviceName, subscribe)` | 0.2.0 | + 订阅控制 |
| `selectOneHealthyInstance(serviceName, groupName, subscribe)` | 0.8.0 | 全参数 |
| `selectOneHealthyInstance(serviceName, clusters)` | 0.2.0 | 集群过滤 |
| `selectOneHealthyInstance(serviceName, groupName, clusters)` | 0.8.0 | + 分组 |
| `selectOneHealthyInstance(serviceName, clusters, subscribe)` | 0.8.0 | + 订阅控制 |
| `selectOneHealthyInstance(serviceName, groupName, clusters, subscribe)` | 1.0.0 | 全参数版本 |

**作用**：使用内置的负载均衡策略（默认基于权重的随机算法），从健康实例中**选出一个**实例返回。这是最常用的服务消费方法，相当于一次完整的服务发现 + 负载均衡。

---

### 6. 服务订阅与模糊监听（18个方法）

#### 6.1 精确订阅（6个）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `subscribe(serviceName, listener)` | 0.2.0 | 订阅服务变更，实例列表变化时回调 |
| `subscribe(serviceName, groupName, listener)` | 0.2.0 | 指定分组订阅 |
| `subscribe(serviceName, clusters, listener)` | 0.2.0 | 指定集群订阅 |
| `subscribe(serviceName, groupName, clusters, listener)` | 1.0.0 | 全参数订阅 |
| `subscribe(serviceName, selector, listener)` | 0.2.0 | 用 NamingSelector 自定义过滤条件订阅 |
| `subscribe(serviceName, groupName, selector, listener)` | 1.0.0 | + 分组 |

#### 6.2 取消精确订阅（6个）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `unsubscribe(serviceName, listener)` | 0.2.0 | 取消订阅 |
| `unsubscribe(serviceName, groupName, listener)` | 0.2.0 | 指定分组取消 |
| `unsubscribe(serviceName, clusters, listener)` | 0.2.0 | 集群取消 |
| `unsubscribe(serviceName, groupName, clusters, listener)` | 1.0.0 | 全参数取消 |
| `unsubscribe(serviceName, selector, listener)` | 0.2.0 | Selector 取消 |
| `unsubscribe(serviceName, groupName, selector, listener)` | 1.0.0 | + 分组取消 |

#### 6.3 模糊监听（6个，3.0.0+ 新增）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `fuzzyWatch(groupNamePattern, listener)` | 3.0.0 | 按组名模式模糊监听所有服务变更 |
| `fuzzyWatch(serviceNamePattern, groupNamePattern, listener)` | 3.0.0 | 按服务名+组名模式模糊监听 |
| `fuzzyWatchWithServiceKeys(groupNamePattern, listener)` | 3.0.0 | 模糊监听并返回匹配的服务键列表（Future） |
| `fuzzyWatchWithServiceKeys(serviceNamePattern, groupNamePattern, listener)` | 3.0.0 | 全参数版本 |
| `cancelFuzzyWatch(groupNamePattern, listener)` | 3.0.0 | 取消模糊监听 |
| `cancelFuzzyWatch(serviceNamePattern, groupNamePattern, listener)` | 3.0.0 | 全参数取消 |

**作用**：

- **精确订阅**：监听特定服务的实例列表变更，服务端通过 gRPC 推送变更事件。
- **模糊监听**（v3 新增）：监听符合模式匹配的多个服务的变更（如 `service_*` 匹配所有以 `service_` 开头的服务），适用于服务网格、网关等需要感知大规模服务注册/注销的场景。

---

### 7. 服务列表查询与生命周期（5个方法）

| 方法 | 引入版本 | 说明 |
|------|----------|------|
| `getServicesOfServer(pageNo, pageSize)` | 0.2.0 | 分页查询服务端所有服务名 |
| `getServicesOfServer(pageNo, pageSize, groupName)` | 0.7.0 | 按分组分页查询 |
| `getServicesOfServer(pageNo, pageSize, selector)` | 0.7.0 | 带 Selector 过滤（`@Deprecated` 3.3.0） |
| `getServicesOfServer(pageNo, pageSize, groupName, selector)` | 1.0.0 | 分组 + Selector（`@Deprecated`） |
| `getSubscribeServices()` | 0.2.0 | 获取当前客户端已订阅的所有服务列表 |
| `getServerStatus()` | 0.2.0 | 获取 Nacos Server 的健康状态（返回 "UP" 或 "DOWN"） |
| `shutDown()` | 1.3.1 | 关闭 NamingService，释放连接、线程等资源 |

**作用**：

- `getServicesOfServer`：用于管理面查询服务端注册了哪些服务
- `getSubscribeServices`：用于调试和监控当前订阅状态
- `getServerStatus`：健康检查，判断与服务端的连接是否正常
- `shutDown`：优雅关闭，释放底层 gRPC 连接、定时任务线程池等资源

---

## 三、核心价值总结

1. **统一门面**：屏蔽底层通信细节（gRPC vs HTTP），用户只需调接口方法
2. **服务注册与发现的核心契约**：涵盖了服务生命周期的完整闭环 — 注册 → 发现 → 订阅 → 变更推送 → 注销
3. **向后兼容**：通过 `@Since` 注解标注每个方法的引入版本，方法重载从简到繁，确保老版本客户端可以平滑升级
4. **SPI 友好**：接口与实现分离，通过 `NamingFactory` 反射加载实现类，允许第三方自定义实现

---

## 四、典型使用流程

```
创建 NamingService
    │
    ▼
registerInstance（注册服务实例）
    │
    ▼
subscribe（订阅服务变更通知）
    │
    ▼
getAllInstances / selectOneHealthyInstance（查询/选择实例）
    │
    ▼
deregisterInstance（注销服务实例）
    │
    ▼
shutDown（关闭资源）
```

### 代码示例

```java
// 1. 创建 NamingService
Properties properties = new Properties();
properties.setProperty("serverAddr", "localhost:8848");
properties.setProperty("namespace", "public");
NamingService naming = NamingFactory.createNamingService(properties);

// 2. 注册实例
naming.registerInstance("my-service", "192.168.1.100", 8080);

// 3. 订阅服务变更
naming.subscribe("my-service", event -> {
    NamingEvent namingEvent = (NamingEvent) event;
    System.out.println("实例列表变更: " + namingEvent.getInstances());
});

// 4. 查询实例
List<Instance> instances = naming.getAllInstances("my-service");

// 5. 负载均衡选择一个健康实例
Instance instance = naming.selectOneHealthyInstance("my-service");

// 6. 注销实例
naming.deregisterInstance("my-service", "192.168.1.100", 8080);

// 7. 关闭资源
naming.shutDown();
```
