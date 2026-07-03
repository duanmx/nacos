# Naming 服务端 - 03 - 数据模型（ServiceManager / Client 抽象 / 享元）

> **原则声明**：本文所有结论均来自 Nacos 服务端源码，标注了精确的文件路径与行号，不含任何主观猜测。阅读时可对照源码逐行验证。
>
> 版本：`3.2.1-SNAPSHOT` ｜ 主分支：`develop`

---

## 目录

1. [为什么 2.x 要重构数据模型](#1-为什么-2x-要重构数据模型)
2. [两条正交的主线：Service 与 Client](#2-两条正交的主线service-与-client)
3. [Service：不可变值对象](#3-service不可变值对象)
4. [ServiceManager：享元 + 单例仓库](#4-servicemanager享元--单例仓库)
5. [Client 接口：发布与订阅两个维度](#5-client-接口发布与订阅两个维度)
6. [AbstractClient：公共存储骨架](#6-abstractclient公共存储骨架)
7. [两种 Client 实现：连接型 vs IP端口型](#7-两种-client-实现连接型-vs-ip端口型)
8. [revision：两种重算策略](#8-revision两种重算策略)
9. [关键设计决策](#9-关键设计决策)
10. [线程安全分析](#10-线程安全分析)
11. [数据模型全景图](#11-数据模型全景图)
12. [总结](#12-总结)
13. [文件索引表](#13-文件索引表)

---

## 1. 为什么 2.x 要重构数据模型

Nacos 1.x 的数据模型是「**Service → Cluster → Instance**」三层树，实例直接挂在服务下。这套模型在纯 HTTP 时代没问题，但进入 gRPC 长连接时代后暴露了痛点：

- **连接与数据脱节**：一个 gRPC 连接可能注册多个服务的多个实例。连接断开时，要遍历所有服务去找「哪些实例属于这个连接」，代价高昂。
- **一致性同步粒度粗**：Distro 协议按「连接」同步比按「服务」同步更自然。

2.x 的解法是引入 **Client 抽象**，把数据模型翻转为：「**Client 拥有它发布的实例、订阅的服务**」。实例的归属从「Service 名下」变为「Client 名下」。这样：

- 连接断开 → 直接拿到该 Client → 一次性清理它名下所有实例与订阅。
- Distro 同步 → 以 Client 为单位打包（`generateSyncData`）。

本篇聚焦这套新模型的两个核心：**Service（被引用的目标）** 与 **Client（数据的实际持有者）**。

---

## 2. 两条正交的主线：Service 与 Client

```
        ┌────────────────────────────┐        ┌────────────────────────────┐
        │        Service（POJO）      │        │        Client（接口）        │
        │  namespace + group + name   │        │  clientId                   │
        │  不可变值对象、享元 key       │◄──────┤  publishers: Service→实例    │
        └────────────┬───────────────┘  引用   │  subscribers: Service→订阅者 │
                     │                          └────────────────────────────┘
                     │ 由 ServiceManager 单例化
                     ▼
        ┌────────────────────────────┐
        │      ServiceManager         │
        │  singletonRepository        │  保证同一个 Service 全局唯一实例
        │  （享元池）                  │
        └────────────────────────────┘
```

- **Service** 是「被指向的目标」，本身不持有实例，只是一个由 namespace+group+name 唯一确定的标识。
- **Client** 是「数据的实际持有者」，内部用 `Map<Service, ...>` 记录「我发布了哪些服务的实例」「我订阅了哪些服务」。
- **ServiceManager** 保证 Service 对象在内存中**全局唯一**（享元），这样所有 Client 的 Map 都能用「同一个 Service 引用」作 key，进而支撑倒排索引（第 04 篇）。

---

## 3. Service：不可变值对象

文件：[Service.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/pojo/Service.java)

### 3.1 不可变设计

`Service` 的三个身份字段全部是 `final`（L35-41）：

```java
public class Service implements Serializable {
    private final String namespace;
    private final String group;
    private final String name;
    private final boolean ephemeral;
    private final AtomicLong revision;
    private long lastUpdatedTime;

    private Service(String namespace, String group, String name, boolean ephemeral) { ... } // 私有构造

    public static Service newService(String namespace, String group, String name) {
        return newService(namespace, group, name, true);   // 默认临时
    }
}
```

构造函数私有（L47），只能通过静态工厂 `newService`（L56-63）创建，默认 `ephemeral=true`（L57）。这是典型的**不可变值对象**：身份一旦确定不可更改，天然线程安全，适合做 Map 的 key。

### 3.2 equals / hashCode 只认「身份三元组」

关键在于 `equals`（L106-117）与 `hashCode`（L119-122）**只比较 namespace + group + name**，忽略 `ephemeral`、`revision`、`lastUpdatedTime`：

```java
@Override
public boolean equals(Object o) {
    if (this == o) { return true; }
    if (!(o instanceof Service)) { return false; }
    Service service = (Service) o;
    return namespace.equals(service.namespace) && group.equals(service.group)
            && name.equals(service.name);           // 仅三元组
}

@Override
public int hashCode() {
    return Objects.hash(namespace, group, name);    // 仅三元组
}
```

**这是享元模式成立的前提**：只要 namespace+group+name 相同，两个 `Service` 对象就「相等」，ServiceManager 才能据此去重，把它们收敛成同一个单例。

---

## 4. ServiceManager：享元 + 单例仓库

文件：[ServiceManager.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/ServiceManager.java)

### 4.1 自身是单例

```java
public class ServiceManager {
    private static final ServiceManager INSTANCE = new ServiceManager();      // 饿汉单例

    private final ConcurrentHashMap<Service, Service> singletonRepository;    // 享元池
    private final ConcurrentHashMap<String, Set<Service>> namespaceSingletonMaps; // 命名空间→服务集

    private ServiceManager() {
        singletonRepository = new ConcurrentHashMap<>(1 << 10);   // 初始 1024
        namespaceSingletonMaps = new ConcurrentHashMap<>(1 << 2); // 初始 4
    }

    public static ServiceManager getInstance() { return INSTANCE; }
}
```

`INSTANCE` 是饿汉式静态单例（L36），类加载即初始化，天然线程安全。它维护两个并发容器（L38-40）：
- `singletonRepository`：**享元池**，key 和 value 都是 Service，`Map<Service, Service>`。
- `namespaceSingletonMaps`：按命名空间归类，便于「列出某命名空间下所有服务」。

### 4.2 getSingleton —— 享元的核心

```java
public Service getSingleton(Service service) {
    Service result = singletonRepository.computeIfAbsent(service, key -> {
        NotifyCenter.publishEvent(new MetadataEvent.ServiceMetadataEvent(service, false)); // 首次创建才发事件
        return service;
    });
    namespaceSingletonMaps
            .computeIfAbsent(result.getNamespace(), namespace -> new ConcurrentHashSet<>())
            .add(result);
    return result;
}
```

`getSingleton`（L61-70）是理解享元的钥匙：

1. `computeIfAbsent`：若池中已有等价 Service（equals 相同），返回**已存在的对象**；否则把传入的对象放进去并返回它。
2. **只有首次创建**才发 `ServiceMetadataEvent`（L63）——因为 `computeIfAbsent` 的 lambda 只在 key 缺失时执行。
3. 无论新旧，都把结果登记到 `namespaceSingletonMaps`。

**效果**：无论多少个 Client 调用 `getSingleton(sameService)`，全局只会有**一个** Service 对象实例。这正是第 02 篇里 `registerInstance` 一进来就调 `getSingleton` 的原因——把「用户传入的临时 Service 对象」替换成「全局唯一的单例引用」，后续所有模型和索引才能用引用一致性对齐。

### 4.3 其他方法

| 方法 | 行号 | 作用 |
|------|------|------|
| `getSingletonIfExist(Service)` | L90-92 | 存在才返回（Optional），不创建——第 02 篇订阅用它 |
| `removeSingleton` | L104-110 | 从两个池中移除 |
| `containSingleton` | L112-114 | 是否存在——第 02 篇注销时短路判断用它 |
| `getSingletons(namespace)` | L51-53 | 列出命名空间下所有服务 |

---

## 5. Client 接口：发布与订阅两个维度

文件：[Client.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/Client.java)

接口 Javadoc 一语道破本质（L28-29）：「存储 **which services the client has published and subscribed**」——客户端发布了哪些服务、订阅了哪些服务。

方法可分四组：

| 组别 | 方法 | 行号 |
|------|------|------|
| **身份** | `getClientId`、`isEphemeral` | L40、L47 |
| **发布（我注册的实例）** | `addServiceInstance`、`removeServiceInstance`、`getInstancePublishInfo`、`getAllPublishedService` | L68、L76、L84、L91 |
| **订阅（我关心的服务）** | `addServiceSubscriber`、`removeServiceSubscriber`、`getSubscriber`、`getAllSubscribeService` | L100、L108、L116、L123 |
| **生命周期 / 同步** | `generateSyncData`、`isExpire`、`release`、`recalculateRevision`/`getRevision`/`setRevision` | L130、L138、L143、L149-161 |

**「发布」与「订阅」是两个正交维度**：同一个 Client 既可以注册实例（作为服务提供者），也可以订阅服务（作为服务消费者）。这就是接口把它们拆成两组对称方法的原因。

---

## 6. AbstractClient：公共存储骨架

文件：[AbstractClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/AbstractClient.java)

`AbstractClient`（L44）用两个并发容器承载「发布」与「订阅」：

```java
public abstract class AbstractClient implements Client {

    protected final ConcurrentHashMap<Service, InstancePublishInfo> publishers =
            new ConcurrentHashMap<>(16, 0.75f, 1);          // 我发布的实例：Service → 实例
    protected final ConcurrentHashMap<Service, Subscriber> subscribers =
            new ConcurrentHashMap<>(16, 0.75f, 1);          // 我订阅的服务：Service → 订阅者

    protected volatile long lastUpdatedTime;
    protected final AtomicLong revision;                    // 版本号
    protected ClientAttributes attributes;
}
```

- `publishers`（L46-47）：key 是 Service 单例引用，value 是该客户端在此服务下发布的实例信息。
- `subscribers`（L49-50）：key 是 Service，value 是订阅者。
- 两个 Map 的并发级别都设为 `1`（构造参数第三个），因为单个 Client 的写并发不高。
- `revision` 用 `AtomicLong`（L54），保证版本号自增/设置的原子性。

### 6.1 addServiceInstance —— 挂实例并发事件

```java
@Override
public boolean addServiceInstance(Service service, InstancePublishInfo instancePublishInfo) {
    if (instancePublishInfo instanceof BatchInstancePublishInfo) {
        InstancePublishInfo old = publishers.put(service, instancePublishInfo);
        MetricsMonitor.incrementIpCountWithBatchRegister(old, (BatchInstancePublishInfo) instancePublishInfo);
    } else {
        if (null == publishers.put(service, instancePublishInfo)) {
            MetricsMonitor.incrementInstanceCount();       // 新增才计数
        }
    }
    NotifyCenter.publishEvent(new ClientEvent.ClientChangedEvent(this)); // 通知 Client 变更（Distro 同步）
    Loggers.SRV_LOG.info("Client change for service {}, {}", service, getClientId());
    return true;
}
```

`addServiceInstance`（L73-87）除了把实例放进 `publishers`，还做两件事：
1. **指标统计**：区分批量/单个，维护实例计数。
2. **发 `ClientChangedEvent`**（L84）：这是 Distro AP 协议感知「本地 Client 变了、需要同步给其他节点」的信号（第 06 篇展开）。

注意区分：第 02 篇 `EphemeralClientOperationServiceImpl` 发的是 `ClientRegisterServiceEvent`（驱动索引/推送），而这里发的是 `ClientChangedEvent`（驱动集群同步）——**两个事件、两个关注点**。

`removeServiceInstance`（L89-102）对称：移除后若确有实例，同样发 `ClientChangedEvent`。

### 6.2 订阅相关

`addServiceSubscriber`（L114-120）/ `removeServiceSubscriber`（L122-128）只操作 `subscribers` Map 与订阅指标，**不发 `ClientChangedEvent`**——订阅关系不参与 Distro 同步（订阅是本地连接的状态）。

### 6.3 generateSyncData —— 打包给 Distro

`generateSyncData`（L140-173）把 `publishers` 里的所有实例（区分普通/批量）打包成 `ClientSyncData`，并带上当前 revision（L171）。这是 Distro 协议做全量/增量同步的数据载体（第 06 篇）。

---

## 7. 两种 Client 实现：连接型 vs IP端口型

`AbstractClient` 有两个具体子类，对应两种接入方式：

### 7.1 ConnectionBasedClient —— gRPC 长连接

文件：[ConnectionBasedClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/impl/ConnectionBasedClient.java)

```java
public class ConnectionBasedClient extends AbstractClient {
    private final String connectionId;
    private final boolean isNative;       // true=直连本节点，false=从其他节点同步来的
    private volatile long lastRenewTime;

    @Override
    public boolean isEphemeral() { return true; }   // 恒为临时
}
```

- 绑定一个 **gRPC 连接**（`connectionId` 即 clientId，L52-54）。
- `isEphemeral()` **恒返回 true**（L57-59）——连接型客户端一定是临时的，连接断即数据清。
- `isNative`（L37）区分「本机直连」和「Distro 从其他节点同步过来」的客户端。
- `isExpire`（L73-77）：只有**非原生**（同步来的）客户端才会因超过续约时间而过期——本机直连的过期由连接断开事件驱动，而非超时。

### 7.2 IpPortBasedClient —— IP:端口

文件：[IpPortBasedClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/impl/IpPortBasedClient.java)

```java
public class IpPortBasedClient extends AbstractClient {
    public static final String ID_DELIMITER = "#";
    private final String clientId;
    private final boolean ephemeral;            // 可临时可持久
    private ClientBeatCheckTaskV2 beatCheckTask;    // 临时：心跳检测
    private HealthCheckTaskV2 healthCheckTaskV2;    // 持久：健康检查
}
```

它模拟「一个 TCP 会话客户端」，主要用于 **1.x 兼容的 HTTP 注册** 和**持久实例**：

- clientId 形如 `ip:port#ephemeral`（`getClientId(address, ephemeral)` L69-71）。
- `ephemeral` 可真可假（L45），既支持临时也支持持久。
- **重写 `addServiceInstance`**（L87-90）：先把实例包装成 `HealthCheckInstancePublishInfo`（`parseToHealthCheckInstance` L113-130）再交给父类，因为 IP端口型实例需要挂健康检查数据。
- **init()**（L135-143）：临时实例启动 `ClientBeatCheckTaskV2`（心跳检测），持久实例启动 `HealthCheckTaskV2`（主动健康检查）——健康检查机制详见第 07 篇。
- `putServiceInstance`（L148-152）：一个「纯放入、不发事件」的方法，供 Distro 同步落地时使用（同步来的数据不该再触发本地事件）。

### 7.3 对比

| 维度 | ConnectionBasedClient | IpPortBasedClient |
|------|----------------------|-------------------|
| 绑定 | gRPC 连接 | IP:端口 |
| isEphemeral | 恒 true | 可 true 可 false |
| 过期判定 | 非原生 + 超续约时间 | 临时 + 无实例 + 超时 |
| 健康检查 | 无（靠连接存活） | 心跳/主动检查任务 |
| 主要场景 | 2.x gRPC 注册 | 1.x HTTP 兼容 / 持久实例 |

---

## 8. revision：两种重算策略

`recalculateRevision` 在两个实现里**策略不同**，这是一个容易忽略但很关键的设计点：

### 8.1 ConnectionBasedClient：简单自增

```java
@Override
public long recalculateRevision() {
    return revision.addAndGet(1);      // 每次变更 +1
}
```

（`ConnectionBasedClient.java` L79-82）连接型客户端每次变更就把 revision **加 1**。因为连接型是「本机权威数据」，自增即可标识版本递进。

### 8.2 AbstractClient（IpPortBasedClient 沿用）：内容哈希

```java
@Override
public long recalculateRevision() {
    int hash = DistroUtils.hash(this);   // 基于内容算哈希
    revision.set(hash);
    return hash;
}
```

（`AbstractClient.java` L202-207）默认实现用 `DistroUtils.hash(this)` 基于**客户端当前内容**算哈希做 revision。

**为什么要两种策略？**

- 连接型是单一权威源，自增计数足够表达「变了几次」。
- IP端口型（尤其持久实例）会在多节点间通过 Distro/Raft 同步，**用内容哈希做 revision**可以让不同节点在数据内容相同时得到相同的 revision，便于比对「两个节点的数据是否一致」，而不受各自变更次数差异影响。

第 02 篇 `registerInstance` 里那句 `client.recalculateRevision()`，在连接型上是 `+1`，在 IP端口型上是「重算内容哈希」——同一行代码，两种语义。

---

## 9. 关键设计决策

### 9.1 为什么把实例挂在 Client 而非 Service？

回到第 1 节的痛点。挂在 Client 上后：
- **连接断开清理 O(1) 定位**：拿到 Client 直接 `release()`，遍历它自己的 `publishers` 即可，无需扫全局。
- **同步单位自然**：Distro 以 Client 为粒度打包同步。

代价是「查某个服务有哪些实例」需要反查——这正是第 04 篇**倒排索引**要解决的问题。

### 9.2 为什么 Service 要享元？

若每个 Client 的 `publishers` Map 都持有各自 new 出来的 Service 对象，虽然 equals 相等，但：
- 内存里有海量重复 Service 对象；
- 倒排索引（`Map<Service, Set<clientId>>`）无法用引用一致性快速对齐。

享元后全局唯一引用，内存省、对齐快。

### 9.3 为什么用两种 revision 策略？

见第 8 节：单一权威源用自增，多副本同步用内容哈希，各取所长。

### 9.4 为什么 addServiceInstance 发 ClientChangedEvent 而非业务事件？

职责分层：`AbstractClient` 是「模型层」，只关心「我变了，需要被同步」（`ClientChangedEvent` → Distro）；「注册这个业务动作意味着什么」（索引、推送）由上层 `ClientOperationService` 发 `ClientRegisterServiceEvent` 表达。模型层不该知道业务语义。

---

## 10. 线程安全分析

| 组件 | 机制 | 说明 |
|------|------|------|
| `Service` | 不可变（final 字段） | 天然线程安全，可安全做 Map key |
| `ServiceManager.INSTANCE` | 饿汉静态单例 | 类加载即初始化，无竞态 |
| `singletonRepository` | `ConcurrentHashMap` + `computeIfAbsent` | 原子的「查或建」，多线程注册同服务只产生一个单例 |
| `publishers`/`subscribers` | `ConcurrentHashMap`（并发级别1） | 单 Client 写并发低，读写安全 |
| `lastUpdatedTime` | `volatile` | 保证可见性 |
| `revision` | `AtomicLong` | 自增/设置原子 |

**一个细节**：`getSingleton` 用 `computeIfAbsent` 而非「先 get 再 put」，避免了「检查-创建」之间的竞态窗口，保证 `ServiceMetadataEvent` 对同一服务只发一次。

---

## 11. 数据模型全景图

```
                          ServiceManager (单例)
                          ┌──────────────────────────────┐
                          │ singletonRepository           │
                          │   {Service(A)} → Service(A)    │  ← 享元池，全局唯一引用
                          │   {Service(B)} → Service(B)    │
                          │ namespaceSingletonMaps         │
                          │   "public" → {A, B}            │
                          └──────────────┬─────────────────┘
                                         │ 提供唯一 Service 引用
        ┌────────────────────────────────┼────────────────────────────────┐
        ▼                                 ▼                                 ▼
  ConnectionBasedClient           IpPortBasedClient              (更多 Client...)
  clientId=conn-1                 clientId=1.1.1.1#true
  ┌───────────────────┐          ┌───────────────────┐
  │ publishers        │          │ publishers        │
  │  Service(A)→实例   │          │  Service(A)→实例   │  ← 都引用同一个 Service(A)
  │ subscribers       │          │ subscribers       │
  │  Service(B)→订阅者 │          │                   │
  │ revision(自增)     │          │ revision(内容哈希) │
  └───────────────────┘          └───────────────────┘
        │                                 │
        │ addServiceInstance 发           │
        ▼ ClientChangedEvent              ▼
     Distro 同步（第06篇）           健康检查任务（第07篇）
```

---

## 12. 总结

1. **模型翻转**：2.x 把「实例挂 Service」改为「实例挂 Client」，让连接断开清理、Distro 同步都以 Client 为自然单位。
2. **Service 是不可变值对象**：`final` 三元组 + 只按 namespace+group+name 做 equals/hashCode，是享元的前提。
3. **ServiceManager 是享元池 + 饿汉单例**：`computeIfAbsent` 保证同一服务全局唯一引用，首次创建才发 `ServiceMetadataEvent`。
4. **Client 双维度**：`publishers`（我发布的实例）与 `subscribers`（我订阅的服务）两个正交的 `ConcurrentHashMap`。
5. **两种实现**：`ConnectionBasedClient`（gRPC 连接，恒临时）与 `IpPortBasedClient`（IP:端口，含健康检查，兼容 1.x/持久）。
6. **两种 revision**：连接型简单自增；IP端口型（默认实现）用内容哈希，服务于多副本一致性比对。
7. **事件分层**：模型层发 `ClientChangedEvent`（同步），业务层发 `ClientRegisterServiceEvent`（索引/推送），关注点分离。

下一篇（第 04 篇）将讲 **事件驱动的倒排索引**：既然实例挂在 Client 上，「查某服务有哪些实例/订阅者」如何高效实现——`ClientServiceIndexesManager` 如何订阅本篇提到的各类事件，动态维护 `Service → clientId 集合` 的倒排表。

---

## 13. 文件索引表

| 文件 | 关键内容 | 关键行号 |
|------|----------|----------|
| [Service.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/pojo/Service.java) | 不可变值对象、私有构造+静态工厂、equals/hashCode 仅三元组 | final 字段 L35-43；newService L56-63；equals L106-117；hashCode L119-122 |
| [ServiceManager.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/ServiceManager.java) | 饿汉单例、享元池、getSingleton | INSTANCE L36；两池 L38-40；getSingleton L61-70；getSingletonIfExist L90-92；removeSingleton L104-110；containSingleton L112-114 |
| [Client.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/Client.java) | 客户端抽象接口、发布/订阅双维度 | 发布 L68-91；订阅 L100-123；同步/生命周期 L130-161 |
| [AbstractClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/AbstractClient.java) | 公共存储骨架、addServiceInstance、generateSyncData、默认 revision=内容哈希 | publishers/subscribers L46-50；addServiceInstance L73-87；removeServiceInstance L89-102；generateSyncData L140-173；recalculateRevision L202-207 |
| [ConnectionBasedClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/impl/ConnectionBasedClient.java) | gRPC 连接型、恒临时、revision 自增 | isEphemeral L56-59；isNative L61-63；isExpire L73-77；recalculateRevision L79-82 |
| [IpPortBasedClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/impl/IpPortBasedClient.java) | IP端口型、健康检查包装、init 任务 | clientId 拼接 L69-71；addServiceInstance 重写 L87-90；parseToHealthCheckInstance L113-130；init L135-143；putServiceInstance L148-152 |

---

> 系列导航：
> - [第 01 篇：gRPC 请求处理链路](./Naming服务端-01-gRPC请求处理链路.md)
> - [第 02 篇：客户端操作服务](./Naming服务端-02-客户端操作服务.md)
> - **第 03 篇：数据模型（ServiceManager / Client 抽象）（本篇）**
> - 第 04 篇：事件驱动倒排索引（ClientServiceIndexesManager）*（待续）*
