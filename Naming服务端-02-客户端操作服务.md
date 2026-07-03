# Naming 服务端 - 02 - 客户端操作服务（注册 / 注销 / 订阅）

> **原则声明**：本文所有结论均来自 Nacos 服务端源码，标注了精确的文件路径与行号，不含任何主观猜测。阅读时可对照源码逐行验证。
>
> 版本：`3.2.1-SNAPSHOT` ｜ 主分支：`develop`

---

## 目录

1. [为什么需要「客户端操作服务」这一层](#1-为什么需要客户端操作服务这一层)
2. [在整体链路中的位置](#2-在整体链路中的位置)
3. [核心抽象：ClientOperationService 接口](#3-核心抽象clientoperationservice-接口)
4. [临时实例路径：EphemeralClientOperationServiceImpl](#4-临时实例路径ephemeralclientoperationserviceimpl)
5. [持久实例路径：PersistentClientOperationServiceImpl](#5-持久实例路径persistentclientoperationserviceimpl)
6. [统一范式：改模型 → 发事件](#6-统一范式改模型--发事件)
7. [事件家族：ClientOperationEvent](#7-事件家族clientoperationevent)
8. [关键设计决策](#8-关键设计决策)
9. [线程安全与异常边界](#9-线程安全与异常边界)
10. [完整数据流转时间线](#10-完整数据流转时间线)
11. [总结](#11-总结)
12. [文件索引表](#12-文件索引表)

---

## 1. 为什么需要「客户端操作服务」这一层

在 [第 01 篇](./Naming服务端-01-gRPC请求处理链路.md) 中，请求经过四层链路，最终落到 `InstanceRequestHandler.handle()` 这样的具体 Handler。但 Handler 本身并不直接操作数据模型，而是转手交给 **ClientOperationService**。

这一层存在的意义是把「协议无关的业务动作」独立出来：

- **屏蔽协议差异**：无论请求来自 gRPC 还是 HTTP，Handler / Controller 都调同一个 `registerInstance(service, instance, clientId)`。
- **屏蔽一致性差异**：临时实例走 AP（Distro）、持久实例走 CP（Raft），两条路径被同一个接口收敛，上层无感知。
- **统一副作用出口**：所有「模型变更」都通过这里发出**领域事件**，倒排索引、推送、元数据管理等下游模块只订阅事件，彼此解耦（下游细节见后续篇章）。

一句话：**它是「一次客户端写操作」的业务语义中枢**。

---

## 2. 在整体链路中的位置

```
gRPC / HTTP 请求
      │
      ▼
InstanceRequestHandler.handle()          ← 第 01 篇：协议入口
      │  clientOperationService.registerInstance(service, instance, clientId)
      ▼
┌───────────────────────────────────────────────────────┐
│           ClientOperationService（接口）                │  ← 本篇
│                                                         │
│   ┌─────────────────────────┐  ┌─────────────────────┐ │
│   │ EphemeralClientOperation │  │ PersistentClient... │ │
│   │  ServiceImpl（AP 直连）   │  │  ServiceImpl（CP）  │ │
│   └───────────┬─────────────┘  └──────────┬──────────┘ │
└───────────────┼───────────────────────────┼────────────┘
                │ 直接改模型                  │ Raft 提交 → onApply 改模型
                ▼                            ▼
        Client 模型（实例挂在 Client 上）
                │
                ▼
        NotifyCenter.publishEvent(...)      ← 领域事件出口
                │
     ┌──────────┼──────────────┐
     ▼          ▼              ▼
  倒排索引    推送模块       元数据管理     ← 后续篇章
```

关键认知：**Nacos 2.x 起，实例不再直接挂在 Service 上，而是挂在 Client 上**。「谁注册的实例，就记在谁（Client）的名下」。ClientOperationService 正是操作 Client 模型的入口。

---

## 3. 核心抽象：ClientOperationService 接口

文件：[ClientOperationService.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/ClientOperationService.java)

### 3.1 接口方法

接口定义了五个操作方法（`ClientOperationService.java` L46-87）：

```java
public interface ClientOperationService {

    // 注册单个实例（临时实例可能抛 NacosException）
    void registerInstance(Service service, Instance instance, String clientId) throws NacosException;

    // 批量注册
    void batchRegisterInstance(Service service, List<Instance> instances, String clientId);

    // 注销
    void deregisterInstance(Service service, Instance instance, String clientId);

    // 订阅（default 空实现）
    default void subscribeService(Service service, Subscriber subscriber, String clientId) { }

    // 取消订阅（default 空实现）
    default void unsubscribeService(Service service, Subscriber subscriber, String clientId) { }
}
```

**设计细节**：`subscribeService` / `unsubscribeService` 被设计成 **default 空实现**（L74-87）。原因是——订阅只对临时客户端有意义，持久实现根本不支持订阅（见第 5 节），用 default 空实现避免持久实现被迫写无意义代码。

### 3.2 内置的转换逻辑：getPublishInfo

接口中还有一个 **default 方法** `getPublishInfo(Instance)`（`ClientOperationService.java` L97-118），负责把对外的 API 模型 `Instance` 转换成内部存储模型 `InstancePublishInfo`：

```java
default InstancePublishInfo getPublishInfo(Instance instance) {
    InstancePublishInfo result = new InstancePublishInfo(instance.getIp(), instance.getPort());
    Map<String, Object> extendDatum = result.getExtendDatum();
    if (null != instance.getMetadata() && !instance.getMetadata().isEmpty()) {
        extendDatum.putAll(instance.getMetadata());                              // 元数据
    }
    if (StringUtils.isNotEmpty(instance.getInstanceId())) {
        extendDatum.put(Constants.CUSTOM_INSTANCE_ID, instance.getInstanceId()); // 自定义实例 ID
    }
    if (Math.abs(Constants.DEFAULT_INSTANCE_WEIGHT - instance.getWeight()) >= EPSILON) {
        extendDatum.put(Constants.PUBLISH_INSTANCE_WEIGHT, instance.getWeight()); // 非默认权重才存
    }
    if (!instance.isEnabled()) {
        extendDatum.put(Constants.PUBLISH_INSTANCE_ENABLE, instance.isEnabled()); // 非启用才存
    }
    String clusterName = StringUtils.isBlank(instance.getClusterName())
            ? UtilsAndCommons.DEFAULT_CLUSTER_NAME : instance.getClusterName();
    result.setHealthy(instance.isHealthy());
    result.setCluster(clusterName);
    return result;
}
```

**两个值得注意的点**：

1. **浮点比较用 EPSILON**：`EPSILON = 1e-10`（L89），权重是 `double`，用 `Math.abs(a - b) >= EPSILON` 判断「是否等于默认值」，避免浮点直接 `==` 的精度陷阱。
2. **「非默认才存」的节流策略**：只有权重不等于默认值、实例被禁用时才写入 `extendDatum`。默认值不落盘，减少内存与同步开销。

**为什么放在接口里当 default 方法？** 因为临时实现和持久实现都要做同样的转换，放接口里天然复用，无需抽象类。

---

## 4. 临时实例路径：EphemeralClientOperationServiceImpl

文件：[EphemeralClientOperationServiceImpl.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/impl/EphemeralClientOperationServiceImpl.java)

这是**默认路径**（绝大多数服务注册都是临时实例）。Bean 名 `ephemeralClientOperationService`（L46），构造时注入 `ClientManagerDelegate`（L51-53）。

### 4.1 registerInstance —— 范式的样板

`registerInstance`（L55-78）是理解整个操作服务的钥匙：

```java
@Override
public void registerInstance(Service service, Instance instance, String clientId) throws NacosException {
    NamingUtils.checkInstanceIsLegal(instance);                                        // ① 参数校验

    Service singleton = ServiceManager.getInstance().getSingleton(service);            // ② 取/建单例 Service
    if (!singleton.isEphemeral()) {                                                    // ③ 临时/持久互斥校验
        throw new NacosRuntimeException(NacosException.INVALID_PARAM,
                String.format("Current service %s is persistent service, can't register ephemeral instance.",
                        singleton.getGroupedServiceName()));
    }
    Client client = clientManager.getClient(clientId);                                 // ④ 取 Client
    checkClientIsLegal(client, clientId);                                              // ⑤ Client 合法性校验
    InstancePublishInfo instanceInfo = getPublishInfo(instance);                       // ⑥ 模型转换
    client.addServiceInstance(singleton, instanceInfo);                               // ⑦ 改模型：实例挂到 Client
    client.setLastUpdatedTime();                                                       // ⑧ 更新时间戳
    client.recalculateRevision();                                                      // ⑨ 重算版本号
    NotifyCenter.publishEvent(
            new ClientOperationEvent.ClientRegisterServiceEvent(singleton, clientId)); // ⑩ 发注册事件
    NotifyCenter.publishEvent(
            new MetadataEvent.InstanceMetadataEvent(singleton, instanceInfo.getMetadataId(), false)); // ⑪ 发元数据事件
}
```

九个动作可归纳成三段：

| 阶段 | 步骤 | 作用 |
|------|------|------|
| **校验** | ①②③④⑤ | 参数合法、Service 单例化、临时/持久不能混、Client 存在且类型匹配 |
| **改模型** | ⑥⑦⑧⑨ | 转换模型 → 挂实例 → 更新时间戳 → 重算 revision |
| **发事件** | ⑩⑪ | 一个业务事件 + 一个元数据事件 |

- **② getSingleton**（L60）：`ServiceManager` 保证同一个 `Service`（namespace+group+name）在内存中只有一个单例对象（享元模式，详见第 03 篇），后续所有索引都以这个单例引用为 key。
- **⑨ recalculateRevision**（L72）：Client 维护一个版本号，客户端变更后重算，用于后续推送时判断「数据是否真的变了」，避免无意义推送。

### 4.2 batchRegisterInstance —— 批量版

`batchRegisterInstance`（L80-106）逻辑与单个注册几乎一致，差别在于：

- 把多个 `Instance` 转换后装进一个 `BatchInstancePublishInfo`（L91-97）；
- 一次性 `client.addServiceInstance(singleton, batchInstancePublishInfo)`（L98）；
- 同样发 `ClientRegisterServiceEvent` + `InstanceMetadataEvent`（L101-105）。

**注意**：批量注册**没有** `throws NacosException`，也没有调 `NamingUtils.checkInstanceIsLegal` 逐个校验（校验前置到了调用方 Handler）。

### 4.3 deregisterInstance —— 注销

`deregisterInstance`（L108-127）多了一层「服务不存在直接返回」的短路：

```java
@Override
public void deregisterInstance(Service service, Instance instance, String clientId) {
    if (!ServiceManager.getInstance().containSingleton(service)) {              // 服务都不存在，注销无意义
        Loggers.SRV_LOG.warn("remove instance from non-exist service: {}", service);
        return;
    }
    Service singleton = ServiceManager.getInstance().getSingleton(service);
    Client client = clientManager.getClient(clientId);
    checkClientIsLegal(client, clientId);
    InstancePublishInfo removedInstance = client.removeServiceInstance(singleton); // 摘实例
    client.setLastUpdatedTime();
    client.recalculateRevision();
    if (null != removedInstance) {                                              // 确实摘掉了才发事件
        NotifyCenter.publishEvent(
                new ClientOperationEvent.ClientDeregisterServiceEvent(singleton, clientId));
        NotifyCenter.publishEvent(
                new MetadataEvent.InstanceMetadataEvent(singleton, removedInstance.getMetadataId(), true));
    }
}
```

**两处防御式编程**：
1. 服务不存在 → 打 warn 日志后直接 return，不抛异常（注销要幂等友好）。
2. `removedInstance` 为 null（本来就没这个实例）→ 不发事件，避免下游做无用功。

注意元数据事件最后一个参数从注册时的 `false` 变成了 `true`（L124），表示「这是删除动作」。

### 4.4 subscribeService / unsubscribeService —— 订阅

`subscribeService`（L129-139）与注册相比有一个关键差异——**取 Service 单例用的是 `getSingletonIfExist`**：

```java
@Override
public void subscribeService(Service service, Subscriber subscriber, String clientId) {
    Service singleton = ServiceManager.getInstance().getSingletonIfExist(service).orElse(service); // 存在就用单例，不存在用入参
    Client client = clientManager.getClient(clientId);
    checkClientIsLegal(client, clientId);
    client.addServiceSubscriber(singleton, subscriber);       // 记录订阅关系
    client.setLastUpdatedTime();
    NotifyCenter.publishEvent(
            new ClientOperationEvent.ClientSubscribeServiceEvent(singleton, clientId));
}
```

**为什么订阅用 `getSingletonIfExist().orElse(service)` 而注册用 `getSingfor`？**

- **注册**用 `getSingleton`：注册意味着「服务应该存在」，没有就创建。
- **订阅**用 `getSingletonIfExist`：订阅一个还不存在的服务是合法的（客户端可以先订阅、后有实例）。若服务尚未创建，就不必强行创建一个空 Service 污染内存，直接用入参对象占位即可。

`unsubscribeService`（L141-151）逻辑对称：`removeServiceSubscriber` + 发 `ClientUnsubscribeServiceEvent`。

订阅操作**不调用 `recalculateRevision`**——因为订阅不改变「本客户端发布的实例数据」，只是记录「本客户端关心哪些服务」，无需版本号参与推送判定。

### 4.5 checkClientIsLegal —— Client 合法性守卫

`checkClientIsLegal`（L153-168）是所有写操作的公共守卫：

```java
private void checkClientIsLegal(Client client, String clientId) {
    if (client == null) {                                           // 连接已断开
        Loggers.SRV_LOG.warn("Client connection {} already disconnect", clientId);
        throw new NacosRuntimeException(NacosException.CLIENT_DISCONNECT, ...);
    }
    if (!client.isEphemeral()) {                                    // 类型不匹配
        Loggers.SRV_LOG.warn("Client connection {} type is not ephemeral", clientId);
        throw new NacosRuntimeException(NacosException.INVALID_PARAM, ...);
    }
}
```

两种非法情况：
1. **client == null**：连接已断开（clientId 已从 ClientManager 移除），抛 `CLIENT_DISCONNECT`。
2. **!client.isEphemeral()**：拿到的是持久 Client，却来做临时操作，类型错配，抛 `INVALID_PARAM`。

---

## 5. 持久实例路径：PersistentClientOperationServiceImpl

文件：[PersistentClientOperationServiceImpl.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/impl/PersistentClientOperationServiceImpl.java)

持久实例走 **CP（Raft）** 一致性协议。这个类的最大特点：它**继承了 `RequestProcessor4CP`**（L85），既是「操作服务」又是「Raft 状态机处理器」。

```java
@Component("persistentClientOperationServiceImpl")
public class PersistentClientOperationServiceImpl extends RequestProcessor4CP
        implements ClientOperationService {

    public PersistentClientOperationServiceImpl(final PersistentIpPortClientManager clientManager) {
        this.clientManager = clientManager;
        this.protocol = ApplicationUtils.getBean(ProtocolManager.class).getCpProtocol();
        this.protocol.addRequestProcessors(Collections.singletonList(this)); // 把自己注册进 CP 协议
    }
}
```

### 5.1 写操作 = 提交 Raft 日志（不直接改模型）

与临时实现「当场改模型」不同，持久实现的 `registerInstance`（L106-131）**只是把请求打包成 Raft 日志提交出去**：

```java
@Override
public void registerInstance(Service service, Instance instance, String clientId) {
    Service singleton = ServiceManager.getInstance().getSingleton(service);
    if (singleton.isEphemeral()) {                                 // 持久实现只接受持久服务
        throw new NacosRuntimeException(NacosException.INVALID_PARAM, ...);
    }
    final InstanceStoreRequest request = new InstanceStoreRequest();
    request.setService(service);
    request.setInstance(instance);
    request.setClientId(clientId);
    final WriteRequest writeRequest = WriteRequest.newBuilder().setGroup(group())
            .setData(ByteString.copyFrom(serializer.serialize(request)))
            .setOperation(DataOperation.ADD.name())               // 标记操作类型
            .build();
    try {
        protocol.write(writeRequest);                             // 提交 Raft，等待多数派确认
    } catch (Exception e) {
        throw new NacosRuntimeException(NacosException.SERVER_ERROR, e);
    }
}
```

`deregisterInstance`（L166-183）同理，只是 `DataOperation` 换成 `DELETE`。而 `subscribeService` / `unsubscribeService` 直接抛 `UnsupportedOperationException("No persistent subscribers")`（L185-193）——**持久服务不支持订阅**。

### 5.2 onApply = Raft 提交成功后真正改模型

Raft 达成多数派共识后，回调 `onApply(WriteRequest)`（L200-235），这里才是真正改模型的地方：

```java
@Override
public Response onApply(WriteRequest request) {
    final Lock lock = readLock;
    lock.lock();                                                  // 加读锁（与快照互斥）
    try {
        final InstanceStoreRequest instanceRequest = serializer.deserialize(request.getData().toByteArray());
        final DataOperation operation = DataOperation.valueOf(request.getOperation());
        switch (operation) {
            case ADD:
                onInstanceRegister(...);                          // 落地注册
                break;
            case DELETE:
                onInstanceDeregister(...);                        // 落地注销
                break;
            case CHANGE:
                if (instanceAndServiceExist(instanceRequest)) {
                    onInstanceRegister(...);                      // 更新（存在才改）
                }
                break;
            default:
                return Response.newBuilder().setSuccess(false)...
        }
        return Response.newBuilder().setSuccess(true).build();
    } catch (Exception e) {
        ...
    } finally {
        lock.unlock();
    }
}
```

而 `onInstanceRegister`（L243-257）内部的动作，**和临时实现的 registerInstance 几乎一模一样**：

```java
private void onInstanceRegister(Service service, Instance instance, String clientId) {
    Service singleton = ServiceManager.getInstance().getSingleton(service);
    if (!clientManager.contains(clientId)) {
        clientManager.clientConnected(clientId, new ClientAttributes()); // 持久 client 按需创建
    }
    Client client = clientManager.getClient(clientId);
    InstancePublishInfo instancePublishInfo = getPublishInfo(instance);
    client.addServiceInstance(singleton, instancePublishInfo);           // 改模型（同临时）
    client.setLastUpdatedTime();
    NotifyCenter.publishEvent(new ClientOperationEvent.ClientRegisterServiceEvent(singleton, clientId)); // 发事件（同临时）
    NotifyCenter.publishEvent(new MetadataEvent.InstanceMetadataEvent(singleton, instancePublishInfo.getMetadataId(), false));
}
```

**这印证了第 6 节要讲的核心：无论 AP 还是 CP，「落地」动作都收敛为同一个范式——改模型 → 发事件。** 差别只在「什么时候落地」：AP 立即落地，CP 等 Raft 共识后落地。

---

## 6. 统一范式：改模型 → 发事件

把临时、持久两条路径的落地动作并排对比：

```
                    ┌────────────────────────────────────────────────────┐
                    │              统一落地范式                            │
                    │                                                      │
  ①校验合法性  →   │  ②getSingleton   ③client.addServiceInstance         │
                    │      ↓                    ↓                          │
                    │  Service 单例化      实例挂到 Client 名下             │
                    │                           ↓                          │
                    │                    ④setLastUpdatedTime               │
                    │                    ⑤recalculateRevision（临时）      │
                    │                           ↓                          │
                    │  ⑥NotifyCenter.publishEvent(ClientXxxServiceEvent)   │
                    │  ⑦NotifyCenter.publishEvent(InstanceMetadataEvent)   │
                    └────────────────────────────────────────────────────┘
                                          │
              ┌───────────────────────────┼────────────────────────────┐
              ▼                           ▼                             ▼
     倒排索引更新（第04篇）        触发推送（第05篇）           元数据管理
   ClientRegisterServiceEvent   服务变更 → 推给订阅者      InstanceMetadataEvent
```

**为什么坚持「发事件」而不是「直接调用下游」？**

| 若直接调用下游 | 采用事件驱动 |
|----------------|--------------|
| 操作服务需要 import 倒排索引、推送、元数据等所有下游 | 操作服务只 import `NotifyCenter` |
| 下游增减都要改操作服务代码 | 新增订阅者零侵入 |
| 同步调用，一个下游慢拖垮整条链路 | 事件异步分发，天然削峰 |
| 强耦合，难以单测 | 只需断言「事件被发出」即可单测 |

这正是 Nacos 服务端「**写操作中枢 + 事件总线**」架构的精髓（事件机制本身详见第 04、05 篇）。

---

## 7. 事件家族：ClientOperationEvent

文件：[ClientOperationEvent.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/event/client/ClientOperationEvent.java)

基类 `ClientOperationEvent extends Event`（L30-49），携带两个核心字段：`clientId` + `service`（L34-36）。它用**静态内部类**方式定义了一整个事件家族：

| 事件子类 | 行号 | 触发时机 | 携带的额外信息 |
|----------|------|----------|----------------|
| `ClientRegisterServiceEvent` | L54-61 | 实例注册成功 | 无 |
| `ClientDeregisterServiceEvent` | L66-73 | 实例注销成功 | 无 |
| `ClientSubscribeServiceEvent` | L78-85 | 订阅成功 | 无 |
| `ClientUnsubscribeServiceEvent` | L90-97 | 取消订阅成功 | 无 |
| `ClientFuzzyWatchEvent` | L102-141 | 模糊监听 | `groupKeyPattern`、`clientReceivedServiceKeys`、`isInitializing` |
| `ClientReleaseEvent` | L143-164 | 连接断开释放 | `client`、`isNative`（是否本机原生连接） |

**设计观察**：
- 前四个事件是「一对镜像」（注册↔注销、订阅↔取消订阅），结构完全对称，只靠**类型**区分语义，让下游能用 `instanceof` 精确分派。
- `ClientReleaseEvent`（L143）特殊：它携带整个 `Client` 对象（L147），因为连接断开时要一次性清理该 Client 名下的**所有**实例与订阅，需要完整上下文。`isNative`（L149）区分是本机连接还是集群同步过来的连接。

---

## 8. 关键设计决策

### 8.1 为什么用接口 + 双实现，而不是 if-else 分流？

`ClientOperationService` 一个接口，两个 Spring Bean：`ephemeralClientOperationService` 与 `persistentClientOperationServiceImpl`。上层 Handler 按需注入对应实现。

**好处**：临时/持久的逻辑差异巨大（一个直连、一个走 Raft），用**多态**隔离，比在一个方法里写 `if (ephemeral) {...} else {...}` 清晰得多，也符合开闭原则。

### 8.2 为什么临时实现「当场改」、持久实现「Raft 后改」？

这是 **AP vs CP** 的本质体现：

- 临时实例数据量大、变更频繁（心跳），追求**可用性与低延迟**（AP），本机立即写入，靠 Distro 协议异步同步到其他节点。
- 持久实例是「注册后长期存在」的关键数据，追求**强一致**（CP），必须过 Raft 多数派确认才落地，宁可牺牲一点延迟。

而两者最终的「落地动作」都复用同一范式，说明 **一致性协议 与 业务落地 被干净地分离**了。

### 8.3 为什么把 getPublishInfo 放在接口做 default 方法？

模型转换（`Instance` → `InstancePublishInfo`）是两个实现的公共逻辑。Java 8 的 default 方法让接口能承载「共享实现」，避免为了复用而额外引入抽象类，保持类型层级扁平。

### 8.4 为什么注册发两个事件而非一个？

`ClientRegisterServiceEvent`（业务事件）驱动**倒排索引与推送**；`InstanceMetadataEvent`（元数据事件）驱动**元数据管理**（如实例的扩展元数据过期清理）。两类关注点不同、订阅者不同，拆成两个事件让订阅方各取所需。

---

## 9. 线程安全与异常边界

### 9.1 临时路径的并发

- **Service 单例化**：`ServiceManager.getSingleton` 内部用并发容器保证同一 Service 只有一个实例对象（详见第 03 篇），多线程注册同一服务不会产生多个 Service。
- **Client 内部结构**：`client.addServiceInstance` 等操作的线程安全由 `Client` 实现自己保证（第 03 篇展开）。
- **事件发布**：`NotifyCenter.publishEvent` 是线程安全的，事件进入各自的发布队列异步分发。

### 9.2 持久路径的并发

`onApply` 全程持 `readLock`（L202-203、L232-234），与快照读写（`writeSnapshot`/`readSnapshot` 持写锁）互斥，保证「应用 Raft 日志」与「打/装快照」不会并发踩踏。Raft 本身串行 apply，因此这里用读锁即可（多个 apply 互不阻塞，仅与快照互斥）。

### 9.3 异常处理

| 场景 | 处理方式 | 位置 |
|------|----------|------|
| 实例参数非法 | `NamingUtils.checkInstanceIsLegal` 抛异常 | Ephemeral L58 |
| 临时/持久服务错配 | 抛 `INVALID_PARAM` | Ephemeral L61-66 / Persistent L109-114 |
| Client 已断开 | 抛 `CLIENT_DISCONNECT` | Ephemeral L154-160 |
| Client 类型错配 | 抛 `INVALID_PARAM` | Ephemeral L161-167 |
| 注销不存在的服务 | 打 warn，直接 return（幂等） | Ephemeral L110-113 |
| 注销不存在的实例 | removedInstance == null，不发事件 | Ephemeral L120 |
| Raft 提交失败 | 包装成 `SERVER_ERROR` 抛出 | Persistent L128-130 |
| onApply 反序列化/应用失败 | 捕获后返回失败 Response（不抛出） | Persistent L228-231 |

---

## 10. 完整数据流转时间线

以「gRPC 客户端注册一个临时实例」为例：

```
T0  客户端发起 InstanceRequest(REGISTER_INSTANCE)
      │
T1  【第01篇】GrpcRequestAcceptor → RequestHandlerRegistry → InstanceRequestHandler.handle()
      │
T2  InstanceRequestHandler 调 ephemeralClientOperationService.registerInstance(service, instance, clientId)
      │
T3  ── 校验段 ──
      │  NamingUtils.checkInstanceIsLegal(instance)          // 参数合法？
      │  singleton = ServiceManager.getSingleton(service)     // Service 单例化
      │  singleton.isEphemeral() == true ?                    // 临时校验
      │  client = clientManager.getClient(clientId)           // 取 Client
      │  checkClientIsLegal(client, clientId)                 // Client 合法？
      │
T4  ── 改模型段 ──
      │  instanceInfo = getPublishInfo(instance)              // Instance → InstancePublishInfo
      │  client.addServiceInstance(singleton, instanceInfo)   // 实例挂到 Client
      │  client.setLastUpdatedTime()                          // 时间戳
      │  client.recalculateRevision()                         // 重算版本号
      │
T5  ── 发事件段 ──
      │  publishEvent(ClientRegisterServiceEvent)  ──┐
      │  publishEvent(InstanceMetadataEvent)       ──┤ 异步分发
      │                                              │
T6  ── 下游异步响应（后续篇章）──                    ▼
      │  倒排索引更新（第04篇）  推送订阅者（第05篇）  元数据管理
      │
T7  registerInstance 返回 → InstanceRequestHandler 返回 InstanceResponse → 客户端收到成功响应
```

**关键点**：T5 发完事件后 `registerInstance` 立即返回，**不等待** T6 的下游处理。客户端拿到「注册成功」响应时，倒排索引与推送可能还在异步进行中——这是 AP 模型下的最终一致性。

---

## 11. 总结

1. **定位**：`ClientOperationService` 是「一次客户端写操作」的业务语义中枢，向上屏蔽协议差异、向下屏蔽一致性差异。
2. **双实现**：临时（`Ephemeral*`，AP 直连）与持久（`Persistent*`，CP Raft）通过多态隔离，`subscribe` 只在临时侧有效。
3. **统一范式**：所有落地动作收敛为 **校验 → 改 Client 模型（挂实例 + 重算 revision）→ 发领域事件**。
4. **AP/CP 分野**：临时当场改模型；持久先提交 Raft，`onApply` 共识后再复用同一范式落地。
5. **事件驱动**：通过 `NotifyCenter` 发出 `ClientOperationEvent` 家族 + `InstanceMetadataEvent`，与倒排索引、推送、元数据下游彻底解耦。
6. **模型转换**：`getPublishInfo` 作为接口 default 方法复用，用 `EPSILON` 处理权重浮点比较，「非默认才存」节流。

下一篇（第 03 篇）将深入 **数据模型**：`ServiceManager` 的享元单例、`Client` 抽象及其实现，解释「实例为什么挂在 Client 上」以及 `addServiceInstance` / `recalculateRevision` 的内部细节。

---

## 12. 文件索引表

| 文件 | 关键内容 | 关键行号 |
|------|----------|----------|
| [ClientOperationService.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/ClientOperationService.java) | 操作服务接口、default 空订阅、getPublishInfo | 接口 L46-87；EPSILON L89；getPublishInfo L97-118 |
| [EphemeralClientOperationServiceImpl.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/impl/EphemeralClientOperationServiceImpl.java) | 临时实例 AP 直连实现 | registerInstance L55-78；batchRegister L80-106；deregister L108-127；subscribe L129-139；unsubscribe L141-151；checkClientIsLegal L153-168 |
| [PersistentClientOperationServiceImpl.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/service/impl/PersistentClientOperationServiceImpl.java) | 持久实例 CP Raft 实现 | 类声明 L84-104；registerInstance L106-131；deregister L166-183；不支持订阅 L185-193；onApply L200-235；onInstanceRegister L243-257；onInstanceDeregister L259-278 |
| [ClientOperationEvent.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/event/client/ClientOperationEvent.java) | 客户端操作事件家族 | 基类 L30-49；Register L54；Deregister L66；Subscribe L78；Unsubscribe L90；FuzzyWatch L102；Release L143 |

---

> 系列导航：
> - [第 01 篇：gRPC 请求处理链路](./Naming服务端-01-gRPC请求处理链路.md)
> - **第 02 篇：客户端操作服务（本篇）**
> - 第 03 篇：数据模型（ServiceManager / Client 抽象）*（待续）*
