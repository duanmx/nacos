# Naming 服务端源码解析（07）：健康检查机制

> **原则声明**：本文只讲 Nacos 2.x/3.x 服务端 `naming` 模块的**健康检查**——即"实例活着还是死了、谁来判定、判定后怎么摘除"。所有结论都落到具体类、具体方法、具体行号（`file:///` 可点击跳转），不臆测、不泛谈。示例代码保持源码原貌，仅裁剪无关分支。

本篇是系列第 7 篇（收官篇）。前 6 篇讲了请求怎么进来（01）、怎么改模型（02）、模型长什么样（03）、变更怎么建索引（04）、怎么推给订阅者（05）、怎么在集群间同步（06）。本篇回答最后一个问题：**注册进来的实例，服务端怎么持续确认它还活着？**

---

## 目录

1. [为什么需要两套健康检查](#1-为什么需要两套健康检查)
2. [整体架构分层图](#2-整体架构分层图)
3. [调度入口：init 与 HealthCheckReactor](#3-调度入口init-与-healthcheckreactor)
4. [临时实例：客户端心跳超时检测](#4-临时实例客户端心跳超时检测)
5. [临时实例：拦截器链与两个 Checker](#5-临时实例拦截器链与两个-checker)
6. [持久实例：服务端主动探测](#6-持久实例服务端主动探测)
7. [探测结果收敛：HealthCheckCommonV2](#7-探测结果收敛healthcheckcommonv2)
8. [关键设计决策](#8-关键设计决策)
9. [线程安全与异常边界](#9-线程安全与异常边界)
10. [完整数据流转时间线](#10-完整数据流转时间线)
11. [总结](#11-总结)
12. [文件索引表](#12-文件索引表)

---

## 1. 为什么需要两套健康检查

Nacos 的实例分两类（见第 03 篇），健康判定方向**恰好相反**：

| 维度 | 临时实例（ephemeral） | 持久实例（persistent） |
|------|----------------------|------------------------|
| 载体 | `ConnectionBasedClient` / `IpPortBasedClient` | `IpPortBasedClient` |
| 判活方向 | **客户端主动上报**心跳，服务端**被动**等 | 服务端**主动**探测（TCP/HTTP/MySQL） |
| 超时后果 | 先标记不健康，再超时删除 | 只标记健康/不健康，**从不删除** |
| 判定任务 | `ClientBeatCheckTaskV2` | `HealthCheckTaskV2` |
| 一致性 | Distro（AP，第 06 篇） | Raft（CP） |

核心原因：临时实例代表"一个活着的进程"，进程没了就该消失，所以要能删；持久实例代表"一个被运维登记的节点"，节点临时挂了不代表要注销，所以只翻健康标志，等它恢复。

> 注意：gRPC 长连接的临时实例（`ConnectionBasedClient`）其实靠**连接断开事件**即时清理（第 06 篇 `ClientDisconnectEvent`），`ClientBeatCheckTaskV2` 主要服务于 `IpPortBasedClient`（兼容 1.x HTTP 心跳 / OpenAPI 注册的实例）。两种任务都在 `IpPortBasedClient.init()` 里按 `ephemeral` 二选一启动。

---

## 2. 整体架构分层图

```
                         IpPortBasedClient.init()  (第03篇)
                                    │  按 ephemeral 二选一
                 ┌──────────────────┴───────────────────┐
                 ▼ ephemeral=true                        ▼ ephemeral=false
      ClientBeatCheckTaskV2                     HealthCheckTaskV2
      （等客户端上报心跳）                        （服务端主动去探）
                 │                                        │
      HealthCheckReactor.scheduleCheck            HealthCheckReactor.scheduleCheck
      （固定 5s 周期 futureMap）                   （动态 checkRtNormalized 自调度）
                 │                                        │
                 ▼                                        ▼
      doHealthCheck: 遍历 published service        doHealthCheck: 遍历 published service
                 │                                        │
      InstanceBeatCheckTask                       HealthCheckProcessorV2Delegate
      （interceptorChain 前置过滤）                 （按 clusterMetadata.type 选 processor）
                 │                                        │
     ┌───────────┴───────────┐          ┌────────────────┼────────────────┐
     ▼                       ▼          ▼                ▼                ▼
UnhealthyInstanceChecker  ExpiredInstanceChecker   Tcp...   Http...   Mysql...Processor
（超时→标记 unhealthy）    （超时→删除实例）              │        │           │
     │                       │                     └────────┴─── HealthCheckCommonV2 ──┘
     ▼                       ▼                              checkOk / checkFail
ServiceChangedEvent      ClientDeregisterServiceEvent               │
ClientChangedEvent       InstanceMetadataEvent           PersistentHealthStatusSynchronizer
     │                   DeregisterInstanceTraceEvent     （CP：updateInstance）
     ▼                       ▼                                       │
  推送(05)/同步(06)       索引(04)/推送(05)/同步(06)              Raft 提交 → onApply
```

两条链路**入口相同**（`init` → `HealthCheckReactor`）、**出口相同**（发领域事件 → 驱动索引/推送/同步），中间处理逻辑完全不同。

---

## 3. 调度入口：init 与 HealthCheckReactor

### 3.1 二选一启动

任务在客户端建立时启动，见 [IpPortBasedClient.init](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/impl/IpPortBasedClient.java#L135-L143)：

```java
public void init() {
    if (ephemeral) {
        beatCheckTask = new ClientBeatCheckTaskV2(this);
        HealthCheckReactor.scheduleCheck(beatCheckTask);
    } else {
        healthCheckTaskV2 = new HealthCheckTaskV2(this);
        HealthCheckReactor.scheduleCheck(healthCheckTaskV2);
    }
}
```

一个 `IpPortBasedClient` 只会有**一种**检查任务。注意粒度：任务挂在 **Client** 上（一个 ip:port），任务内部再遍历该 client 发布的所有 service（`getAllPublishedService`）。

### 3.2 两种调度策略

两个重载的 `scheduleCheck` 策略不同，见 [HealthCheckReactor](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/HealthCheckReactor.java#L44-L64)：

```java
// 持久实例：动态自调度，首次延迟 = checkRtNormalized（2~7s 抖动）
public static void scheduleCheck(HealthCheckTaskV2 task) {
    task.setStartTime(System.currentTimeMillis());
    Runnable wrapperTask = new HealthCheckTaskInterceptWrapper(task);
    GlobalExecutor.scheduleNamingHealth(wrapperTask, task.getCheckRtNormalized(),
        TimeUnit.MILLISECONDS);                                    // 一次性，跑完再自己排下一次
}

// 临时实例：固定 5s 周期
public static void scheduleCheck(BeatCheckTask task) {
    Runnable wrapperTask = task instanceof NacosHealthCheckTask
            ? new HealthCheckTaskInterceptWrapper((NacosHealthCheckTask) task) : task;
    futureMap.computeIfAbsent(task.taskKey(),
        k -> GlobalExecutor.scheduleNamingHealth(wrapperTask, 5000, 5000,
            TimeUnit.MILLISECONDS));                               // 固定频率周期任务
}
```

关键区别：
- **临时**：`scheduleAtFixedRate` 式固定 5s，用 `futureMap`（key=`taskKey()`）保证同一 client 只排一次，可 `cancelCheck` 取消（L71-82）。
- **持久**：一次性延迟任务，跑完在 `doHealthCheck` 的 `finally` 里**自己排下一次**（见 §6），延迟时间根据历史 RT 动态计算——探测慢的服务自动降频。

---

## 4. 临时实例：客户端心跳超时检测

见 [ClientBeatCheckTaskV2](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ClientBeatCheckTaskV2.java#L66-L78)：

```java
@Override
public void doHealthCheck() {
    try {
        Collection<Service> services = client.getAllPublishedService();
        for (Service each : services) {
            HealthCheckInstancePublishInfo instance = (HealthCheckInstancePublishInfo) client
                .getInstancePublishInfo(each);
            interceptorChain.doInterceptor(new InstanceBeatCheckTask(client, each, instance));
        }
    } catch (Exception e) {
        Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
    }
}
```

它自己**不判定**任何东西，只做三件事：
1. 拿到该 client 发布的所有 service；
2. 每个 service 取出实例（强转 `HealthCheckInstancePublishInfo`——第 03 篇 `IpPortBasedClient.addServiceInstance` 已包装好，带 `lastHeartBeatTime`）；
3. 包成 `InstanceBeatCheckTask` 丢进拦截器链。

`taskId = client.getResponsibleId()`（[L47](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ClientBeatCheckTaskV2.java#L45-L49)），责任判定用得到（§5.2）；`taskKey` 用 clientId + ephemeral 拼（L56-59），是 `futureMap` 去重的 key。

---

## 5. 临时实例：拦截器链与两个 Checker

### 5.1 先过滤，再检查

`InstanceBeatCheckTask` 走的是"拦截器链 + 检查器列表"两级结构，见 [InstanceBeatCheckTask](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceBeatCheckTask.java#L35-L61)：

```java
private static final List<InstanceBeatChecker> CHECKERS = new LinkedList<>();

static {
    CHECKERS.add(new UnhealthyInstanceChecker());   // 顺序重要：先标记
    CHECKERS.add(new ExpiredInstanceChecker());     // 再删除
    CHECKERS.addAll(NacosServiceLoader.load(InstanceBeatChecker.class));  // SPI 扩展
}

@Override
public void passIntercept() {          // 通过所有拦截器后才执行
    for (InstanceBeatChecker each : CHECKERS) {
        each.doCheck(client, service, instancePublishInfo);
    }
}
```

`passIntercept` 只有在拦截器链**全部放行**时才被调用。拦截器链是单例（[InstanceBeatCheckTaskInterceptorChain](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceBeatCheckTaskInterceptorChain.java#L26-L38)），加载所有 `AbstractBeatCheckInterceptor`，按 `order` 排序。两个内置拦截器：

- [InstanceEnableBeatCheckInterceptor](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceEnableBeatCheckInterceptor.java#L36-L57)（order `MIN+1`）：若实例/服务元数据显式关闭了 `ENABLE_CLIENT_BEAT`，就拦掉（不做心跳检查）。
- [InstanceBeatCheckResponsibleInterceptor](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceBeatCheckResponsibleInterceptor.java#L31-L39)（order `MIN+2`）：`!responsible` 就拦掉——**只有责任节点才检查**（配合第 06 篇 Distro 一致性 hash，避免每个节点都摘同一个实例）。

### 5.2 UnhealthyInstanceChecker：超时标记不健康

见 [UnhealthyInstanceChecker](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/UnhealthyInstanceChecker.java#L48-L92)：

```java
@Override
public void doCheck(Client client, Service service, HealthCheckInstancePublishInfo instance) {
    if (instance.isHealthy() && isUnhealthy(service, instance)) {   // 只处理"健康→不健康"
        changeHealthyStatus(client, service, instance);
    }
}

private boolean isUnhealthy(Service service, HealthCheckInstancePublishInfo instance) {
    long beatTimeout = getTimeout(service, instance);              // 默认 DEFAULT_HEART_BEAT_TIMEOUT(15s)
    return System.currentTimeMillis() - instance.getLastHeartBeatTime() > beatTimeout;
}
```

超时（默认 15 秒无心跳）就 `changeHealthyStatus`：

```java
private void changeHealthyStatus(Client client, Service service, HealthCheckInstancePublishInfo instance) {
    instance.setHealthy(false);                                    // ① 改内存标志
    Loggers.EVT_LOG.info("{POS} {IP-DISABLED} ...");
    NotifyCenter.publishEvent(
        new ServiceEvent.ServiceChangedEvent(service, Constants.ServiceChangedType.HEART_BEAT)); // ② 触发推送(05)
    NotifyCenter.publishEvent(new ClientEvent.ClientChangedEvent(client));                       // ③ 触发 Distro 同步(06)
    NotifyCenter.publishEvent(new HealthStateChangeTraceEvent(...));                             // ④ 可观测
}
```

三个事件对应三条下游链路：`ServiceChangedEvent` → 推送订阅者（第 05 篇）；`ClientChangedEvent` → Distro 同步到其他节点（第 06 篇）；`HealthStateChangeTraceEvent` → trace 埋点。**这里只翻标志、不删实例**——被标记 unhealthy 的实例仍在列表里，只是 `healthy=false`。

### 5.3 ExpiredInstanceChecker：再超时才删除

见 [ExpiredInstanceChecker](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ExpiredInstanceChecker.java#L50-L91)：

```java
@Override
public void doCheck(Client client, Service service, HealthCheckInstancePublishInfo instance) {
    boolean expireInstance = ApplicationUtils.getBean(GlobalConfig.class).isExpireInstance();
    if (expireInstance && isExpireInstance(service, instance)) {   // 开关 + 更久超时
        deleteIp(client, service, instance);
    }
}

private boolean isExpireInstance(Service service, HealthCheckInstancePublishInfo instance) {
    long deleteTimeout = getTimeout(service, instance);           // 默认 DEFAULT_IP_DELETE_TIMEOUT(30s)
    return System.currentTimeMillis() - instance.getLastHeartBeatTime() > deleteTimeout;
}

private void deleteIp(Client client, Service service, InstancePublishInfo instance) {
    Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", ...);
    client.removeServiceInstance(service);                        // ① 真删模型（第03篇）
    NotifyCenter.publishEvent(
        new ClientOperationEvent.ClientDeregisterServiceEvent(service, client.getClientId())); // ② 拆索引(04)
    NotifyCenter.publishEvent(
        new MetadataEvent.InstanceMetadataEvent(service, instance.getMetadataId(), true));     // ③ 清元数据
    NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(..., DeregisterInstanceReason.HEARTBEAT_EXPIRE, ...));
}
```

两级超时是核心设计：**15s 标记不健康（还在列表，标红）→ 30s 彻底删除（消失）**。删除受 `GlobalConfig.isExpireInstance()` 开关保护，且发的是 `ClientDeregisterServiceEvent`（与第 02 篇正常注销同一事件），复用第 04 篇拆索引、第 05 篇推送、第 06 篇同步的全套下游逻辑。

> 两个 Checker 共享同一个 `getTimeout` 取值优先级：**实例元数据 > 实例 extendDatum > 默认常量**（[UnhealthyInstanceChecker.getTimeout L59-66](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/UnhealthyInstanceChecker.java#L59-L74)），所以客户端可以通过元数据自定义心跳超时/删除超时。

---

## 6. 持久实例：服务端主动探测

持久实例没有客户端心跳，服务端要**主动连过去**。见 [HealthCheckTaskV2.doHealthCheck](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/HealthCheckTaskV2.java#L110-L152)：

```java
@Override
public void doHealthCheck() {
    try {
        initIfNecessary();
        for (Service each : client.getAllPublishedService()) {
            if (switchDomain.isHealthCheckEnabled(each.getGroupedServiceName())) {
                InstancePublishInfo instancePublishInfo = client.getInstancePublishInfo(each);
                ClusterMetadata metadata = getClusterMetadata(each, instancePublishInfo);
                ApplicationUtils.getBean(HealthCheckProcessorV2Delegate.class)
                    .process(this, each, metadata);                     // 委派给具体 processor
            }
        }
    } catch (Throwable e) {
        Loggers.SRV_LOG.error("[HEALTH-CHECK] error ...", e);
    } finally {
        if (!cancelled) {
            initCheckRt();
            HealthCheckReactor.scheduleCheck(this);                     // 关键：自己排下一次
        }
    }
}
```

两个要点：
1. **自调度**：`finally` 里只要没 `cancelled` 就重新 `scheduleCheck(this)`，形成"跑一次→排下一次"的自驱动循环；下次延迟 `checkRtNormalized` 在 [L86-99](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/HealthCheckTaskV2.java#L86-L99) 计算——基础 `LOWER_CHECK_RT=2000ms` 加随机抖动（错峰，避免所有任务同时触发）。
2. **选探测方式**：委派给 [HealthCheckProcessorV2Delegate.process](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/processor/HealthCheckProcessorV2Delegate.java#L58-L66)，按 `clusterMetadata.getHealthyCheckType()` 从 map 里挑 processor，挑不到用 `NoneHealthCheckProcessor` 兜底：

```java
public void process(HealthCheckTaskV2 task, Service service, ClusterMetadata metadata) {
    String type = metadata.getHealthyCheckType();
    HealthCheckProcessorV2 processor = healthCheckProcessorMap.get(type);
    if (processor == null) {
        processor = healthCheckProcessorMap.get(NoneHealthCheckProcessor.TYPE);
    }
    processor.process(task, service, metadata);
}
```

内置三种 processor：`TcpHealthCheckProcessor`（建 TCP 连接）、`HttpHealthCheckProcessor`（发 HTTP 请求看状态码）、`MysqlHealthCheckProcessor`（执行探测 SQL）。以 [TcpHealthCheckProcessor.process](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/processor/TcpHealthCheckProcessor.java#L96-L116) 为例，它用 NIO Selector 异步探测：

```java
public void process(HealthCheckTaskV2 task, Service service, ClusterMetadata metadata) {
    HealthCheckInstancePublishInfo instance = (...) task.getClient().getInstancePublishInfo(service);
    if (null == instance) {
        return;
    }
    if (!instance.tryStartCheck()) {                    // CAS 防重入：上次没跑完就跳过
        healthCheckCommon.reEvaluateCheckRt(task.getCheckRtNormalized() * 2, task, ...); // 退避
        return;
    }
    taskQueue.add(new Beat(task, service, metadata, instance));   // 丢队列，NIO 线程异步连
    MetricsMonitor.getTcpHealthCheckMonitor().incrementAndGet();
}
```

探测本身走单独的 NIO 循环线程（`run()` L139-161），连接成功回调 `checkOk`，失败回调 `checkFail`——都收敛到 `HealthCheckCommonV2`。

---

## 7. 探测结果收敛：HealthCheckCommonV2

所有 processor 的探测结果最终都调 `HealthCheckCommonV2` 的三个方法。核心是**连续 N 次才翻转**，避免抖动。见 [checkFail](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/processor/HealthCheckCommonV2.java#L144-L188)：

```java
public void checkFail(HealthCheckTaskV2 task, Service service, String msg) {
    HealthCheckInstancePublishInfo instance = (...) task.getClient().getInstancePublishInfo(service);
    if (instance == null) {
        return;
    }
    try {
        if (instance.isHealthy()) {                                     // 只处理"健康→不健康"
            if (instance.getFailCount().incrementAndGet() >= switchDomain.getCheckTimes()) { // 连续失败达阈值
                if (switchDomain.isHealthCheckEnabled(serviceName) && !task.isCancelled()
                        && distroMapper.responsible(task.getClient().getResponsibleId())) {  // 责任节点才改
                    healthStatusSynchronizer.instanceHealthStatusChange(false, task.getClient(), service, instance);
                    NotifyCenter.publishEvent(new HealthStateChangeTraceEvent(..., false, msg));
                }
            }
        }
    } finally {
        instance.resetOkCount();                                        // 一次失败清零成功计数
        instance.finishCheck();                                         // 释放 tryStartCheck 的门闩
    }
}
```

对称地 [checkOk](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/processor/HealthCheckCommonV2.java#L93-L135) 在连续成功 `>= checkTimes` 时把不健康翻回健康。三个方法的共同点：
- **`checkTimes` 抖动抑制**：连续 N 次同向结果才翻转，单次网络抖动不生效（`checkFailNow` L197 例外，用于确定性失败立即翻）。
- **责任节点约束**：`distroMapper.responsible(...)` 保证集群里只有责任节点改状态。
- **`finally` 兜底**：无论成败都 `finishCheck()` 释放 `tryStartCheck` 的门闩（对应 §6 的 CAS 防重入），否则任务会永久卡死。

状态落地走 [PersistentHealthStatusSynchronizer](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/PersistentHealthStatusSynchronizer.java#L42-L49)：

```java
@Override
public void instanceHealthStatusChange(boolean isHealthy, Client client, Service service, InstancePublishInfo instance) {
    Instance updateInstance = InstanceUtil.parseToApiInstance(service, instance);
    updateInstance.setHealthy(isHealthy);
    persistentClientOperationService.updateInstance(service, updateInstance, client.getClientId());
}
```

关键：持久实例的健康变更走 **CP 路径**（`persistentClientOperationService.updateInstance` → 提交 Raft → `onApply` 落地，见第 02 篇），保证集群内一致；而临时实例（§5）直接改内存 + Distro 异步扩散。这正是两类实例一致性模型的分野。

---

## 8. 关键设计决策

1. **判活方向按实例语义分流**：临时实例进程死了要消失 → 被动等心跳 + 两级超时删除；持久实例是登记节点 → 主动探测 + 只翻标志不删。语义决定机制。
2. **两级超时（15s 标记 / 30s 删除）**：给"网络抖动"和"真死"之间留缓冲——先标红让流量绕开，再确认删除，避免误删。
3. **责任节点约束贯穿两条链路**：临时侧用 `InstanceBeatCheckResponsibleInterceptor`，持久侧用 `distroMapper.responsible`，都保证集群内同一实例只被一个节点判定，避免重复摘除/状态打架。
4. **`checkTimes` 连续阈值**：主动探测对单次抖动不敏感，连续 N 次同向才翻转，牺牲一点实时性换稳定性。
5. **持久实例自调度 + RT 自适应**：`doHealthCheck` 在 `finally` 里自排下一次，延迟按历史 RT 动态计算，慢服务自动降频，快服务保持灵敏，避免固定频率的资源浪费或探测积压。
6. **入口/出口统一，中间可插拔**：两条链路都从 `init` 进、都以领域事件出，中间 processor（TCP/HTTP/MySQL）和 checker 都支持 SPI 扩展（`NacosServiceLoader.load`），符合开闭原则。
7. **CAS 防重入（`tryStartCheck`/`finishCheck`）**：主动探测是异步 NIO，用门闩保证同一实例上一次探测没结束就不发起新的，`finally` 里必然释放，防止任务泄漏。

---

## 9. 线程安全与异常边界

| 关注点 | 处理方式 | 位置 |
|--------|----------|------|
| 任务重复调度 | 临时侧 `futureMap.computeIfAbsent`（clientId 去重） | HealthCheckReactor L61-63 |
| 探测重入 | `tryStartCheck()` CAS + `finally finishCheck()` | TcpProcessor L104；CommonV2 L130/183/227 |
| checker 异常 | `ClientBeatCheckTaskV2.doHealthCheck` try-catch 兜底，单个 client 异常不影响其他 | ClientBeatCheckTaskV2 L75-77 |
| 探测异常 | `HealthCheckTaskV2.doHealthCheck` catch `Throwable`，且 `finally` 必排下一次 | HealthCheckTaskV2 L126-151 |
| 状态并发翻转 | `okCount`/`failCount` 为原子计数，`incrementAndGet` 判阈值 | CommonV2 L105/156 |
| 集群重复判定 | 责任节点约束（拦截器 / distroMapper） | §5.1 / §7 |
| 任务取消后仍执行 | `cancelled` 标志 + `isCancelled()` 双检 | HealthCheckTaskV2 L69/130/157 |

一个易错点：`HealthCheckTaskV2` 的自调度**必须**在 `finally` 里，否则 `doHealthCheck` 抛异常后任务链就断了、该 client 再也不被探测。源码用 `catch(Throwable)` + `finally` 双保险规避。

---

## 10. 完整数据流转时间线

**场景 A：临时实例心跳丢失（默认 15s/30s）**

```
T0    客户端最后一次心跳，instance.lastHeartBeatTime = T0，healthy=true
      （HealthCheckReactor 每 5s 触发一次 ClientBeatCheckTaskV2）
T5    doHealthCheck → InstanceBeatCheckTask → 拦截器放行 → UnhealthyInstanceChecker
      now-T0=5s < 15s → 不动作
T15   now-T0>15s → UnhealthyInstanceChecker.changeHealthyStatus
      ├─ instance.setHealthy(false)
      ├─ ServiceChangedEvent(HEART_BEAT) → 推送订阅者(05)：客户端收到"该实例不健康"
      └─ ClientChangedEvent           → Distro 同步(06)：其他节点同步标记
T30   now-T0>30s 且 isExpireInstance 开 → ExpiredInstanceChecker.deleteIp
      ├─ client.removeServiceInstance(service)（真删）
      ├─ ClientDeregisterServiceEvent → 拆倒排索引(04) + 推送(05) + Distro 同步(06)
      └─ DeregisterInstanceTraceEvent(HEARTBEAT_EXPIRE)
      → 实例彻底从服务列表消失
```

**场景 B：持久实例 TCP 探测失败（假设 checkTimes=3）**

```
T0    HealthCheckTaskV2.doHealthCheck → Delegate → TcpHealthCheckProcessor.process
      tryStartCheck() 成功 → 入 taskQueue → NIO 线程 connect
Tn    connect 失败 → PostProcessor → healthCheckCommon.checkFail
      instance.isHealthy()=true → failCount=1 < 3 → 仅记录，不翻转；finishCheck 释放门闩
      （finally 里 scheduleCheck 自排下一次，延迟 checkRtNormalized）
...   第 2 次失败 failCount=2 < 3 → 不翻转
Tn+k  第 3 次失败 failCount=3 >= 3 且 responsible → instanceHealthStatusChange(false)
      → PersistentHealthStatusSynchronizer.updateInstance
      → persistentClientOperationService.updateInstance（提交 Raft，CP）
      → onApply 落地：instance.healthy=false（集群一致）
      → HealthStateChangeTraceEvent 埋点
      （实例始终在列表里，只是 healthy=false；恢复后连续 3 次成功再翻回 true）
```

---

## 11. 总结

1. **两套机制，语义驱动**：临时实例被动等心跳（超时标记→再超时删除），持久实例主动探测（连续 N 次才翻标志、从不删）。
2. **入口统一**：都在 `IpPortBasedClient.init()` 按 `ephemeral` 二选一，交给 `HealthCheckReactor` 调度。
3. **调度策略不同**：临时固定 5s 周期（`futureMap` 去重），持久一次性自调度（`finally` 自排 + RT 自适应）。
4. **临时链路**：`ClientBeatCheckTaskV2` → 拦截器过滤（enable / responsible）→ `UnhealthyInstanceChecker`（15s 标记）+ `ExpiredInstanceChecker`（30s 删除）。
5. **持久链路**：`HealthCheckTaskV2` → `HealthCheckProcessorV2Delegate`（按类型选 TCP/HTTP/MySQL）→ `HealthCheckCommonV2.checkOk/checkFail`（`checkTimes` 阈值）→ `PersistentHealthStatusSynchronizer`（CP）。
6. **一致性分野**：临时改内存 + Distro 异步扩散（AP，第 06 篇），持久走 Raft 提交（CP，第 02 篇）。
7. **出口统一**：两条链路都发领域事件（`ServiceChangedEvent`/`ClientChangedEvent`/`ClientDeregisterServiceEvent`），复用第 04（索引）、05（推送）、06（同步）的全套下游。

至此，Naming 服务端 7 大主题全部串起：**请求进来（01）→ 改模型（02）→ 模型结构（03）→ 建索引（04）→ 推订阅者（05）→ 集群同步（06）→ 持续保活（07）**，构成一个完整闭环。

---

## 12. 文件索引表

| 文件 | 关键内容 | 关键行号 |
|------|----------|----------|
| [IpPortBasedClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/impl/IpPortBasedClient.java) | 健康检查任务的启动入口 | init 二选一 L135-143 |
| [HealthCheckReactor.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/HealthCheckReactor.java) | 两种调度策略 | scheduleCheck(持久) L44-49；scheduleCheck(临时) L56-64；cancelCheck L71-82；scheduleNow L90-92 |
| [ClientBeatCheckTaskV2.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ClientBeatCheckTaskV2.java) | 临时实例心跳检查任务 | taskId L47；taskKey L56-59；doHealthCheck L66-78 |
| [InstanceBeatCheckTask.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceBeatCheckTask.java) | checker 列表 + passIntercept | CHECKERS 静态注册 L35-47；passIntercept L56-61 |
| [InstanceBeatCheckTaskInterceptorChain.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceBeatCheckTaskInterceptorChain.java) | 拦截器链单例 | L26-38 |
| [InstanceEnableBeatCheckInterceptor.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceEnableBeatCheckInterceptor.java) | 关闭心跳则拦截 | intercept L36-52；order MIN+1 L55-57 |
| [InstanceBeatCheckResponsibleInterceptor.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/InstanceBeatCheckResponsibleInterceptor.java) | 非责任节点拦截 | intercept L31-34；order MIN+2 L37-39 |
| [UnhealthyInstanceChecker.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/UnhealthyInstanceChecker.java) | 超时标记不健康 | doCheck L48-52；isUnhealthy L54-57；getTimeout L59-74；changeHealthyStatus L76-92 |
| [ExpiredInstanceChecker.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/heartbeat/ExpiredInstanceChecker.java) | 再超时删除实例 | doCheck L50-55；isExpireInstance L57-60；deleteIp L79-91 |
| [HealthCheckTaskV2.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/HealthCheckTaskV2.java) | 持久实例主动探测任务 | initCheckRt L86-99；doHealthCheck L110-152；afterIntercept L159-169 |
| [HealthCheckProcessorV2Delegate.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/processor/HealthCheckProcessorV2Delegate.java) | 按类型选 processor | addProcessor L51-56；process L58-66 |
| [TcpHealthCheckProcessor.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/processor/TcpHealthCheckProcessor.java) | TCP NIO 探测示例 | TYPE L60；process L96-116；NIO run L139-161；PostProcessor L163+ |
| [HealthCheckCommonV2.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/processor/HealthCheckCommonV2.java) | 结果收敛 + checkTimes 阈值 | reEvaluateCheckRt L60-84；checkOk L93-135；checkFail L144-188；checkFailNow L197-232 |
| [PersistentHealthStatusSynchronizer.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/healthcheck/v2/PersistentHealthStatusSynchronizer.java) | 持久健康变更走 CP | instanceHealthStatusChange L42-49 |

---

> 系列导航：
> - [第 01 篇：gRPC 请求处理链路](./Naming服务端-01-gRPC请求处理链路.md)
> - [第 02 篇：客户端操作服务](./Naming服务端-02-客户端操作服务.md)
> - [第 03 篇：数据模型（ServiceManager / Client 抽象）](./Naming服务端-03-数据模型.md)
> - [第 04 篇：事件驱动倒排索引](./Naming服务端-04-事件驱动倒排索引.md)
> - [第 05 篇：服务变更推送机制](./Naming服务端-05-服务变更推送机制.md)
> - [第 06 篇：Distro AP 一致性协议](./Naming服务端-06-Distro一致性协议.md)
> - **第 07 篇：健康检查机制（本篇 · 收官）**
