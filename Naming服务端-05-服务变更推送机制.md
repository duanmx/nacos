# Naming 服务端 - 05 - 服务变更推送机制（延迟合并 + 异步执行）

> **原则声明**：本文所有结论均来自 Nacos 服务端源码，标注了精确的文件路径与行号，不含任何主观猜测。阅读时可对照源码逐行验证。
>
> 版本：`3.2.1-SNAPSHOT` ｜ 主分支：`develop`

---

## 目录

1. [为什么推送要「延迟合并」](#1-为什么推送要延迟合并)
2. [推送链路总览](#2-推送链路总览)
3. [订阅事件的入口：NamingSubscriberServiceV2Impl](#3-订阅事件的入口namingsubscriberservicev2impl)
4. [PushDelayTask：可合并的延迟任务](#4-pushdelaytask可合并的延迟任务)
5. [PushDelayTaskExecuteEngine：延迟合并引擎](#5-pushdelaytaskexecuteengine延迟合并引擎)
6. [PushExecuteTask：真正的推送执行](#6-pushexecutetask真正的推送执行)
7. [推送回调与失败重试](#7-推送回调与失败重试)
8. [推送配置 PushConfig](#8-推送配置-pushconfig)
9. [关键设计决策](#9-关键设计决策)
10. [线程安全与异常边界](#10-线程安全与异常边界)
11. [完整数据流转时间线](#11-完整数据流转时间线)
12. [总结](#12-总结)
13. [文件索引表](#13-文件索引表)

---

## 1. 为什么推送要「延迟合并」

第 04 篇里，倒排索引每次变更都会发 `ServiceChangedEvent`。设想一个场景：一个服务瞬间上线 100 个实例（批量发布、扩容），倒排索引会连发 100 次 `ServiceChangedEvent`。

如果每个事件都立刻给所有订阅者推一次全量列表：

- **推送风暴**：100 次事件 × N 个订阅者 = 100N 次推送，绝大多数是中间态、无意义。
- **带宽与 CPU 浪费**：每次都要序列化整个服务列表。

正确做法是 **延迟合并（debounce）**：收到变更后不立即推，而是**等一小段时间**（默认延迟），把这段时间内针对同一服务的多次变更**合并成一次**，最后只推一次「最终状态」。

这就是 Nacos 推送机制的核心思想，通过 **两级任务引擎** 实现：
1. **延迟合并引擎**（`PushDelayTaskExecuteEngine`）：按服务聚合、延迟触发。
2. **执行分发**（`NamingExecuteTaskDispatcher` → `PushExecuteTask`）：真正组装数据、异步推送。

---

## 2. 推送链路总览

```
第04篇：ClientServiceIndexesManager
   publishEvent(ServiceChangedEvent / ServiceSubscribedEvent)
        │
        ▼
┌──────────────────────────────────────────────┐
│  NamingSubscriberServiceV2Impl (SmartSubscriber)│  ← 订阅事件，转成延迟任务
│    onEvent() → delayTaskEngine.addTask(...)     │
└───────────────────┬──────────────────────────┘
                    ▼
┌──────────────────────────────────────────────┐
│  PushDelayTaskExecuteEngine                     │  ← 第一级：延迟 + 合并
│    (extends NacosDelayTaskExecuteEngine)        │
│    同一 service 的任务 merge，延迟到期后触发     │
│    PushDelayTaskProcessor.process()             │
└───────────────────┬──────────────────────────┘
                    ▼ dispatchAndExecuteTask
┌──────────────────────────────────────────────┐
│  PushExecuteTask (extends AbstractExecuteTask)  │  ← 第二级：异步执行
│    generatePushData() → 组装 ServiceInfo        │
│    getTargetClientIds() → 查订阅者               │
│    pushExecutor.doPushWithCallback(...)          │
└───────────────────┬──────────────────────────┘
                    ▼
             实际 gRPC 推送给订阅客户端
                    │
                    ▼ 回调
             onSuccess / onFail（失败重试）
```

---

## 3. 订阅事件的入口：NamingSubscriberServiceV2Impl

文件：[NamingSubscriberServiceV2Impl.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/NamingSubscriberServiceV2Impl.java)

它是 `SmartSubscriber`（L51），构造时创建延迟任务引擎并注册自己（L62-73）：

```java
public NamingSubscriberServiceV2Impl(...) {
    this.clientManager = clientManager;
    this.indexesManager = indexesManager;
    this.delayTaskEngine = new PushDelayTaskExecuteEngine(clientManager, indexesManager,
            serviceStorage, metadataManager, pushExecutor, switchDomain);  // 建延迟引擎
    NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
}
```

### 3.1 订阅两种事件

```java
@Override
public List<Class<? extends Event>> subscribeTypes() {
    List<Class<? extends Event>> result = new LinkedList<>();
    result.add(ServiceEvent.ServiceChangedEvent.class);      // 服务变更
    result.add(ServiceEvent.ServiceSubscribedEvent.class);   // 服务被订阅
    return result;
}
```

（L110-116）正是第 04 篇倒排索引发出的两种事件。

### 3.2 onEvent —— 两种推送策略

```java
@Override
public void onEvent(Event event) {
    if (event instanceof ServiceEvent.ServiceChangedEvent) {
        // 服务变了 → 推给所有订阅者
        Service service = ((ServiceChangedEvent) event).getService();
        delayTaskEngine.addTask(service, new PushDelayTask(service, PushConfig.getInstance().getPushTaskDelay()));
        MetricsMonitor.incrementServiceChangeCount(service);
    } else if (event instanceof ServiceEvent.ServiceSubscribedEvent) {
        // 某客户端刚订阅 → 只推这个客户端
        ServiceSubscribedEvent subscribedEvent = (ServiceSubscribedEvent) event;
        Service service = subscribedEvent.getService();
        delayTaskEngine.addTask(service, new PushDelayTask(service,
                PushConfig.getInstance().getPushTaskDelay(), subscribedEvent.getClientId()));
    }
}
```

（L118-137）两条路对应两种粒度：

| 事件 | 语义 | 推送对象 | 构造的任务 |
|------|------|----------|-----------|
| `ServiceChangedEvent` | 服务实例列表变了 | **全部订阅者** | `PushDelayTask(service, delay)`（pushToAll=true） |
| `ServiceSubscribedEvent` | 某客户端新订阅 | **仅该客户端** | `PushDelayTask(service, delay, clientId)`（pushToAll=false） |

新订阅只推它自己（它需要首份数据），而实例变更要广播给所有人——两种粒度都通过同一个 `addTask` 进入延迟引擎，靠 `PushDelayTask` 的两个构造函数区分。

注意 `addTask` 的 key 都是 `service`——**同一个服务的任务会被引擎按 key 合并**（下节）。

---

## 4. PushDelayTask：可合并的延迟任务

文件：[PushDelayTask.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/task/PushDelayTask.java)

```java
public class PushDelayTask extends AbstractDelayTask {
    private final Service service;
    private boolean pushToAll;              // 是否推给所有订阅者
    private Set<String> targetClients;      // 指定推送对象（pushToAll=false 时有效）
}
```

（L31-37）两个构造函数：

- **全量构造**（L39-45）：`pushToAll = true`，`targetClients = null`，用于 `ServiceChangedEvent`。
- **定向构造**（L47-54）：`pushToAll = false`，`targetClients = {targetClient}`，用于 `ServiceSubscribedEvent`。

都调 `setTaskInterval(delay)`（延迟时间）和 `setLastProcessTime(now)`。

### 4.1 merge —— 合并的核心

```java
@Override
public void merge(AbstractDelayTask task) {
    if (!(task instanceof PushDelayTask)) { return; }
    PushDelayTask oldTask = (PushDelayTask) task;
    if (isPushToAll() || oldTask.isPushToAll()) {
        pushToAll = true;               // 只要有一方是全量，合并结果就是全量
        targetClients = null;
    } else {
        targetClients.addAll(oldTask.getTargetClients());  // 都是定向 → 合并目标集合
    }
    setLastProcessTime(Math.min(getLastProcessTime(), task.getLastProcessTime())); // 取最早时间
    Loggers.PUSH.info("[PUSH] Task merge for {}", service);
}
```

（L56-70）合并规则很讲究：

1. **全量吸收定向**（L62-64）：只要新旧任务里有一个是「推全部」，合并后就是「推全部」——因为全量本就包含所有定向对象。
2. **定向叠加**（L65-67）：两个都是定向 → 把目标客户端集合并起来，一次推给这批人。
3. **时间取最早**（L68）：`lastProcessTime` 取两者较小值，保证「最早那次变更」的时间基准不丢，避免因不断合并而无限推迟执行（防饥饿）。

`AbstractDelayTask` 框架保证：在延迟窗口内对同一 key 反复 `addTask`，会不断调用 `merge` 把它们揉成一个任务，到期只执行一次。

---

## 5. PushDelayTaskExecuteEngine：延迟合并引擎

文件：[PushDelayTaskExecuteEngine.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/task/PushDelayTaskExecuteEngine.java)

```java
public class PushDelayTaskExecuteEngine extends NacosDelayTaskExecuteEngine {
    public PushDelayTaskExecuteEngine(...) {
        super(PushDelayTaskExecuteEngine.class.getSimpleName(), Loggers.PUSH);
        ...
        setDefaultTaskProcessor(new PushDelayTaskProcessor(this));  // 设置到期处理器
    }
}
```

（L37-63）继承通用的 `NacosDelayTaskExecuteEngine`（延迟合并框架），核心是设置了 `PushDelayTaskProcessor`。

### 5.1 processTasks —— 推送总开关

```java
@Override
protected void processTasks() {
    if (!switchDomain.isPushEnabled()) {   // 全局推送开关
        return;
    }
    super.processTasks();
}
```

（L85-91）覆写扫描逻辑，先检查全局开关 `isPushEnabled`——运维可一键关闭推送（应急降级）。

### 5.2 PushDelayTaskProcessor.process —— 移交第二级

```java
@Override
public boolean process(NacosTask task) {
    PushDelayTask pushDelayTask = (PushDelayTask) task;
    Service service = pushDelayTask.getService();
    NamingExecuteTaskDispatcher.getInstance()
            .dispatchAndExecuteTask(service, new PushExecuteTask(service, executeEngine, pushDelayTask));
    return true;
}
```

（L101-109）延迟到期后，处理器**不亲自推送**，而是把任务包装成 `PushExecuteTask`，交给 `NamingExecuteTaskDispatcher` 异步分发执行。

**为什么再转一手？** 延迟引擎是单线程扫描（保证合并语义），推送本身是耗时 IO。若在扫描线程里直接推送会阻塞后续任务的合并调度。所以第一级只管「何时该推」，第二级（分发器 + 线程池）管「实际推送」，**调度与执行分离**。分发器还能按 service 做 hash，让同一服务的推送落到固定线程，保证顺序。

---

## 6. PushExecuteTask：真正的推送执行

文件：[PushExecuteTask.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/task/PushExecuteTask.java)

`PushExecuteTask extends AbstractExecuteTask`（L42），`run()` 是推送主体（L57-82）：

```java
@Override
public void run() {
    try {
        PushDataWrapper wrapper = generatePushData();               // ① 组装推送数据（一次）
        ClientManager clientManager = delayTaskEngine.getClientManager();
        for (String each : getTargetClientIds()) {                  // ② 确定推送对象
            Client client = clientManager.getClient(each);
            if (null == client) { continue; }                       // 客户端已断开，跳过
            Subscriber subscriber = client.getSubscriber(service);
            if (subscriber == null) { continue; }                   // 未订阅，跳过
            delayTaskEngine.getPushExecutor().doPushWithCallback(each, subscriber, wrapper,
                    new ServicePushCallback(each, subscriber, wrapper.getOriginalData(),
                            delayTask.isPushToAll()));              // ③ 异步推送 + 回调
        }
    } catch (Exception e) {
        Loggers.PUSH.error("Push task ... execute failed ", e);
        delayTaskEngine.addTask(service, new PushDelayTask(service, 1000L)); // ④ 整体失败，1秒后重投
    }
}
```

### 6.1 generatePushData —— 数据只组装一次

```java
private PushDataWrapper generatePushData() {
    ServiceInfo serviceInfo = delayTaskEngine.getServiceStorage().getPushData(service); // 服务实例列表
    ServiceMetadata serviceMetadata = delayTaskEngine.getMetadataManager()
            .getServiceMetadata(service).orElse(null);                                  // 服务元数据
    return new PushDataWrapper(serviceMetadata, serviceInfo);
}
```

（L84-89）**关键优化**：一次变更对应一份数据，组装一次后复用给所有订阅者，而不是每人组装一遍。`getPushData` 从 `ServiceStorage` 拿聚合后的实例列表（跨所有提供者 Client 聚合而来）。

### 6.2 getTargetClientIds —— 全量 vs 定向

```java
private Collection<String> getTargetClientIds() {
    return delayTask.isPushToAll()
            ? delayTaskEngine.getIndexesManager().getAllClientsSubscribeService(service) // 查倒排索引拿所有订阅者
            : delayTask.getTargetClients();                                              // 用任务里指定的目标
}
```

（L91-95）这里正是第 04 篇倒排索引 `getAllClientsSubscribeService` 的调用方——**倒排索引的价值在此兑现**：O(1) 拿到某服务的所有订阅者。

---

## 7. 推送回调与失败重试

内部类 `ServicePushCallback implements NamingPushCallback`（L97）处理异步推送结果。

### 7.1 onSuccess —— 记录三个耗时指标

```java
@Override
public void onSuccess() {
    long pushFinishTime = System.currentTimeMillis();
    long pushCostTimeForNetWork = pushFinishTime - executeStartTime;              // 网络耗时
    long pushCostTimeForAll = pushFinishTime - delayTask.getLastProcessTime();    // 含延迟合并的总耗时
    long serviceLevelAgreementTime = pushFinishTime - service.getLastUpdatedTime(); // SLA：从数据变到推达
    ...
    PushResult result = PushResult.pushSuccess(...);
    NotifyCenter.publishEvent(getPushServiceTraceEvent(pushFinishTime, result));  // 发追踪事件
    PushResultHookHolder.getInstance().pushSuccess(result);                       // 触发 hook
}
```

（L133-160）三个耗时指标层层递进，尤其 **SLA**（`pushFinishTime - service.getLastUpdatedTime()`）衡量「从实例真正变化到订阅者收到」的端到端延迟，是推送质量的核心指标。

### 7.2 onFail —— 选择性重试

```java
@Override
public void onFail(Throwable e) {
    long pushCostTime = System.currentTimeMillis() - executeStartTime;
    Loggers.PUSH.error("[PUSH-FAIL] ...", ...);
    if (!(e instanceof NoRequiredRetryException)) {                 // 关键：并非所有失败都重试
        delayTaskEngine.addTask(service, new PushDelayTask(service,
                PushConfig.getInstance().getPushTaskRetryDelay(), clientId)); // 定向重试该 client
    }
    PushResult result = PushResult.pushFailed(...);
    PushResultHookHolder.getInstance().pushFailed(result);
}
```

（L162-178）**重试的精细控制**：

1. **不是所有失败都重试**：若是 `NoRequiredRetryException`（如客户端已不需要该数据），就不重试，避免无意义投递。
2. **定向重试**：重投的任务只针对失败的那个 `clientId`（`new PushDelayTask(service, retryDelay, clientId)`），而非重推所有人——避免因一个订阅者失败而骚扰全体。
3. **重试也走延迟引擎**：重投的任务同样进 `addTask`，会与期间的新变更再次合并。

对比 `run()` 里的整体异常兜底（L80，`1000L` 延迟重投全量）：那是「组装数据阶段就崩了」的粗粒度兜底；而 `onFail` 是「单个客户端推送失败」的细粒度重试。

---

## 8. 推送配置 PushConfig

文件：[PushConfig.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/PushConfig.java)

```java
public class PushConfig extends AbstractDynamicConfig {
    private long pushTaskDelay = PushConstants.DEFAULT_PUSH_TASK_DELAY;         // 延迟合并窗口
    private long pushTaskTimeout = PushConstants.DEFAULT_PUSH_TASK_TIMEOUT;     // 单次推送超时
    private long pushTaskRetryDelay = PushConstants.DEFAULT_PUSH_TASK_RETRY_DELAY; // 失败重试延迟
}
```

（L28-43）三个可调参数：

| 参数 | 用途 | 使用点 |
|------|------|--------|
| `pushTaskDelay` | 延迟合并窗口 | `onEvent` 建任务时的 delay（第 3 节） |
| `pushTaskTimeout` | 单次推送超时 | `ServicePushCallback.getTimeout()`（L129-131） |
| `pushTaskRetryDelay` | 失败重试延迟 | `onFail` 重投任务（第 7 节） |

它继承 `AbstractDynamicConfig`，`getConfigFromEnv`（L45-55）从环境变量读取，支持**动态刷新**——运维可在线调整延迟窗口而无需重启。

---

## 9. 关键设计决策

### 9.1 为什么用两级任务引擎？

- **第一级（延迟合并）**：解决「短时多次变更 → 合并成一次」，是**削峰**。单线程扫描保证合并语义正确。
- **第二级（分发执行）**：解决「推送是耗时 IO，不能阻塞调度」，是**并发**。按 service hash 分发保证同服务推送有序。

调度与执行分离，各司其职。

### 9.2 为什么数据只组装一次？

一次服务变更，所有订阅者收到的实例列表是相同的。在 `run()` 开头 `generatePushData()` 一次，循环里复用，避免 N 个订阅者重复 N 次序列化开销。

### 9.3 为什么区分全量推送与定向推送？

- 实例变更影响所有订阅者 → 全量。
- 新订阅只影响它自己 → 定向。

`merge` 里「全量吸收定向」保证语义正确：若窗口内既有变更又有新订阅，合并为全量即可覆盖所有人。

### 9.4 为什么失败要区分是否重试？

盲目重试会放大故障（客户端已断开还反复推）。用 `NoRequiredRetryException` 作为「不必重试」的显式信号，只对真正的临时性失败做定向重试，兼顾可靠性与资源保护。

---

## 10. 线程安全与异常边界

| 环节 | 机制 | 说明 |
|------|------|------|
| 任务合并 | `NacosDelayTaskExecuteEngine` 单线程扫描 + `merge` | 同 key 任务串行合并，无竞态 |
| 推送执行 | `NamingExecuteTaskDispatcher` 按 service 分发到固定线程 | 同服务推送有序 |
| 客户端失效 | `run` 里 `client == null` / `subscriber == null` 跳过 | 推送时客户端可能已断开 |
| 组装阶段异常 | `try-catch` 兜底，1 秒后重投全量 | 粗粒度恢复 |
| 单客户端推送失败 | `onFail` 选择性定向重试 | 细粒度恢复 |
| 全局降级 | `processTasks` 检查 `isPushEnabled` | 可一键停推 |

---

## 11. 完整数据流转时间线

以「order-service 在 200ms 内连续新增 3 个实例」为例（假设延迟窗口 500ms）：

```
T0    实例1注册 → 第04篇 publishEvent(ServiceChangedEvent(order))
T0+1  onEvent → delayTaskEngine.addTask(order, PushDelayTask(pushToAll)) 【新建，计划 T0+500 执行】
      │
T0+100 实例2注册 → ServiceChangedEvent(order)
T0+101 addTask(order, ...) → 引擎发现已有 order 任务 → merge（仍全量，lastProcessTime 取最早）
      │
T0+200 实例3注册 → ServiceChangedEvent(order)
T0+201 addTask → 再次 merge
      │
T0+500 延迟到期 → processTasks（isPushEnabled? 是）
      │  PushDelayTaskProcessor.process → dispatchAndExecuteTask(PushExecuteTask)
      ▼
T0+501 PushExecuteTask.run()（线程池中）
      │  generatePushData() → 组装 order 最终实例列表（含全部3个实例，只组装一次）
      │  getTargetClientIds() → 查倒排索引 getAllClientsSubscribeService(order) → {clientA, clientB}
      │  对每个订阅者 doPushWithCallback(...)
      ▼
T0+X  clientA 推送成功 → onSuccess（记录 SLA = 推达时间 - 数据变更时间）
      clientB 推送失败(非NoRequiredRetry) → onFail → addTask(order, retryDelay, clientB) 定向重投
```

**效果**：3 次变更只推 1 次「最终态」，而非 3 次中间态。这就是延迟合并的价值。

---

## 12. 总结

1. **核心思想**：延迟合并（debounce）——短时多次变更聚合成一次推送，消除中间态推送风暴。
2. **两级引擎**：`PushDelayTaskExecuteEngine`（延迟+合并，单线程调度）+ `NamingExecuteTaskDispatcher`/`PushExecuteTask`（异步执行，按服务分发），调度与执行分离。
3. **入口**：`NamingSubscriberServiceV2Impl` 订阅第 04 篇的 `ServiceChangedEvent`（全量推）与 `ServiceSubscribedEvent`（定向推），统一转成 `PushDelayTask`。
4. **合并规则**：全量吸收定向、定向叠加、时间取最早（防饥饿）。
5. **数据复用**：`generatePushData` 一次组装，所有订阅者复用；订阅者列表来自第 04 篇倒排索引。
6. **失败重试**：`onFail` 用 `NoRequiredRetryException` 区分是否重试，只对失败客户端定向重投。
7. **可运维**：`PushConfig` 动态配置延迟/超时/重试延迟；`isPushEnabled` 全局开关。

下一篇（第 06 篇）将讲 **Distro AP 一致性协议**：临时实例数据如何在集群节点间同步——第 03 篇 `addServiceInstance` 发出的 `ClientChangedEvent` 如何驱动 Distro 把本节点的 Client 数据异步复制到其他节点。

---

## 13. 文件索引表

| 文件 | 关键内容 | 关键行号 |
|------|----------|----------|
| [NamingSubscriberServiceV2Impl.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/NamingSubscriberServiceV2Impl.java) | 订阅入口、两种推送策略 | 构造 L62-73；getSubscribers L83-90；subscribeTypes L110-116；onEvent L118-137 |
| [PushDelayTask.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/task/PushDelayTask.java) | 可合并延迟任务、merge 规则 | 全量构造 L39-45；定向构造 L47-54；merge L56-70 |
| [PushDelayTaskExecuteEngine.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/task/PushDelayTaskExecuteEngine.java) | 延迟合并引擎、推送开关、移交第二级 | 构造+设处理器 L51-63；processTasks L85-91；process L101-109 |
| [PushExecuteTask.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/task/PushExecuteTask.java) | 推送执行、数据组装、回调、重试 | run L57-82；generatePushData L84-89；getTargetClientIds L91-95；onSuccess L133-160；onFail L162-178 |
| [PushConfig.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/push/v2/PushConfig.java) | 动态推送配置 | 三参数 L34-38；getConfigFromEnv L45-55 |

关联文件（前序篇章）：
- 事件来源：[ClientServiceIndexesManager.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/index/ClientServiceIndexesManager.java)（第 04 篇）

---

> 系列导航：
> - [第 01 篇：gRPC 请求处理链路](./Naming服务端-01-gRPC请求处理链路.md)
> - [第 02 篇：客户端操作服务](./Naming服务端-02-客户端操作服务.md)
> - [第 03 篇：数据模型（ServiceManager / Client 抽象）](./Naming服务端-03-数据模型.md)
> - [第 04 篇：事件驱动倒排索引](./Naming服务端-04-事件驱动倒排索引.md)
> - **第 05 篇：服务变更推送机制（本篇）**
> - 第 06 篇：Distro AP 一致性协议*（待续）*
