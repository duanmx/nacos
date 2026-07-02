# 客户端故障转移（Failover）机制

> **原则声明**：本文档所有结论均来源于 Nacos 3.2.1-SNAPSHOT 源代码的实际引用，不含任何猜测性描述。每一段分析都附有源码文件路径和行号。

---

## 目录

1. [为什么需要故障转移](#1-为什么需要故障转移)
2. [Naming Failover：FailoverReactor 架构](#2-naming-failoverfailoverreactor-架构)
3. [Naming Failover：DiskFailoverDataSource 开关检测](#3-naming-failoverdiskfailoverdatasource-开关检测)
4. [Naming Failover：完整数据流转](#4-naming-failover完整数据流转)
5. [Config Failover：三级降级链路](#5-config-failover三级降级链路)
6. [Config Failover：checkLocalConfig 运行时检测](#6-config-failoverchecklocalconfig-运行时检测)
7. [Naming 与 Config Failover 对比](#7-naming-与-config-failover-对比)
8. [关键设计决策](#8-关键设计决策)
9. [总结](#9-总结)

---

## 1. 为什么需要故障转移

Nacos 客户端的故障转移机制服务于一个核心目标：**在服务端完全不可达时，客户端仍能基于本地缓存继续提供服务发现和配置读取能力**。

两种场景触发：

| 场景 | 触发方式 | 适用模块 |
|------|---------|----------|
| **运维主动切换** | 运维人员向磁盘文件写入开关和应急数据 | Naming + Config |
| **自动降级** | 服务端不可达时自动回退到本地 snapshot | 仅 Config |

**Naming 不支持自动降级**：因为服务实例列表是动态变化的（实例上下线），使用过期的磁盘缓存可能导致流量打到已下线的实例，比"暂时返回空列表"更危险。

**Config 支持自动降级**：配置内容相对稳定，本地 snapshot 通常是上一次成功获取的最新值，使用过期数据风险较低。

---

## 2. Naming Failover：FailoverReactor 架构

### 2.1 组件关系

> 源码：[FailoverReactor.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverReactor.java)

```
ServiceInfoHolder 构造
  │
  └─ new FailoverReactor(this, notifierEventScope)         [L145-161]
       ├─ SPI 加载 FailoverDataSource → DiskFailoverDataSource  [L150-156]
       └─ init() → scheduleWithFixedDelay(5s)                   [L169-172]
            └─ FailoverSwitchRefresher.run()
                 ├─ dataSource.getSwitch()         ← 检查开关文件
                 ├─ 开启 → getFailoverData()       ← 加载磁盘数据
                 │         → diff → publishEvent()  ← 通知用户
                 └─ 关闭 → diff 正常数据 → publishEvent() → clear()
```

### 2.2 构造与 SPI 加载

> 源码：[FailoverReactor.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverReactor.java) L145-161

```java
public FailoverReactor(ServiceInfoHolder serviceInfoHolder, String notifierEventScope) {
    this.serviceInfoHolder = serviceInfoHolder;
    this.notifierEventScope = notifierEventScope;
    this.instancesDiffer = new InstancesDiffer();
    // SPI 加载 FailoverDataSource，默认为 DiskFailoverDataSource
    Collection<FailoverDataSource> dataSources =
        NacosServiceLoader.load(FailoverDataSource.class);
    for (FailoverDataSource dataSource : dataSources) {
        failoverDataSource = dataSource;
        break;  // 只取第一个 SPI 实现
    }
    this.executorService = new ScheduledThreadPoolExecutor(1,
        new NameThreadFactory("com.alibaba.nacos.naming.failover"));
    this.init();  // 立即启动 5s 轮询
}
```

**关键设计：SPI 可替换数据源**

> 源码：[FailoverDataSource.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverDataSource.java) L26-42

```java
public interface FailoverDataSource {
    FailoverSwitch getSwitch();                  // 读取开关状态
    Map<String, FailoverData> getFailoverData();  // 读取故障转移数据
}
```

**为什么设计为 SPI 接口？** 默认实现的 `DiskFailoverDataSource` 从磁盘读取，但运维平台可能有自己的灾备数据源（如从配置中心下发、从对象存储拉取）。通过 SPI 机制，运维平台可以实现自己的 `FailoverDataSource`，无需修改客户端代码。

### 2.3 定时轮询机制

> 源码：[FailoverReactor.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverReactor.java) L169-172

```java
public void init() {
    executorService.scheduleWithFixedDelay(new FailoverSwitchRefresher(), 0L, 5000L,
        TimeUnit.MILLISECONDS);
}
```

**为什么用 `scheduleWithFixedDelay` 而不是 `scheduleAtFixedRate`？**

`scheduleWithFixedDelay` 确保上一次任务执行完成后才开始计时，避免磁盘 IO 慢时多个任务并发执行导致文件读取竞争。

### 2.4 FailoverSwitchRefresher：核心状态机

> 源码：[FailoverReactor.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverReactor.java) L184-268

```java
class FailoverSwitchRefresher implements Runnable {
    @Override
    public void run() {
        // 1. 从数据源读取当前开关状态
        FailoverSwitch fSwitch = failoverDataSource.getSwitch();

        // ============ 分支 A：开关开启 ============
        if (fSwitch.getEnabled()) {
            Map<String, ServiceInfo> failoverMap = new ConcurrentHashMap<>(200);
            Map<String, FailoverData> failoverData = failoverDataSource.getFailoverData();
            for (Map.Entry<String, FailoverData> entry : failoverData.entrySet()) {
                ServiceInfo newService = (ServiceInfo) entry.getValue().getData();
                ServiceInfo oldService = serviceMap.get(entry.getKey());
                InstancesDiff diff = instancesDiffer.doDiff(oldService, newService);
                // 只有数据变化时才通知用户（避免无变化时每 5s 骚扰用户）
                if (diff.hasDifferent()) {
                    NotifyCenter.publishEvent(
                        new InstancesChangeEvent(notifierEventScope, ...));
                }
                failoverMap.put(entry.getKey(), (ServiceInfo) entry.getValue().getData());
            }
            // 整批替换（原子性，避免并发读到半更新状态）
            if (!failoverMap.isEmpty()) {
                serviceMap = failoverMap;
            }
            failoverSwitchEnable = true;
            return;
        }

        // ============ 分支 B：开关从开启→关闭 ============
        // 仅在之前是开启状态、现在变为关闭时才执行
        if (failoverSwitchEnable && !fSwitch.getEnabled()) {
            // 取正常缓存数据，与 failover 数据做 diff
            // 目的：让用户感知"failover 数据 → 正常数据"的变化
            Map<String, ServiceInfo> serviceInfoMap = serviceInfoHolder.getServiceInfoMap();
            for (Map.Entry<String, ServiceInfo> entry : serviceMap.entrySet()) {
                ServiceInfo oldService = entry.getValue();
                ServiceInfo newService = serviceInfoMap.get(entry.getKey());
                if (newService != null) {
                    InstancesDiff diff = instancesDiffer.doDiff(oldService, newService);
                    if (diff.hasDifferent()) {
                        NotifyCenter.publishEvent(
                            new InstancesChangeEvent(notifierEventScope, ...));
                    }
                }
            }
            serviceMap.clear();
            failoverSwitchEnable = false;
        }
    }
}
```

**关键设计：开关关闭时做 diff 通知**

当 failover 从开启变为关闭时，同一个服务在 failover 数据中的实例列表和正常内存缓存中的实例列表可能不同（因为 failover 数据是运维人员手动准备的，可能与真实服务端数据不一致）。客户端通过 `InstancesDiffer.doDiff()` 计算差异并发布事件，让用户感知到"从应急数据恢复到正常数据"的变化。

---

## 3. Naming Failover：DiskFailoverDataSource 开关检测

### 3.1 磁盘目录结构

```
{cacheDir}/failover/
├── 00-00---000-VIPSRV_FAILOVER_SWITCH-000---00-00   ← 开关文件，内容 "1"=开启 "0"=关闭
├── {encodedServiceKey1}                               ← 服务1的 failover 缓存文件
└── {encodedServiceKey2}                               ← 服务2的 failover 缓存文件
```

> 开关文件名常量：[UtilAndComs.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/utils/UtilAndComs.java) L38

```java
public static final String FAILOVER_SWITCH =
    "00-00---000-VIPSRV_FAILOVER_SWITCH-000---00-00";
```

### 3.2 getSwitch()：基于 lastModified 的优化读取

> 源码：[DiskFailoverDataSource.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/backups/datasource/DiskFailoverDataSource.java) L111-153

```java
public FailoverSwitch getSwitch() {
    File switchFile = Paths.get(failoverDir, UtilAndComs.FAILOVER_SWITCH).toFile();
    if (!switchFile.exists()) {
        return FAILOVER_SWITCH_FALSE;
    }

    long modified = switchFile.lastModified();
    // 关键优化：只有文件被修改过才重新读取内容
    if (lastModifiedMillis < modified) {
        lastModifiedMillis = modified;
        String failover = ConcurrentDiskUtil.getFileContent(switchFile.getPath(), ...);
        for (String line : lines) {
            if ("1".equals(line.trim())) {
                new FailoverFileReader().run();  // 开关开启时立即加载数据
                return FAILOVER_SWITCH_TRUE;
            } else if ("0".equals(line.trim())) {
                return FAILOVER_SWITCH_FALSE;
            }
        }
    }
    // 文件未修改 → 直接返回缓存状态，避免每 5s 读磁盘
    return switchParams.get(FAILOVER_MODE_PARAM).equals("true")
        ? FAILOVER_SWITCH_TRUE : FAILOVER_SWITCH_FALSE;
}
```

**为什么用 `lastModifiedMillis` 优化？**

`getSwitch()` 每 5 秒被调用一次。如果没有 `lastModified` 优化，每次都要打开文件 → 读内容 → 解析。通过缓存 `lastModifiedMillis`，只有运维人员真正修改了开关文件（touch / 写入）才触发重新读取，大幅减少磁盘 IO。

### 3.3 FailoverFileReader：加载数据文件

> 源码：[DiskFailoverDataSource.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/backups/datasource/DiskFailoverDataSource.java) L71-108

```java
class FailoverFileReader implements Runnable {
    @Override
    public void run() {
        Map<String, FailoverData> domMap = new HashMap<>(200);
        File[] files = new File(failoverDir).listFiles();
        for (File file : files) {
            if (!file.isFile()) continue;
            if (file.getName().equals(UtilAndComs.FAILOVER_SWITCH)) continue;
            // 复用 DiskCache.parseServiceInfoFromCache() 解析文件
            for (Map.Entry<String, ServiceInfo> entry :
                DiskCache.parseServiceInfoFromCache(file).entrySet()) {
                domMap.put(entry.getKey(),
                    NamingFailoverData.newNamingFailoverData(entry.getValue()));
            }
        }
        if (domMap.size() > 0) {
            serviceMap = domMap;  // 整批替换
        }
    }
}
```

**关键设计：复用 DiskCache 的解析逻辑**

Failover 数据文件的格式与 `ServiceInfoHolder` 的磁盘缓存格式完全一致（都是 JSON 格式的 ServiceInfo）。因此直接复用 `DiskCache.parseServiceInfoFromCache()` 方法，同时享受了对新旧格式（Nacos 1.x 逐行 Instance JSON vs 2.x 整行 ServiceInfo JSON）的兼容支持。

---

## 4. Naming Failover：完整数据流转

### 4.1 用户查询优先走 failover

> 源码：[NacosNamingService.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/NacosNamingService.java) L434-451

```java
private ServiceInfo getServiceInfo(String serviceName, String groupName,
    List<String> clusters, boolean subscribe) throws NacosException {
    // Step 1: 检查 failover 开关
    if (serviceInfoHolder.isFailoverSwitch()) {
        serviceInfo = getServiceInfoByFailover(serviceName, groupName, clusterSelector);
        if (serviceInfo != null && !serviceInfo.getHosts().isEmpty()) {
            return serviceInfo;  // ← failover 命中，直接返回，不请求服务端
        }
    }
    // Step 2: failover 未开启或未命中 → 走正常流程
    serviceInfo = getServiceInfoBySubscribe(serviceName, groupName, clusters,
        clusterSelector, subscribe);
    return serviceInfo;
}
```

**决策优先级**：

```
selectInstances()
  │
  ├─ failover 开启 且 failover 数据非空？
  │   └─ YES → 直接返回 failover 数据（不请求网络）
  │
  └─ NO  → 正常流程
      ├─ 查内存缓存 serviceInfoMap
      ├─ 缓存有 且 已订阅 → 返回
      └─ 缓存无 或 未订阅 → gRPC 订阅
```

### 4.2 服务端推送在 failover 模式下被抑制

> 源码：[ServiceInfoHolder.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L355-367

```java
if (diff.hasDifferent()) {
    // 如果 failover 开启，跳过事件通知（failover 模式下用户走磁盘数据）
    if (!failoverReactor.isFailoverSwitch(serviceKey)) {
        NotifyCenter.publishEvent(
            new InstancesChangeEvent(notifierEventScope, serviceInfo.getName(), ...));
    }
    // 但磁盘缓存仍然写入（为下次 failover 做准备）
    publishDiskCacheRefreshEvent(serviceKey, serviceInfo);
}
```

**为什么 failover 模式下抑制推送但不抑制刷盘？**

- 抑制推送：因为用户查询走的是 failover 数据，服务端推送的数据不应该"覆盖" failover 数据展示给用户
- 继续刷盘：因为 failover 关闭后需要最新的数据，持续刷盘保证关闭时能立即切换到最新数据

### 4.3 getFailoverServiceInfo 委托链

```
NacosNamingService.getServiceInfoByFailover()
  → serviceInfoHolder.getFailoverServiceInfo(serviceName, groupName)  [ServiceInfoHolder L448-451]
    → failoverReactor.getService(key)                                  [FailoverReactor L308-317]
      → serviceMap.get(key)                                            [内存中的 failover 数据]
      → 不存在则返回 new ServiceInfo()（空的 ServiceInfo，不返回 null）
```

---

## 5. Config Failover：三级降级链路

> 源码：[NacosConfigService.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/config/NacosConfigService.java) L293-355

```java
private ConfigResponse getConfigInnerWithResponse(String tenant, String dataId,
    String group, long timeoutMs) throws NacosException {
    ConfigResponse cr = new ConfigResponse();

    // ===== 级别 1：尝试本地 failover 文件 =====
    String content = LocalConfigInfoProcessor.getFailover(
        worker.getAgentName(), dataId, group, tenant);
    if (content != null) {
        cr.setContent(content);
        configFilterChainManager.doFilter(null, cr);
        return cr;  // ← 命中 failover，直接返回
    }

    // ===== 级别 2：尝试从服务端获取 =====
    try {
        ConfigResponse response = worker.getServerConfig(
            dataId, group, tenant, timeoutMs, false);
        cr.setContent(response.getContent());
        cr.setMd5(response.getMd5());
        configFilterChainManager.doFilter(null, cr);
        return cr;  // ← 服务端成功返回
    } catch (NacosException ioe) {
        if (NacosException.NO_RIGHT == ioe.getErrCode()) {
            throw ioe;  // 403 鉴权失败不降级，直接抛出
        }
    }

    // ===== 级别 3：降级到本地 snapshot =====
    content = LocalConfigInfoProcessor.getSnapshot(
        worker.getAgentName(), dataId, group, tenant);
    cr.setContent(content);
    configFilterChainManager.doFilter(null, cr);
    return cr;  // ← 即使 snapshot 为 null 也返回
}
```

**三级降级链路图**：

```
getConfig()
  │
  ├─ Level 1: LocalConfigInfoProcessor.getFailover()
  │   └─ 运维人员手动准备的 failover 文件
  │       → 存在 → 直接返回（最高优先级）
  │
  ├─ Level 2: worker.getServerConfig()
  │   └─ 通过 gRPC 向服务端查询
  │       → 成功 → 返回 + 写入 snapshot
  │       → 403  → 直接抛异常（鉴权失败不降级）
  │       → 其他异常 → 继续降级
  │
  └─ Level 3: LocalConfigInfoProcessor.getSnapshot()
      └─ 上一次成功获取时自动保存的本地快照
          → 存在 → 返回（可能过期，但比没有好）
          → 不存在 → 返回 null
```

**为什么 403 鉴权失败不降级？**

如果客户端用户名/密码错误，降级到本地 snapshot 会掩盖鉴权问题。403 直接抛出异常，让调用方感知到"鉴权失败"这一事实，而不是静默使用过期的 snapshot 数据。

### 5.1 failover 文件路径

> 源码：[LocalConfigInfoProcessor.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/config/impl/LocalConfigInfoProcessor.java) L198-224

```
{user.home}/nacos/config/
├── {serverName}_nacos/
│   └── data/
│       ├── config-data/
│       │   └── {dataId}              ← 默认 group 的 failover 文件
│       └── config-data-tenant/
│           └── {tenant}/
│               └── {group}/
│                   └── {dataId}      ← 指定 tenant+group 的 failover 文件
```

**Naming failover 与 Config failover 文件路径的关键区别**：

- Naming failover：文件在 `{cacheDir}/failover/` 下，通过全局开关文件统一控制
- Config failover：文件在 `{user.home}/nacos/config/` 下，每 5 秒通过 `checkLocalConfig()` 逐个检查文件是否存在

---

## 6. Config Failover：checkLocalConfig 运行时检测

> 源码：[ClientWorker.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/config/impl/ClientWorker.java) L992-1038

Config 的 failover 不是通过全局开关控制的，而是**通过检测 failover 文件是否存在**来实现的。每次 `executeConfigListen()` 执行时（每 5 秒或收到推送事件），都会遍历所有 `CacheData`，检查对应的 failover 文件：

```java
public void checkLocalConfig(CacheData cacheData) {
    File file = LocalConfigInfoProcessor.getFailoverFile(
        envName, dataId, group, tenant);

    // 情况 1: 之前没用本地配置，但 failover 文件出现了 → 切换到 failover
    if (!cacheData.isUseLocalConfigInfo() && file.exists()) {
        String content = LocalConfigInfoProcessor.getFailover(
            envName, dataId, group, tenant);
        cacheData.setUseLocalConfigInfo(true);
        cacheData.setLocalConfigInfoVersion(file.lastModified());
        cacheData.setContent(content);  // ← 覆盖内存中的配置内容
        return;
    }

    // 情况 2: 之前用了 failover，但文件被删除了 → 切换回服务端配置
    if (cacheData.isUseLocalConfigInfo() && !file.exists()) {
        cacheData.setUseLocalConfigInfo(false);
        return;
    }

    // 情况 3: failover 文件内容更新了 → 刷新内存中的配置
    if (cacheData.isUseLocalConfigInfo() && file.exists()
        && cacheData.getLocalConfigInfoVersion() != file.lastModified()) {
        String content = LocalConfigInfoProcessor.getFailover(
            envName, dataId, group, tenant);
        cacheData.setLocalConfigInfoVersion(file.lastModified());
        cacheData.setContent(content);  // ← 刷新为最新 failover 内容
    }
}
```

### 6.1 在 executeConfigListen 中的调用位置

> 源码：[ClientWorker.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/config/impl/ClientWorker.java) L931-968

```java
public void executeConfigListen() throws NacosException {
    for (CacheData cache : cacheMap.get().values()) {
        synchronized (cache) {
            checkLocalConfig(cache);  // ← 每次执行先检查 failover 文件

            if (cache.isConsistentWithServer()) {
                cache.checkListenerMd5();
                if (!needAllSync) continue;
            }

            // 如果正在使用 failover，跳过后面的监听同步
            if (cache.isUseLocalConfigInfo()) {
                continue;
            }
            // ... 正常的长轮询/服务端同步逻辑
        }
    }
}
```

**关键设计**：`isUseLocalConfigInfo() == true` 时跳过服务端同步。因为运维人员已经手动准备了配置内容，不需要再向服务端发起长轮询。但下一次 `checkLocalConfig()` 仍会检查文件是否被删除——删除了就自动切回服务端。

---

## 7. Naming 与 Config Failover 对比

| 维度 | Naming | Config |
|------|--------|--------|
| **触发方式** | 运维写入全局开关文件 + 数据文件 | 运维写入 failover 配置文件 |
| **开关检测** | DiskFailoverDataSource 每 5s 轮询 `lastModified` | checkLocalConfig 每 5s 检查文件是否存在 |
| **全局开关** | ✅ 一个开关控制所有服务 | ❌ 按 dataId 粒度，每个配置独立 |
| **自动降级** | ❌ 不支持（过期实例数据有风险） | ✅ 三级降级（failover→server→snapshot） |
| **数据源 SPI** | ✅ FailoverDataSource 接口 | ❌ 硬编码 LocalConfigInfoProcessor |
| **开关→关闭通知** | ✅ 通过 InstancesDiffer 发布差异 | ✅ 通过 CacheData checkListenerMd5 |
| **数据加载时机** | 开关打开时一次性加载全部 | 逐个 dataId 懒加载 |
| **指标监控** | ✅ Micrometer Gauge (failover_instances) | ❌ 无 |

**为什么 Naming 需要全局开关而 Config 不需要？**

- Naming：服务实例列表是批量查询的（一次 `selectInstances` 可能涉及多个集群的 IP），failover 需要整体切换
- Config：每个 dataId 是独立的（一个配置文件的 failover 不应影响其他配置），粒度更细

**为什么 Naming 不支持自动降级到 snapshot？**

Naming 的实例列表是高度动态的——实例可能在几秒内上下线。如果服务端不可达时自动使用 5 分钟前的磁盘缓存，客户端可能把流量发给已经下线的实例，造成业务故障。Config 的配置内容相对稳定，使用上一次成功获取的 snapshot 风险较低。

---

## 8. 关键设计决策

### 8.1 为什么 FailoverReactor 用 scheduleWithFixedDelay 而非 scheduleAtFixedRate？

**决策理由**：FailoverSwitchRefresher 内部有磁盘 IO（读开关文件、读数据文件）。如果某次轮询因磁盘 IO 慢而耗时超过 5 秒，`scheduleAtFixedRate` 会立即启动下一个任务，导致多个任务并发执行、文件读取竞争。`scheduleWithFixedDelay` 确保上一次完成后才开始计时，避免并发。

### 8.2 为什么 DiskFailoverDataSource.getSwitch() 用 lastModified 缓存？

**决策理由**：`getSwitch()` 每 5 秒被调用。如果没有 `lastModified` 优化，每次都要 `ConcurrentDiskUtil.getFileContent()` 读磁盘。通过文件修改时间判断是否需要重新读取，将磁盘 IO 从"每 5 秒一次"降为"仅在运维修改时一次"。这是典型的"缓存失效"模式在文件系统上的应用。

### 8.3 为什么 Config 的 403 不降级到 snapshot？

**决策理由**：鉴权失败（用户名/密码错误、token 过期未刷新）是一个需要运维介入修复的错误。如果此时静默降级到 snapshot，运维人员不会意识到鉴权出了问题——配置仍能读到（虽然是过期的），掩盖了问题。相反，连接超时/网络不可达降级到 snapshot 是合理的，因为这是临时性故障。

### 8.4 为什么 Naming failover 数据变化时才发布事件？

> 源码：[FailoverReactor.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/backups/FailoverReactor.java) L212-222

```java
InstancesDiff diff = instancesDiffer.doDiff(oldService, newService);
if (diff.hasDifferent()) {
    NotifyCenter.publishEvent(new InstancesChangeEvent(...));
}
```

**决策理由**：避免每 5 秒无意义地触发用户回调。如果 failover 数据没有变化（运维人员没有更新文件），用户 EventListener 不应该被重复调用。`InstancesDiffer.doDiff()` 保证了"只有真正变化时才通知"。

### 8.5 为什么 Config failover 是文件粒度而非全局开关？

**决策理由**：Config 的使用场景通常是"服务端挂了，运维人员为几个关键配置手工准备 failover 文件"。运维人员可能只想为支付服务的数据库连接串准备 failover，而不想影响其他 200 个配置的正常监听。文件粒度的 failover 提供了精细控制能力。

---

## 9. 总结

### 9.1 Naming Failover 完整时序

```
T+0s: 运维人员向 {cacheDir}/failover/ 写入开关文件 + 数据文件
T+0~5s: FailoverSwitchRefresher 检测到开关变化
  → DiskFailoverDataSource.getSwitch() 读取 "1"
  → FailoverFileReader 加载所有数据文件
  → InstancesDiffer.doDiff() 计算差异
  → NotifyCenter.publishEvent() 通知用户
  → serviceMap 更新为 failover 数据

T+5s~: 用户调用 selectInstances()
  → NacosNamingService.getServiceInfo()
  → serviceInfoHolder.isFailoverSwitch() → true
  → getServiceInfoByFailover() → 返回 failover 数据
  → 不请求网络

运维人员删除开关文件:
T+0~5s: FailoverSwitchRefresher 检测到开关关闭
  → serviceMap 中的 failover 数据 与 serviceInfoHolder 中的正常数据做 diff
  → NotifyCenter.publishEvent() 通知用户恢复到正常数据
  → serviceMap.clear()
```

### 9.2 Config Failover 完整时序

```
T+0s: 运维人员向 {user.home}/nacos/config/{serverName}_nacos/data/ 写入 failover 文件

T+0~5s: executeConfigListen() 执行
  → checkLocalConfig(cacheData)
  → 发现 failover 文件存在 → setUseLocalConfigInfo(true)
  → setContent(failover 内容) → checkListenerMd5() → 触发用户 Listener

T+5s~: 用户调用 getConfig()
  → getConfigInnerWithResponse()
  → LocalConfigInfoProcessor.getFailover() → 命中 → 直接返回
  → 不请求服务端

运维人员删除 failover 文件:
T+0~5s: executeConfigListen() → checkLocalConfig()
  → 发现文件不存在 → setUseLocalConfigInfo(false)
  → 重新走长轮询 → refreshContentAndCheck() → checkListenerMd5()
```

### 9.3 关键文件索引

| 文件 | 相对路径 | 角色 |
|------|---------|------|
| FailoverReactor | `client/src/main/java/.../naming/backups/FailoverReactor.java` | Naming failover 反应器，5s 轮询 |
| DiskFailoverDataSource | `client/src/main/java/.../naming/backups/datasource/DiskFailoverDataSource.java` | 磁盘 failover 数据源 |
| FailoverDataSource | `client/src/main/java/.../naming/backups/FailoverDataSource.java` | failover 数据源 SPI 接口 |
| FailoverSwitch | `client/src/main/java/.../naming/backups/FailoverSwitch.java` | failover 开关布尔封装 |
| FailoverData | `client/src/main/java/.../naming/backups/FailoverData.java` | failover 数据抽象 |
| NamingFailoverData | `client/src/main/java/.../naming/backups/NamingFailoverData.java` | Naming failover 数据 |
| ServiceInfoHolder | `client/src/main/java/.../naming/cache/ServiceInfoHolder.java` | 持有 FailoverReactor，提供 isFailoverSwitch() |
| NacosNamingService | `client/src/main/java/.../naming/NacosNamingService.java` | getServiceInfo() 优先走 failover |
| NacosConfigService | `client/src/main/java/.../config/NacosConfigService.java` | 三级降级（failover→server→snapshot） |
| ClientWorker | `client/src/main/java/.../config/impl/ClientWorker.java` | checkLocalConfig() 运行时 failover 检测 |
| LocalConfigInfoProcessor | `client/src/main/java/.../config/impl/LocalConfigInfoProcessor.java` | Config failover/snapshot 文件读写 |
| UtilAndComs | `client/src/main/java/.../naming/utils/UtilAndComs.java` | FAILOVER_SWITCH 文件名常量 |
| DiskCache | `client/src/main/java/.../naming/cache/DiskCache.java` | 被 DiskFailoverDataSource 复用解析逻辑 |
