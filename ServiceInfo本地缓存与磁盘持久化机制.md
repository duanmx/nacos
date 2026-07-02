# ServiceInfo 本地缓存与磁盘持久化机制

> **原则声明**：本文档所有结论均来源于 Nacos 3.2.1-SNAPSHOT 源代码的实际引用，不含任何猜测性描述。

---

## 目录

1. [为什么需要本地缓存与磁盘持久化](#1-为什么需要本地缓存与磁盘持久化)
2. [三层缓存体系](#2-三层缓存体系)
3. [ServiceInfoHolder：内存缓存中心](#3-serviceinfoholder内存缓存中心)
4. [DiskCache：磁盘读写工具](#4-diskcache磁盘读写工具)
5. [ServiceInfoDiskCacheRefresher：异步批量刷盘](#5-serviceinfodiskcacherefresher异步批量刷盘)
6. [完整数据流转时间线](#6-完整数据流转时间线)
7. [关键设计决策](#7-关键设计决策)
8. [总结](#8-总结)

---

## 1. 为什么需要本地缓存与磁盘持久化

Nacos 客户端的本地缓存服务于两个目标：

### 1.1 内存缓存：减少网络开销

```
没有缓存：每次 selectInstances() → 请求服务端 → 网络延迟 10-50ms
有缓存： 每次 selectInstances() → 直接从 ConcurrentHashMap 读取 → <1μs
```

`subscribe()` 之后，gRPC 服务端推送保证数据实时性。缓存用于覆盖两次推送之间的查询请求。

### 1.2 磁盘缓存：进程重启恢复

```
场景：客户端进程重启
  → gRPC 连接断开 → 内存缓存丢失
  → 重连需要时间（网络建立 + 服务端推送）
  → 这段时间内查询返回空 → 业务调用失败

有磁盘缓存：
  → ServiceInfoHolder 构造时从磁盘加载
  → 即使服务端尚未推送，查询也能返回历史数据
```

---

## 2. 三层缓存体系

```
┌─────────────────────────────────────────────────────────────┐
│  第 1 层：内存缓存（ServiceInfoHolder.serviceInfoMap）      │
│  ConcurrentHashMap<String, ServiceInfo>                     │
│  所有查询优先命中，gRPC 推送实时更新                           │
├─────────────────────────────────────────────────────────────┤
│  第 2 层：异步刷盘（ServiceInfoDiskCacheRefresher）         │
│  pendingEvents Map（同 key 覆盖/去抖）+ 100ms 定时器        │
│  实例变更后通过 DiskCacheRefreshEvent 触发                   │
├─────────────────────────────────────────────────────────────┤
│  第 3 层：磁盘文件（DiskCache）                              │
│  {cacheDir}/{URL-encoded-serviceKey}                        │
│  JSON 格式，ConcurrentDiskUtil 并发安全写入                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. ServiceInfoHolder：内存缓存中心

### 3.1 构造与磁盘加载

> 源码：[ServiceInfoHolder.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L184-201

```java
public ServiceInfoHolder(String namespace, String notifierEventScope,
    NacosClientProperties properties) {
    cacheDir = CacheDirUtil.initCacheDir(namespace, properties);
    instancesDiffer = new InstancesDiffer();
    // 如果配置了 NAMING_LOAD_CACHE_AT_START=true，从磁盘加载
    if (isLoadCacheAtStart(properties)) {
        this.serviceInfoMap = new ConcurrentHashMap<>(DiskCache.read(this.cacheDir));
    } else {
        this.serviceInfoMap = new ConcurrentHashMap<>(16);
    }
    this.failoverReactor = new FailoverReactor(this, notifierEventScope);
    this.serviceInfoDiskCacheRefresher = new ServiceInfoDiskCacheRefresher();
    this.pushEmptyProtection = isPushEmptyProtect(properties);
}
```

**关键设计 `NAMING_LOAD_CACHE_AT_START`**：

> 源码：[ServiceInfoHolder.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L208-216

```java
private boolean isLoadCacheAtStart(NacosClientProperties properties) {
    boolean loadCacheAtStart = false;
    if (properties != null && StringUtils.isNotEmpty(
        properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START))) {
        loadCacheAtStart = ConvertUtils.toBoolean(
            properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START));
    }
    return loadCacheAtStart;
}
```

**默认 false**。设置为 true 后，客户端启动时立即从磁盘加载上一次缓存的数据，无需等待 gRPC 连接建立和服务端推送。

**为什么默认关闭？** 磁盘缓存可能已过期（服务端实例列表已变化），使用过期数据可能导致流量打到已下线的实例。只有在明确的容灾场景下才需要开启。

### 3.2 查询方法：返回 clone 防止污染

> 源码：[ServiceInfoHolder.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L255-260

```java
public ServiceInfo getServiceInfo(final String serviceName, final String groupName) {
    String key = NamingUtils.getGroupedName(serviceName, groupName);
    ServiceInfo serviceInfo = serviceInfoMap.get(key);
    // 返回 clone 副本，防止调用方修改对象导致缓存数据被污染
    return serviceInfo == null ? null : serviceInfo.clone();
}
```

**为什么返回 clone？** `serviceInfoMap` 是共享的中央缓存，如果调用方直接修改返回的 ServiceInfo（如 addHost），会污染缓存，导致后续其他查询拿到错误数据。

### 3.3 processServiceInfo：统一入口

> 源码：[ServiceInfoHolder.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L319-372

这是三条数据来源的统一入口：

```java
public ServiceInfo processServiceInfo(ServiceInfo serviceInfo) {
    // Step 1: 校验 serviceKey
    String serviceKey = serviceInfo.getKeyWithoutClusters();
    if (serviceKey == null) return null;

    ServiceInfo oldService = serviceInfoMap.get(serviceKey);

    // Step 2: 空推送保护
    if (isEmptyOrErrorPush(serviceInfo)) {
        return oldService;  // 拒绝空推送，保留旧数据
    }

    // Step 3: 更新内存 + 计算差异
    serviceInfoMap.put(serviceKey, serviceInfo);
    InstancesDiff diff = getServiceInfoDiff(oldService, serviceInfo);

    // Step 4: 发布事件
    if (diff.hasDifferent()) {
        if (!failoverReactor.isFailoverSwitch(serviceKey)) {
            // 非 failover 模式：发布 InstancesChangeEvent 通知用户
            NotifyCenter.publishEvent(new InstancesChangeEvent(...));
        }
        // 无论是 failover 还是正常模式：都异步写入磁盘
        publishDiskCacheRefreshEvent(serviceKey, serviceInfo);
    }
    return serviceInfo;
}
```

### 3.4 空推送保护

> 源码：[ServiceInfoHolder.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L400-402

```java
private boolean isEmptyOrErrorPush(ServiceInfo serviceInfo) {
    return null == serviceInfo.getHosts() || (pushEmptyProtection && !serviceInfo.validate());
}
```

**为什么需要空推送保护？** 服务端异常（如数据丢失、网络分区恢复）时可能推送空实例列表。如果客户端接受，会导致所有实例被摘除。`NAMING_PUSH_EMPTY_PROTECTION` 配置项默认为 false（因为正常场景下推送空列表是合理的——如服务真的全部下线）。

---

## 4. DiskCache：磁盘读写工具

### 4.1 写入

> 源码：[DiskCache.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/DiskCache.java) L108-135

```java
static boolean writeWithResult(ServiceInfo dom, String dir) {
    try {
        makeSureCacheDirExists(dir);
        File file = new File(dir, dom.getKeyEncoded());
        createFileIfAbsent(file, false);

        // 优先使用服务端原始 JSON，避免二次序列化
        String json = dom.getJsonFromServer();
        if (StringUtils.isEmpty(json)) {
            json = JsonUtils.toJson(dom);
        }

        // 并发安全的原子写入（临时文件 → rename）
        ConcurrentDiskUtil.writeFileContent(file, json, StandardCharsets.UTF_8.name());
        return true;
    } catch (Throwable e) {
        NAMING_LOGGER.error("[NA] failed to write cache for dom:" + dom.getName(), e);
        return false;
    }
}
```

**为什么优先使用 `getJsonFromServer()`？**
- gRPC 推送时，`NamingPushRequestHandler` 接收的是 JSON → 反序列化为 ServiceInfo → 保留原始 JSON（`setJsonFromServer`）
- 写磁盘时直接用原始 JSON，避免 `JsonUtils.toJson()` 二次序列化
- 减少 CPU 开销，且保证磁盘内容与服务端完全一致

### 4.2 读取

> 源码：[DiskCache.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/DiskCache.java) L161-181

```java
public static Map<String, ServiceInfo> read(String cacheDir) {
    Map<String, ServiceInfo> domMap = new HashMap<>(16);
    File[] files = makeSureCacheDirExists(cacheDir).listFiles();
    for (File file : files) {
        if (!file.isFile()) continue;
        domMap.putAll(parseServiceInfoFromCache(file));
    }
    return domMap;
}
```

### 4.3 文件格式兼容（新旧格式）

> 源码：[DiskCache.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/DiskCache.java) L198-242

```java
public static Map<String, ServiceInfo> parseServiceInfoFromCache(File file) {
    String fileName = URLDecoder.decode(file.getName(), "UTF-8");
    // 跳过元数据文件
    if (fileName.endsWith("meta") || fileName.endsWith("special-url")) {
        return result;
    }

    ServiceInfo dom = new ServiceInfo(fileName);
    List<Instance> ips = new ArrayList<>();

    // 逐行读取
    while ((json = reader.readLine()) != null) {
        if (!json.startsWith("{")) continue;
        newFormat = JsonUtils.toObj(json, ServiceInfo.class);
        if (StringUtils.isEmpty(newFormat.getName())) {
            // 旧格式：每行一个 Instance JSON
            ips.add(JsonUtils.toObj(json, Instance.class));
        }
    }

    // 优先用新格式，回退到旧格式
    if (newFormat != null && !StringUtils.isEmpty(newFormat.getName())) {
        result.put(dom.getKey(), newFormat);
    } else if (!CollectionUtils.isEmpty(dom.getHosts())) {
        result.put(dom.getKey(), dom);
    }
}
```

**为什么兼容新旧格式？** Nacos 1.x 的磁盘缓存格式是每行一个 Instance JSON（旧格式），Nacos 2.x 改为一整行 ServiceInfo JSON（新格式）。兼容旧格式保证从 1.x 升级到 2.x 时磁盘缓存不被丢弃。

---

## 5. ServiceInfoDiskCacheRefresher：异步批量刷盘

### 5.1 设计动机

> 源码：[ServiceInfoDiskCacheRefresher.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoDiskCacheRefresher.java) L37-40

服务端推送实例变更时，如果在 `processServiceInfo()` 中同步调用 `DiskCache.write()`，JSON 序列化 + 文件 IO 会阻塞推送处理线程（gRPC 网络线程），导致后续推送被延迟。

### 5.2 批处理 + 去抖

```java
// publishEvent() — 存入 pendingEvents（同 key 覆盖）
public void publishEvent(ServiceInfoDiskCacheRefreshEvent event) {
    pendingEvents.put(event.getServiceKey(), event);
}

// 定时器每 100ms 执行
this.refreshExecutor.scheduleWithFixedDelay(this::safeFlushPendingEvents,
    flushIntervalMilliseconds, flushIntervalMilliseconds, TimeUnit.MILLISECONDS);

// flushPendingEvents() — 遍历 pendingEvents，逐个写入磁盘
//   成功 → remove(key, value)（CAS 语义，只移除本次处理的事件）
//   失败 → 保留，下个周期重试
```

**100ms 间隔的作用**：

```
t0: 服务端推送 service-A 变更 → publishEvent → pendingEvents.put("A", eventA)
t1: 服务端推送 service-A 再次变更 → publishEvent → pendingEvents.put("A", eventA')  ← 覆盖

定时器 100ms 后执行：
  → pendingEvents 中只有 service-A → 只写一次磁盘
  → 两次推送 → 一次磁盘写入 ✅
```

### 5.3 shutdown 流程：确保不丢数据

> 源码：[ServiceInfoDiskCacheRefresher.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoDiskCacheRefresher.java) L256-275

```java
public void shutdown() throws NacosException {
    // 1. 关闭前先刷一次盘
    flushPendingEvents();
    // 2. 停止定时任务
    refreshExecutor.shutdown();
    // 3. 等待线程池终止（最多等 3 秒）
    if (!refreshExecutor.awaitTermination(shutdownTimeoutMilliseconds, TimeUnit.MILLISECONDS)) {
        NAMING_LOGGER.warn("timeout while waiting ... pending event size: {}",
            pendingEvents.size());
    }
    // 4. 最后再刷一次（防止等待期间有新事件）
    flushPendingEvents();
}
```

**四阶段关闭保证**：flush → shutdown → awaitTermination → flush again。即使最后一个 flush 写入失败，也不阻塞关闭。

---

## 6. 完整数据流转时间线

### 6.1 首次启动（loadCacheAtStart=true）

```
1. ServiceInfoHolder 构造
   → DiskCache.read(cacheDir) → 遍历磁盘文件 → parseServiceInfoFromCache()
   → serviceInfoMap = { "order-service": ServiceInfo(磁盘数据) }

2. gRPC 连接建立 → 服务端推送最新 ServiceInfo
   → NamingPushRequestHandler → processServiceInfo()
   → InstancesDiffer.doDiff(磁盘旧数据, 推送新数据) → hasDifferent=true
   → serviceInfoMap.put(新数据)  ← 覆盖磁盘旧数据
   → publishDiskCacheRefreshEvent() → 100ms 后写入磁盘（更新）
```

### 6.2 运行时推送更新

```
1. 服务端推送 → NamingPushRequestHandler
   → serviceInfoHolder.processServiceInfo(serviceInfo)
     → isEmptyOrErrorPush? → 空推送保护检查
     → serviceInfoMap.put(key, serviceInfo)
     → InstancesDiffer.doDiff(old, new) → added/removed/modified
     → diff.hasDifferent()?
       → NotifyCenter.publishEvent(InstancesChangeEvent)  ← 通知用户
       → publishDiskCacheRefreshEvent() → 100ms 后异步刷盘
```

### 6.3 用户查询

```
NacosNamingService.selectInstances()
  → getServiceInfo()
    ├─ failover 开启? → getFailoverServiceInfo() → 磁盘
    └─ failover 关闭? → getServiceInfoBySubscribe()
        → serviceInfoHolder.getServiceInfo(key)
          → serviceInfoMap.get(key).clone()  ← 内存缓存，返回 clone
```

---

## 7. 关键设计决策

### 7.1 为什么用 clone 返回而不是直接引用？

> 源码：[ServiceInfoHolder.java](file:///Users/mmhm/IdeaProjects/nacos/client/src/main/java/com/alibaba/nacos/client/naming/cache/ServiceInfoHolder.java) L255-260

**决策理由**：`serviceInfoMap` 是多个线程共享的中央缓存。如果直接返回引用，调用方可能修改 `hosts` 列表（如排序、过滤），导致其他查询线程拿到被篡改的数据。

### 7.2 为什么异步刷盘而不是同步刷盘？

**决策理由**：

1. **推送处理不能慢**：gRPC 推送线程是网络线程，同步磁盘 IO（文件写入 + fsync）可能耗时 10-100ms，阻塞推送处理
2. **去抖减少 IO**：100ms 窗口内同一服务的多次推送合并为一次写入
3. **失败不阻塞**：磁盘写入失败只记录日志，不影响服务发现功能

### 7.3 为什么优先用 getJsonFromServer()？

**决策理由**：

1. **避免序列化开销**：JsonUtils.toJson() 需要遍历对象图 → 反射 → JSON 序列化
2. **保证一致性**：服务端原始 JSON 与磁盘内容完全一致，避免序列化差异导致的乱码或字段丢失

### 7.4 为什么 loadCacheAtStart 默认 false？

**决策理由**：磁盘缓存的实例列表可能已过时（实例已下线但仍在磁盘文件中）。在非容灾场景下，使用过期数据比"暂时没有数据"更危险——流量可能打到已下线的实例。

### 7.5 为什么空推送保护默认 false？

**决策理由**：Nacos 2.x 的 Distro 协议保证了推送的原子性。空推送在正常场景下表示服务确实没有可用实例了（如全部下线维护）。如果默认开启保护，会导致"服务已全部下线但客户端还在用旧数据"的问题。

---

## 8. 总结

### 8.1 缓存更新路径

```
服务端推送 ServiceInfo
  │
  ▼
ServiceInfoHolder.processServiceInfo()
  │
  ├── 更新内存 serviceInfoMap.put(key, serviceInfo)
  │
  ├── InstancesDiffer.doDiff(old, new) → 计算差异
  │
  ├── diff.hasDifferent()?
  │   ├── InstancesChangeEvent → NotifyCenter → 用户 EventListener
  │   └── publishDiskCacheRefreshEvent()
  │       └── ServiceInfoDiskCacheRefresher.publishEvent()
  │           └── pendingEvents.put(key, event)  ← 去抖
  │               └── 100ms 定时器 → flushPendingEvents()
  │                   └── DiskCache.writeWithResult()
  │                       └── ConcurrentDiskUtil.writeFileContent()
  │                           └── {cacheDir}/{encoded-serviceKey}
  │
  ├── 进程重启 (loadCacheAtStart=true)
  │   └── DiskCache.read(cacheDir)
  │       └── parseServiceInfoFromCache(file)
  │           └── serviceInfoMap = new ConcurrentHashMap<>(磁盘数据)
  │
  └── failover 开启
      └── DiskFailoverDataSource.getFailoverData()
          └── DiskCache.read(failoverDir)  ← 读取运维准备的应急文件
```

### 8.2 关键文件索引

| 文件 | 相对路径 | 角色 |
|------|---------|------|
| ServiceInfoHolder | `client/src/main/java/.../naming/cache/ServiceInfoHolder.java` | 内存缓存 + 统一入口 |
| ServiceInfoDiskCacheRefresher | `client/src/main/java/.../naming/cache/ServiceInfoDiskCacheRefresher.java` | 异步批量刷盘 |
| DiskCache | `client/src/main/java/.../naming/cache/DiskCache.java` | 磁盘读写工具 |
| InstancesDiffer | `client/src/main/java/.../naming/cache/InstancesDiffer.java` | 实例差异比较 |
| DiskFailoverDataSource | `client/src/main/java/.../naming/backups/datasource/DiskFailoverDataSource.java` | failover 数据源 |
| CacheDirUtil | `client/src/main/java/.../naming/utils/CacheDirUtil.java` | 缓存目录路径计算 |
| ConcurrentDiskUtil | `client/src/main/java/.../utils/ConcurrentDiskUtil.java` | 并发安全文件写入 |
