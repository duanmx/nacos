# Nacos 功能协商机制深度解析

> **定位**：Nacos 的 Ability 系统是一种**功能版本协商机制**，通过功能表在服务端与客户端之间自动识别并启用双方共同支持的功能，避免因版本差异导致功能不可用。

---

## 一、为什么需要功能协商？

### 问题场景

Nacos 部署拓扑中可能存在版本混合：

```
  Nacos Server v3.2        Nacos Client v3.1
  ┌──────────────┐         ┌──────────────┐
  │ 支持 FuzzyWatch │  ──── │ 不支持 FuzzyWatch │
  │ 支持 DistroLock│       │ 支持 DistroLock│
  └──────────────┘         └──────────────┘
```

如果没有协商机制，服务端开启了客户端不支持的功能，会导致客户端行为异常。

### 解决方案

**双方在 gRPC 连接建立时交换各自的功能表，后续通信只使用双方同时支持（取交集）的功能。**

---

## 二、整体架构

核心思想：**面向抽象编程**。`NacosAbilityManagerHolder` 只依赖 `AbstractAbilityControlManager`
接口，不关心具体是服务端还是客户端实现。具体实现通过 SPI 按优先级自动选择。

### 2.1 数据流转图（全貌）

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                         gRPC 消费端（调用方）                         │
  │                                                                    │
  │  GrpcClient.connectToServer()  L408-409                            │
  │    └─ NacosAbilityManagerHolder.getInstance()                       │
  │         .getCurrentNodeAbilities(abilityMode())                     │
  │         │                                                          │
  │         ├─ GrpcSdkClient.abilityMode()    → SDK_CLIENT     L62-63  │
  │         └─ GrpcClusterClient.abilityMode() → CLUSTER_CLIENT L73-74 │
  │              → ConnectionSetupRequest（客户端功能表）                 │
  │                                                                    │
  │  GrpcBiStreamRequestAcceptor.onNext()  L209-211                    │
  │    └─ NacosAbilityManagerHolder.getInstance()                       │
  │         .getCurrentNodeAbilities(SERVER)                            │
  │         → SetupAckRequest（服务端功能表）                             │
  └────────────────────────────┬───────────────────────────────────────┘
                               │ 调用 getCurrentNodeAbilities()
                               ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │                    NacosAbilityManagerHolder                        │
  │                    （SPI 发现中枢，面向抽象）                          │
  │                                                                    │
  │   NacosServiceLoader.load(AbstractAbilityControlManager.class)     │
  │     │                                                              │
  │     ├─ 实现 A: ServerAbilityControlManager  priority = 1           │
  │     └─ 实现 B: ClientAbilityControlManager  priority = 0           │
  │              │                                                     │
  │              按 priority 降序排序 → 取最高者作为活性实例               │
  │              Server 优先，Client 作为降级兜底                         │
  └────────────────────────────┬───────────────────────────────────────┘
                               │ 持有 AbstractAbilityControlManager（抽象引用）
                               ▼
  ┌────────────────────────────────────────────────────────────────────┐
  │                AbstractAbilityControlManager（抽象契约层）            │
  │                    模板方法模式，定义功能表的完整契约                    │
  │                                                                    │
  │  ┌─ initAbilityTable()            ← 模板方法，定义初始化骨架         │
  │  │    ├─ initCurrentNodeAbilities() ← 子类提供出厂值（抽象方法）      │
  │  │    ├─ fail-fast 校验                                           │
  │  │    ├─ AbilityPostProcessor SPI 后处理                           │
  │  │    └─ mapStr() → currentNodeAbilities                          │
  │  │                                                                │
  │  ├─ enableCurrentNodeAbility()   ← 运行时开关（被 AbilityConfigs 调用）│
  │  ├─ disableCurrentNodeAbility()  ← 运行时开关                      │
  │  ├─ isCurrentNodeAbilityRunning()← 状态查询                        │
  │  └─ getCurrentNodeAbilities()    ← 导出给 gRPC 消费端               │
  │                                                                    │
  │  存储：Map<AbilityMode, Map<String, Boolean>> currentNodeAbilities │
  └────────────────────────────┬───────────────────────────────────────┘
                               │ 子类实现（模板方法回调）
                    ┌──────────┴──────────┐
                    ▼                     ▼
  ┌──────────────────────────┐  ┌──────────────────────────┐
  │ServerAbilityControlManager│  │ClientAbilityControlManager│
  │  priority = 1（服务端优先） │  │  priority = 0（客户端兜底） │
  │                          │  │                          │
  │  initCurrentNodeAbilities│  │  initCurrentNodeAbilities│
  │  ① ServerAbilities 出厂值│  │   SdkClientAbilities 出厂值│
  │  ② EnvUtil 配置覆盖      │  │                          │
  │  ③ SdkClientAbilities    │  │  仅 SDK_CLIENT 模态       │
  │  ④ ClusterClientAbilities│  │                          │
  │                          │  │                          │
  │  覆盖三模态                │  │                          │
  └──────────────────────────┘  └──────────────────────────┘

  ═══════════════════════════════════════════════════════════════════
  运行时配置变更（侧通道，写入同一个 currentNodeAbilities 存储）

  application.properties 文件变更
    └─ NacosCoreStartUp.FileWatcher.onChange()                    L183-191
         └─ NotifyCenter.publishEvent(
              ServerConfigChangeEvent.newEvent())                 L188
              └─ AbilityConfigs.onEvent()                         L151
                   ├─ 扫描 nacos.core.ability.* 配置项
                   └─ refresh()                                   L187
                        ├─ enableCurrentNodeAbility(key)  ──────────┐
                        └─ disableCurrentNodeAbility(key) ──────────┤
                                                                     │
  ═══════════════════════════════════════════════════════════════════ │
                                                                     │
         ┌───────────────────────────────────────────────────────────┘
         │  调用 AbstractAbilityControlManager（抽象引用）
         │  abilityHandlerRegistry = NacosAbilityManagerHolder.getInstance()
         ▼
  ┌──────────────────────────────────────────────────────┐
  │    AbstractAbilityControlManager（同一个抽象契约）      │
  │    enable/disable → doTurn()                         │
  │      ├─ 修改 currentNodeAbilities 存储               │
  │      └─ publishEvent(AbilityUpdateEvent)（预留）       │
  └──────────────────────────────────────────────────────┘
```

**分层解读**：

| 层 | 组件 | 职责 |
|----|------|------|
| 消费层 | `GrpcClient` / `GrpcBiStreamRequestAcceptor` | gRPC 握手时调用 `getCurrentNodeAbilities()` 读写功能表 |
| SPI 发现层 | `NacosAbilityManagerHolder` | 加载所有实现，按优先级竞争，对外暴露抽象引用 |
| 抽象契约层 | `AbstractAbilityControlManager` | 模板方法骨架：初始化 → 校验 → 后处理 → 存储 → 查询 |
| 具体实现层 | `ServerAbilityControlManager` / `ClientAbilityControlManager` | 提供出厂功能表，区分服务端 / 客户端场景 |
| 配置驱动层 | AbilityConfigs → NotifyCenter | 监听 application.properties 变更，触发运行时开关 |

### 2.2 类关系图（UML）

去掉实现细节，只展示类之间的继承、依赖和关联关系：

```
  ┌───────────────────────┐          ┌──────────────────────────────┐
  │      GrpcClient       │          │  GrpcBiStreamRequestAcceptor │
  │  (gRPC 客户端连接)      │          │  (gRPC 服务端入站处理器)       │
  └───────────┬───────────┘          └──────────────┬───────────────┘
              │  调用                                 │  调用
              ▼                                       ▼
  ┌───────────────────────────────────────────────────────────────────┐
  │               NacosAbilityManagerHolder（SPI 发现中枢）             │
  │   - 通过 NacosServiceLoader 发现所有 AbstractAbilityControlManager │
  │   - 按 priority 降序选择活性实例                                    │
  │   - 对外只暴露 AbstractAbilityControlManager 抽象引用               │
  └───────────────────────────┬───────────────────────────────────────┘
                              │ 持有 1 个活性实例（组合）
                              ▼
  ┌───────────────────────────────────────────────────────────────────┐
  │    <<abstract>> AbstractAbilityControlManager（抽象契约）           │
  │  ───────────────────────────────────────────────────────────────── │
  │  + initAbilityTable()                模板方法（骨架）               │
  │  + enableCurrentNodeAbility(AbilityKey)                            │
  │  + disableCurrentNodeAbility(AbilityKey)                           │
  │  + isCurrentNodeAbilityRunning(AbilityKey) : AbilityStatus         │
  │  + getCurrentNodeAbilities(AbilityMode) : Map<String,Boolean>      │
  │  # initCurrentNodeAbilities() : Map<AbilityMode,Map<...>>  抽象方法│
  │  # getPriority() : int                              抽象方法      │
  └────────────────────────────┬──────────────────────────────────────┘
                               △  继承（实线空心三角）
                    ┌──────────┴──────────┐
                    │                     │
  ┌─────────────────────────────┐  ┌─────────────────────────────┐
  │  ServerAbilityControlManager│  │  ClientAbilityControlManager│
  │  priority = 1               │  │  priority = 0               │
  │  (服务端运行时，优先选中)       │  │  (客户端运行时，兜底)          │
  └─────────────────────────────┘  └─────────────────────────────┘
                    ▲
                    │ 依赖（虚线箭头，读取静态出厂值）
  ┌─────────────────┴──────────────────┐
  │          <<static>>                │
  │  ServerAbilities / SdkClientAblts  │
  │  ClusterClientAbilities            │
  │  (静态能力声明类)                    │
  └────────────────────────────────────┘

  ═══════════════════════════════════════════════════════════════════
  运行时写路径（侧通道）

  ┌───────────────────────┐
  │     AbilityConfigs    │ ──── enable/disable ────>  AbstractAbilityControlManager
  │  (配置变更监听器)       │                           (同一个抽象契约，修改 currentNodeAbilities)
  └───────────────────────┘
         ▲
         │ 订阅 ServerConfigChangeEvent
  ┌──────┴──────────┐
  │  NotifyCenter   │
  │  (事件总线)      │
  └─────────────────┘

  图例：
    ────▶  依赖/调用
    ──◆    组合（持有者指向被持有者）
    ──△    继承（子类指向父类）
    <<X>>  构造型标注
```

### 2.3 用了哪些设计模式？为什么这么设计？

| 模式 | 体现在哪 | 解决什么问题 | 好处 | 坏处 |
|------|---------|-------------|------|------|
| **模板方法** | `AbstractAbilityControlManager.initAbilityTable()` 定义四步骨架，`initCurrentNodeAbilities()` 和 `getPriority()` 交给子类 | 不同场景（服务端/客户端）的能力初始化流程一致，只有出厂值不同 | 骨架复用，子类只需 30 行代码 | 骨架固定后不易调整步骤顺序；子类必须实现所有抽象方法，哪怕用不上 |
| **SPI + 优先级竞争** | `NacosAbilityManagerHolder` 通过 `NacosServiceLoader` 发现所有实现，按 `getPriority()` 降序取最高者 | 同一 JVM 可能同时存在 Server 和 Client 依赖，需要自动选一个 | 松耦合，新增实现只需加 SPI 文件；Client 可作为 Server 的降级兜底 | 优先级是魔法数字（1, 0），缺乏语义；同优先级时行为未定义 |
| **观察者（事件监听）** | `AbilityConfigs` 订阅 `ServerConfigChangeEvent`；`doTurn()` 发布 `AbilityUpdateEvent` | 配置文件变更后，功能表需要实时刷新，但不能让配置读取模块直接耦合能力管理模块 | 读写解耦；配置变更不重启即可生效 | 事件链路（文件→NotifyCenter→Subscriber→enable）分散在 3 个模块，追踪困难；`AbilityUpdateEvent` 目前无人消费，Publisher 线程空转 |
| **单例 + 门面** | `NacosAbilityManagerHolder.getInstance()` 是单例，对外只暴露 `getCurrentNodeAbilities()` / `enable/disable` | 全局需要唯一的能力表权威来源，调用方不关心内部 SPI 加载细节 | 统一入口，避免多处持有不同 Manager 实例导致数据不一致 | 单例持有全局状态，单元测试需要特殊处理；`getInstance()` 是静态方法，无法被接口抽象 |
| **懒加载工厂** | `NotifyCenter.DEFAULT_PUBLISHER_FACTORY` 通过 `computeIfAbsent` 为每个事件类型创建独立的 `DefaultPublisher` 线程 | 事件类型很多但多数可能没有订阅者，提前创建所有 Publisher 浪费线程 | 按需创建，不浪费资源 | 首次发布事件时 `computeIfAbsent` 在 `synchronized` 块中，有微小延迟 |

### 2.4 整体设计的优点

- **对扩展开放，对修改封闭**：加一个新功能项（如 `SERVER_NEW_FEATURE`），只需在 `AbilityKey` 枚举加一项，无需改任何业务逻辑
- **运行时可干预**：运维通过修改 `application.properties` 即可关闭某个功能，不用重启、不用发版
- **版本协商自动化**：gRPC 握手时双向交换功能表，老客户端连新服务端时自动降级
- **分层清晰**：gRPC（读）→ Holder → 抽象 → 实现 的调用链自上而下，单向依赖

### 2.5 潜在问题

- **学习曲线陡峭**：新人要同时理解 SPI、模板方法、事件驱动、gRPC 握手 四套机制才能串起全链路
- **过度预留**：`AbilityUpdateEvent` 和 `AbilityPostProcessor` 当前无人使用，增加了理解负担却没有实际价值
- **配置分散**：`AbilityConfigs` 在 core 模块消费 `NotifyCenter` 事件，但 `NotifyCenter` 在 common 模块，`NacosCoreStartUp` 又在 core 的另一层——三个模块间的隐式约定不读源码无法知道
- **新旧系统并存**：`ServerAbilityInitializer`（老）和 `AbstractAbilityControlManager`（新）都在用，已标注 `@Deprecated` 但未删除，增加认知负担

---

## 三、核心概念

### 3.1 AbilityMode（三模态）

| 模式 | 用途 | 管理方 |
|------|------|--------|
| `SERVER` | 服务端自身功能（如 FuzzyWatch、DistroLock） | `ServerAbilityControlManager` |
| `SDK_CLIENT` | 客户端 SDK 功能（如 NacosClient → Server 通信） | `ClientAbilityControlManager` |
| `CLUSTER_CLIENT` | 集群间客户端功能（集群间 RPC 通信） | 静态声明 |

### 3.2 AbilityKey（功能枚举）

`AbilityKey` 枚举声明了所有可能的原子功能项，每个项有：

- **`name`**：字符串键，用于配置文件查找和 gRPC 线缆传输
- **`mode`**：所属模态
- **`mapStr()`**：将 `Map<AbilityKey, Boolean>` 转换为 `Map<String, Boolean>`（枚举键 → 字符串键，适配 gRPC 传输）

**服务端功能（SERVER 模式）**：

| 功能键 | 含义 |
|--------|------|
| `SERVER_DISTRO` | 支持 Distro AP 一致性协议 |
| `SERVER_RAFT` | 支持 JRaft CP 一致性协议 |
| `SERVER_REMOTE_CONNECTION` | 支持远程连接 |
| `SERVER_CONFIG_CHANGE_NOTIFY` | 支持配置变更通知 |
| `SERVER_FUZZY_WATCH` | 支持模糊监听 |
| `SERVER_DISTRIBUTED_LOCK` | 支持分布式锁 |

**客户端功能（SDK_CLIENT 模式）**：

| 功能键 | 含义 |
|--------|------|
| `SDK_CLIENT_CONFIG_CHANGE_NOTIFY` | 支持配置变更通知 |
| `SDK_CLIENT_FUZZY_WATCH` | 支持模糊监听 |
| `SDK_CLIENT_DISTRIBUTED_LOCK` | 支持分布式锁 |
| `SDK_CLIENT_SERVICE_DISCOVERY` | 支持服务发现 |

**集群客户端功能（CLUSTER_CLIENT 模式）**：

| 功能键 | 含义 |
|--------|------|
| `CLUSTER_CLIENT_READ_ONLY_SERVER` | 只读服务器集群客户端 |

### 3.3 AbilityStatus（三态）

```java
public enum AbilityStatus {
    SUPPORTED,       // 支持
    NOT_SUPPORTED,   // 不支持
    UNKNOWN          // 未知（未初始化）
}
```

---

## 四、数据流转全景

```
┌──────────────────── 启动阶段 ────────────────────┐
│                                                    │
│  ServerAbilityControlManager() 构造                 │
│    ├─ ① initCurrentNodeAbilities()                 │
│    │     ├─ 读 ServerAbilities.getStaticAbilities()│ ← 出厂默认
│    │     ├─ 读 EnvUtil.getProperty(PREFIX + name)  │ ← 配置文件覆盖
│    │     ├─ 读 SdkClientAbilities                  │ ← SDK 客户端出厂
│    │     └─ 读 ClusterClientAbilities              │ ← 集群客户端出厂
│    │                                                │
│    └─ ② initAbilityTable()                         │
│          ├─ fail-fast 校验（功能值不为 null）         │
│          ├─ AbilityPostProcessor SPI 后处理         │ ← SPI 扩展点
│          └─ AbilityKey.mapStr() → currentNodeAbilities │
│                                                    │
│  同时：registerToPublisher(AbilityUpdateEvent)       │ ← 预留发布通道
│        NotifyCenter.registerToPublisher(AbilityUpdateEvent.class, 16384)
│                                                    │
├──────────────────── 运行时 ────────────────────────┤
│                                                    │
│  application.properties 文件变更                     │
│    └─ NacosCoreStartUp.FileWatcher.onChange()       │
│         └─ NotifyCenter.publishEvent(               │
│              ServerConfigChangeEvent.newEvent())    │
│              └─ AbilityConfigs.onEvent()            │
│                   ├─ 扫描 nacos.core.ability.*      │
│                   └─ refresh()                      │
│                        ├─ enableCurrentNodeAbility() │
│                        └─ disableCurrentNodeAbility()│
│                             └─ doTurn()             │
│                                  └─ publishEvent(    │ ← 发布功能变更事件
│                                       AbilityUpdateEvent) │
│                                                    │
├──────────────────── gRPC 协商 ─────────────────────┤
│                                                    │
│  Client → Server（连接建立）                         │
│    GrpcClient.connectToServer()                    │
│      └─ setAbilityTable(                           │
│           NacosAbilityManagerHolder                 │
│             .getCurrentNodeAbilities(SDK_CLIENT))  │ ← 客户端功能表
│           → ConnectionSetupRequest                 │
│                                                    │
│  Server → Client（ACK 应答）                        │
│    GrpcBiStreamRequestAcceptor.onNext()            │
│      └─ getCurrentNodeAbilities(SERVER)            │ ← 服务端功能表
│           → SetupAckRequest                        │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## 五、核心类详解

### 5.1 AbstractAbilityControlManager

**定位**：功能控制中心的抽象基类，模板方法模式。

**存储结构**：
```java
Map<AbilityMode, Map<String, Boolean>> currentNodeAbilities;
// 外层 key = AbilityMode（SERVER / SDK_CLIENT / CLUSTER_CLIENT）
// 内层 key = AbilityKey.name 的字符串（如 "fuzzyWatch"）
//       val = true/false
```

**核心流程**（[源码 L57-L96](file:///Users/mmhm/IdeaProjects/nacos/common/src/main/java/com/alibaba/nacos/common/ability/AbstractAbilityControlManager.java#L57-L96)）：

```
initAbilityTable()
  ├─ Step 1: initCurrentNodeAbilities()  ← 子类提供出厂值
  ├─ Step 2: fail-fast 校验（值非 null）
  ├─ Step 3: AbilityPostProcessor SPI 后处理
  └─ Step 4: mapStr() 转换 → currentNodeAbilities
```

**运行时开关**（[源码 L196-L232](file:///Users/mmhm/IdeaProjects/nacos/common/src/main/java/com/alibaba/nacos/common/ability/AbstractAbilityControlManager.java#L196-L232)）：

```
enableCurrentNodeAbility(abilityKey)
  → doTurn(abilityKey, true)
    → 修改 currentNodeAbilities 中的值
    → publishEvent(AbilityUpdateEvent)  ← 预留扩展点

disableCurrentNodeAbility(abilityKey)
  → doTurn(abilityKey, false)    ← 同上
```

### 5.2 NacosAbilityManagerHolder

**定位**：SPI 发现中枢，通过优先级竞争确定活跃的 AbilityManager。

**初始化**（[源码 L74-L87](file:///Users/mmhm/IdeaProjects/nacos/common/src/main/java/com/alibaba/nacos/common/ability/discover/NacosAbilityManagerHolder.java#L74-L87)）：

```java
// 1. 通过 NacosServiceLoader 加载所有 AbstractAbilityControlManager 实现
// 2. 按 getPriority() 降序排序
// 3. 取优先级最高的作为活性实例
```

当前优先级：
- `ServerAbilityControlManager`：priority = 1（服务端运行时）
- `ClientAbilityControlManager`：priority = 0（客户端运行时）

### 5.3 ServerAbilityControlManager

**配置覆盖逻辑**（[源码 L58-L75](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/ability/control/ServerAbilityControlManager.java#L58-L75)）：

```java
// ① 先查配置文件 nacos.core.ability.xxx
Boolean property = EnvUtil.getProperty(AbilityConfigs.PREFIX + name, Boolean.class);
if (property != null) {
    abilityTable.put(abilityKey, property);  // 配置文件优先
} else {
    unIncludedInConfig.add(abilityKey);      // 标记为回落
}
// ② 配置未覆盖的回落出厂默认值
unIncludedInConfig.forEach(
    key -> abilityTable.put(key, staticAbilities.get(key)));
```

### 5.4 AbilityConfigs

**定位**：配置文件 → 功能开关的运行时翻译器。

**触发链**：

```
application.properties 变更
  └─ NacosCoreStartUp.FileWatcher.onChange()
       └─ NotifyCenter.publishEvent(ServerConfigChangeEvent.newEvent())  [L188]
            └─ AbilityConfigs.onEvent()                                   [L151]
                 ├─ ① 扫描 nacos.core.ability.* 配置项
                 └─ ② refresh() → enable/disable
```

详见 [AbilityConfigs.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/ability/config/AbilityConfigs.java)。

### 5.5 gRPC 消费端

**客户端发送**（[GrpcClient L408-L409](file:///Users/mmhm/IdeaProjects/nacos/common/src/main/java/com/alibaba/nacos/common/remote/client/grpc/GrpcClient.java#L408-L409)）：

```java
// connectToServer() 中将 SDK_CLIENT 功能表写入连接建立请求
request.setAbilityTable(
    NacosAbilityManagerHolder.getInstance()
        .getCurrentNodeAbilities(AbilityMode.SDK_CLIENT));
```

**服务端应答**（[GrpcBiStreamRequestAcceptor L209-L211](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcBiStreamRequestAcceptor.java#L209-L211)）：

```java
// onNext() 中将 SERVER 功能表写入 ACK 应答
response.setAbilityTable(
    NacosAbilityManagerHolder.getInstance()
        .getCurrentNodeAbilities(AbilityMode.SERVER));
```

---

## 六、新旧系统并存

| 维度 | 老系统（@Deprecated） | 新系统（主线） |
|------|----------------------|----------------|
| **入口** | `ServerAbilityInitializer` SPI | `AbstractAbilityControlManager` SPI |
| **存储** | `ServerAbilities`（子对象模型，字段级） | `Map<AbilityMode, Map<String, Boolean>>`（表模型） |
| **功能项** | 硬编码字段(`setSupportRemoteConnection()`) | `AbilityKey` 枚举 + `AbilityMode` 三模态 |
| **配置动态化** | 无 | `AbilityConfigs` 监听 `application.properties` |
| **gRPC 协商** | 无 | `ConnectionSetupRequest/Ack` 双向交换 |
| **状态查询** | 无 | `isCurrentNodeAbilityRunning()` 三态查询 |

**调用点**：老系统残留于 `ServerMemberManager.initMemberAbilities()` [L183-L190](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/cluster/ServerMemberManager.java#L183-L190)，用于在集群成员对象上设置功能标记，但该方法本身也已标注 `@Deprecated`。

---

## 七、扩展点

### 7.1 AbilityPostProcessor（SPI）

```java
public interface AbilityPostProcessor {
    void process(AbilityMode mode, Map<AbilityKey, Boolean> abilities);
}
```

- 调用时机：`initAbilityTable()` 的 Step 3，在 fail-fast 校验之后、写入存储之前
- 当前状态：**无生产实现**（仅有测试 mock）
- 用途：允许插件在初始化阶段修改功能表（如根据运行时环境动态关闭某项功能）

### 7.2 AbilityUpdateEvent（事件）

- 发布时机：每次 `enable/disableCurrentNodeAbility()` → `doTurn()` 完成时
- 当前状态：**无生产订阅者**（基础设施已就绪，等待未来消费方接入）

---

## 八、关键设计决策

1. **枚举键 → 字符串键转换**（`AbilityKey.mapStr()`）：因为 gRPC wire 上传输的是字符串，不是 Java 枚举对象，两边必须对相同的字符串键达成一致
2. **SPI 优先级竞争**：服务端和客户端分别有各自的实现，通过 `getPriority()` 决定谁生效，避免同一个 JVM 中出现两个 Manager 冲突
3. **配置覆盖优先级**：运行时配置（`nacos.core.ability.*`）> 子类工厂出厂值 > 枚举默认值
4. **fail-fast 校验**：如果子类 `initCurrentNodeAbilities()` 返回了 null 值的功能键，直接抛异常，防止静默错误
5. **预留式设计**：`AbilityUpdateEvent` 发布但无人消费，`AbilityPostProcessor` 接口定义但无实现，说明设计者预见到了未来的扩展需求但当前版本暂不需要

---

## 九、相关文件清单

| 文件 | 模块 | 角色 |
|------|------|------|
| `AbilityKey.java` | api | 功能枚举定义 |
| `AbilityMode.java` | api | 三模态枚举 |
| `AbilityStatus.java` | api | 三态枚举 |
| `AbilityPostProcessor.java` | api | SPI 后处理接口 |
| `AbstractAbilityControlManager.java` | common | 功能管理抽象基类 |
| `NacosAbilityManagerHolder.java` | common | SPI 发现 + 优先级竞争 |
| `AbilityConfigs.java` | core | 运行时配置监听 |
| `ServerAbilityControlManager.java` | core | 服务端功能初始化 |
| `ClientAbilityControlManager.java` | client | 客户端功能初始化 |
| `ServerAbilities.java` | api | 服务端出厂默认值 |
| `SdkClientAbilities.java` | api | SDK 客户端出厂默认值 |
| `ClusterClientAbilities.java` | api | 集群客户端出厂默认值 |
| `GrpcClient.java` | common | gRPC 客户端（消费 SDK_CLIENT 功能表） |
| `GrpcBiStreamRequestAcceptor.java` | core | gRPC 服务端（消费 SERVER 功能表） |
