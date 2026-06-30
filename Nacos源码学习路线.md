# Nacos 源码学习路线

## 总体策略：文档即地图

先通过 `specs/` 规范文档理解设计意图和整体架构，再带着"地图"阅读代码。避免在代码中盲目跳转。

---

## 第一步：阅读设计规范（建立地图）

`specs/zh-cn/` 下有 80+ 篇规范文档，按依赖关系分为 5 层——上层概念会被下层直接引用，按层阅读就不会乱。

### 1.0 规范层次总览

```
第 1 层  顶层设计  (3 篇)
  ↓
第 2 层  基础能力  (11 篇)
  ↓
第 3 层  接口规范  (5 篇)
  ↓
第 4 层  领域业务  (按兴趣选)
  ↓
第 5 层  扩展 & 安全 & 测试  (按兴趣选)
```

---

### 第 1 层：顶层设计（必读，建立全局坐标系）

| 顺序 | 文档 | 核心问题 | 预计时间 |
|------|------|---------|---------|
| 1 | `design/nacos-design-spec.md` | Nacos 总体架构是什么？模块如何划分？ | 30 分钟 |
| 2 | `design/resource-model-spec.md` | Namespace / Group / Service / Instance 的层级关系？ | 20 分钟 |
| 3 | `design/compatibility-deprecation-spec.md` | API 的版本策略和废弃规则是怎样的？ | 15 分钟 |

---

### 第 2 层：基础能力（必读，理解运行时骨架）

这 11 篇描述了 Nacos 的"骨架"——不管你关心 config 还是 naming，它们都依赖这些基础机制。

| 顺序 | 文档 | 回答的核心问题 |
|------|------|---------------|
| 2.1 | `design/foundation-server-lifecycle-env-spec.md` | 服务端怎么启动？多上下文架构（Core/Web/Console）是什么？ |
| 2.2 | `design/foundation-cluster-membership-spec.md` | 集群节点如何互相发现？Member Lookup 有哪些方式？ |
| 2.3 | `design/foundation-remote-connection-spec.md` | gRPC 长连接怎么建？客户端怎么识别自己？ |
| 2.4 | `design/foundation-request-context-spec.md` | 请求过滤器链是怎样的？上下文怎么传递？ |
| 2.5 | `design/foundation-event-dispatch-spec.md` | **NotifyCenter 怎么工作？**（理解模块解耦的关键） |
| 2.6 | `design/foundation-task-execution-spec.md` | 定时任务怎么调度？TaskEngine 的机制？ |
| 2.7 | `design/foundation-persistence-dump-spec.md` | 数据怎么持久化？Dump 机制是什么？ |
| 2.8 | `design/foundation-internal-rpc-spec.md` | 集群节点间 RPC 怎么通信？ |
| 2.9 | `design/foundation-ap-consistency-spec.md` | Distro 协议怎么保证最终一致性？ |
| 2.10 | `design/foundation-cp-consistency-spec.md` | JRaft 怎么保证强一致性？什么场景用它？ |
| 2.11 | `design/foundation-observability-hooks-spec.md` | 可观测性钩子有哪些？怎么埋点？ |

> **重点提示**：2.5（NotifyCenter）和 2.4（请求过滤）读完后，其余基础能力可以跳着看，不需要逐字。

---

### 第 3 层：接口规范（根据你关注的方向选读）

| 你关心 | 读这些 |
|--------|-------|
| 服务端 API 怎么设计 | `http-api/api-spec.md` + `http-api/response-error-spec.md` + `http-api/authorization-spec.md` |
| HTTP API 完整清单 | `http-api/v3-api-surface.md`（v3 全部 API 列表） |
| gRPC 协议 | `grpc-api/api-spec.md` |
| Java 客户端怎么用 | `sdk/sdk-spec.md` |
| 客户端运行时行为 | `client/` 目录下 6 篇（client-runtime、连接与故障切换、能力协商、本地缓存与 Redo、运行时推送与重连） |

---

### 第 4 层：领域业务（核心，按需深入）

#### 配置管理（config）— 8 篇

| 顺序 | 文档 | 回答的问题 |
|------|------|-----------|
| 4a.1 | `config/config-spec.md` | 配置管理的总体设计 |
| 4a.2 | `config/config-resource-spec.md` | 配置模型：dataId、group、租户 |
| 4a.3 | `config/config-publish-query-spec.md` | 配置怎么发布和查询？ |
| 4a.4 | `config/config-listener-watch-spec.md` | 客户端怎么监听配置变更？ |
| 4a.5 | `config/config-gray-release-spec.md` | 灰度发布机制 |
| 4a.6 | `config/config-persistence-history-spec.md` | 配置历史版本和持久化 |
| 4a.7 | `config/config-consistency-dump-visibility-spec.md` | 配置如何保证一致性和可见性？ |

#### 服务发现（naming）— 10 篇

| 顺序 | 文档 | 回答的问题 |
|------|------|-----------|
| 4b.1 | `naming/naming-spec.md` | 服务发现的总体设计 |
| 4b.2 | `naming/naming-resource-spec.md` | 命名空间、服务、实例模型 |
| 4b.3 | `naming/naming-instance-lifecycle-spec.md` | 实例注册、注销、心跳 |
| 4b.4 | `naming/naming-discovery-subscription-spec.md` | 服务订阅和变更推送 |
| 4b.5 | `naming/naming-health-protection-spec.md` | 健康检查和保护阈值 |

---

### 第 5 层：扩展、安全、测试（选读）

| 方向 | 核心文档 |
|------|---------|
| 认证鉴权 | `auth/auth-permission-spec.md` + `auth/auth-plugin-spec.md` |
| 插件开发总览 | `plugin/plugin-spec.md`（总览） + 按需读子插件 spec |
| 数据源插件 | `plugin/datasource-dialect-plugin-spec.md` |
| 集成适配 | `integration/integration-adapter-spec.md` |
| 分布式锁 | `lock/lock-spec.md` |
| 编写测试 | `testing/api-integration-test-spec.md` |

---

### 规范阅读方法论

1. **每篇先看标题和首章概述**，快速判断是不是当前需要的，避免陷入细节
2. **不要逐字精读**，重点看状态机图、调用流程图和 API 定义表格
3. **对照 IDEA 中的代码**，用 Find Usages 验证规范中提到的类名和方法名
4. 规范中带 `**MUST**`、`**SHOULD**` 的条款是**强约束**，优先理解
5. 读完一个规范后，用一句话总结它的核心职责，写在文档开头空白处

---

## 第二步：跟着启动流程读代码（代码切入点）

从 `NacosBootstrap.main()` 开始，这是所有模块的交汇点。

### 2.1 启动调用链

```
bootstrap/NacosBootstrap.main()
  │
  ├── startCoreContext()
  │     └── core/NacosServerBasicApplication     ← 基础层：集群、一致性、持久化
  │
  ├── startServerWebContext()
  │     └── server/NacosServerWebApplication     ← 服务层：config + naming 业务
  │
  ├── startConsoleContext()
  │     └── console/NacosConsole                  ← 控制台层：Web UI 后端
  │
  └── startAiRegistryContext()
        └── ai-registry-adaptor/NacosAiRegistry  ← AI 注册中心（可选）
```

### 2.2 启动阶段与关键类

| 阶段 | 触发 | 关键操作 |
|------|------|---------|
| `starting()` | `StartingApplicationListener` | 标记启动阶段 |
| `environmentPrepared()` | 同上 | 创建工作目录、加载预置属性、初始化系统变量 |
| `contextPrepared()` | 同上 | 打印启动日志 |
| `contextLoaded()` | 同上 | 自定义环境配置 |
| `started()` | 同上 | 启动完成回调 |

---

## 第三步：分模块深入

### 3.1 模块职责速览

| 模块 | 核心入口类 | 职责 |
|------|-----------|------|
| **common** | `NotifyCenter`、`NacosServiceLoader` | 事件总线、SPI 加载机制、工具类 |
| **core** | `NacosStartUpManager`、`StartingApplicationListener` | 启动编排、集群成员管理、分布式一致性 |
| **config** | V3 Controller（`config/src/main/java/.../controller/`） | 配置的增删改查、灰度发布、监听推送 |
| **naming** | `InstanceControllerV3`、`ServiceControllerV3` | 服务注册、服务发现、实例健康检查 |
| **auth** | `NacosAuthConfig`、`AuthPlugin` | 认证鉴权、JWT Token、RBAC 权限 |
| **persistence** | `ExternalDataSourceServiceImpl` | 多数据库支持（Derby、MySQL、PostgreSQL） |
| **consistency** | `JRaftProtocol`、`DistroProtocol` | CP 一致性（JRaft）+ AP 一致性（Distro） |
| **api** | gRPC proto 定义、SDK 接口 | 客户端-服务端通信协议 |
| **client** | `NacosClientConfig`、`NamingClient` | Java SDK 实现（JDK 8 兼容） |
| **console** | Console 后端 Controller | 控制台 API |
| **plugin** | `NacosServiceLoader` SPI | 插件接口定义 |
| **plugin-default-impl** | 各插件默认实现 | 鉴权、数据源、配置变更、加密等默认插件 |
| **sys** | `EnvUtil`、文件监听 | 系统环境、JVM 参数管理 |
| **lock** | 分布式锁实现 | 基于一致性协议的分布式锁 |

### 3.2 核心架构模式：SPI + 事件驱动

Nacos 最核心的两个机制：

**SPI 插件加载**：

```java
// 通过 NacosServiceLoader 动态加载接口实现
NacosServiceLoader.load(NacosStartUp.class)      // 加载所有启动阶段实现
NacosServiceLoader.load(NacosApplicationListener.class) // 加载所有监听器
NacosServiceLoader.load(DataSourceService.class)  // 加载数据源实现
```

**NotifyCenter 事件总线**：

```java
// 发布事件
NotifyCenter.publishEvent(new ConfigChangeEvent(...));

// 订阅事件
NotifyCenter.registerSubscriber(new Subscriber() { ... });
```

理解这两个机制后，各模块间的解耦通信一目了然。

---

## 第四步：关键场景走读

选一个你最感兴趣的业务场景，端到端走一遍完整链路。

### 4.1 场景：客户端发布一条配置

```
客户端 SDK                         服务端
─────────────────────────────────────────────────
NacosConfigService.publishConfig()
  → gRPC ConfigPublishRequest ──→ ConfigControllerV3
                                    → ConfigOperateService
                                      → ConfigInfoPersistService  (持久化)
                                      → NotifyCenter.publishEvent(ConfigChangeEvent)
                                        → ConfigChangePublisher
                                          → gRPC Push 通知订阅者
```

### 4.2 场景：客户端注册一个服务实例

```
客户端 SDK                         服务端
─────────────────────────────────────────────────
NamingClient.registerInstance()
  → gRPC InstanceRegRequest ──→ InstanceControllerV3
                                  → InstanceOperator
                                    → DistroProtocol.sync()  (AP 协议同步)
                                    → NotifyCenter (推送订阅者)
```

### 4.3 场景：Nacos 集群成员变更

```
Node A 加入集群
  → ClusterMemberManager.memberJoin()
    → NotifyCenter.publishEvent(MemberChangeEvent)
      → JRaftProtocol → Raft 集群拓扑更新
      → DistroProtocol → Distro 任务重新分配
```

---

## 建议的 10 天学习计划

| 天 | 内容 | 预计 |
|----|------|------|
| 第 1 天 | 第 1 层：顶层设计 3 篇 + 第 2 层基础能力前 5 篇 | 3 小时 |
| 第 2 天 | 第 2 层基础能力后 6 篇，完成运行时骨架理解 | 2 小时 |
| 第 3 天 | 第 3 层：HTTP API + gRPC 规范 | 1.5 小时 |
| 第 4 天 | 第 4 层：config 领域前 4 篇 | 2 小时 |
| 第 5 天 | 第 4 层：config 领域后 3 篇 | 1.5 小时 |
| 第 6 天 | 第 4 层：naming 领域前 4 篇 | 2 小时 |
| 第 7 天 | 第 4 层：naming 领域后几篇 | 1.5 小时 |
| 第 8 天 | 第 5 层：auth 认证鉴权 | 1.5 小时 |
| 第 9 天 | 第 5 层：plugin 插件系统 | 1.5 小时 |
| 第 10 天 | 对着地图开始看代码，用 Find Usages 验证你的理解 | 不限 |

---

## 附：IDEA 启动配置

参照 `IDEA启动Nacos指南.md`，已包含完整的 VM Options 配置，启动成功后访问：

- **服务端 API**：`http://localhost:8848/nacos`
- **新版控制台**：`http://localhost:8080/next`
- **默认账号**：`nacos` / `nacos`
