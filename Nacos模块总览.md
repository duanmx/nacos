# Nacos v3 模块总览

> 从源代码目录结构出发，将 32 个模块分为 8 个层次，逐一说明职责。

---

## 1. 入口与编排层

### `bootstrap`
**一句话：整个 Nacos 的 `main()` 函数。**

只有一个核心类 `NacosBootstrap`。职责是按顺序启动多个 Spring Context：

```
Core Context → Web（Config+Naming）Context → Console Context
```

启动顺序由 `NacosStartUpManager` 管理，不允许跳过阶段。

### `server`
**一句话：Bean 分流机制的核心，两个 Spring Boot 入口的所在地。**

通过 `@ComponentScan` + `TypeFilter` 把同一个 classpath 上的类分派到不同 Context：

| 入口类 | 对应 Context | 过滤器 |
|------|:---:|------|
| `NacosServerBasicApplication` | CoreContext | `NacosWebBeanTypeFilter`：排除 Controller → 只留 Service |
| `NacosServerWebApplication` | WebContext | `NacosNormalBeanTypeFilter`：排除非 Controller → 只留 Web 层 |

依赖 naming、config、istio、prometheus 和默认插件包；同时通过 `NacosDuplicateBeanPostProcessor` 机制防止父子 Context 出现重复 Bean。

---

## 2. 核心业务层 —— 两个拳头产品

### `config`
**一句话：配置中心。**

处理"配置"的完整生命周期：
- 配置的增删改查 —— 用户在控制台写的配置如何落地到数据库
- 配置变更推送 —— 配置改了，如何通知所有订阅的客户端（长轮询或 gRPC push）
- 配置历史与灰度 —— 谁改了什么、回滚、灰度发布
- 配置监听注册 —— 客户端说"我要监听这个配置"，服务端如何记住它

核心入口：`ConfigController`、`ConfigServiceV2Impl`

### `naming`
**一句话：服务发现与注册中心。**

处理"服务实例"的完整生命周期：
- 实例注册 —— 微服务启动后，将自己的 IP:Port 注册到 Nacos
- 实例发现 —— 别的服务来查询 `order-service` 的地址列表
- 健康检查 —— 实例是否存活、心跳检测
- 服务同步 —— 集群内多个 Nacos 节点之间如何同步实例数据（Distro 协议）

核心入口：`InstanceController`、`InstanceOperatorImpl`

---

## 3. 核心基础设施层 —— 支撑业务运转

### `core`
**一句话：集群和命名空间管理。**

不处理业务数据，只管服务器自身：
- 集群成员管理 —— 哪些 Nacos 节点组成集群，谁活着谁挂了
- 命名空间（namespace）—— 如何隔离 dev/test/prod 环境
- 启动生命周期 —— `NacosStartUpManager` 记录每个启动阶段
- `DistroClientDataProcessor` —— Distro 协议的客户端数据处理器

### `consistency`
**一句话：一致性协议的实现。**

Nacos 最"硬核"的模块，实现了两种一致性模型：

| 协议 | 类型 | 用途 | 场景 |
|------|------|------|------|
| **JRaft** | CP（强一致） | 选主 + 日志复制 | 持久化服务的实例数据、配置变更 |
| **Distro** | AP（最终一致） | 对等节点间数据同步 | 临时实例的注册信息 |

关键类：`PersistentConsistencyServiceDelegateImpl`（CP 门面）、`EphemeralConsistencyServiceDelegateImpl`（AP 门面）

### `persistence`
**一句话：数据存哪里。**

抽象底层数据库差异，支持三种数据库：

| 数据库 | 场景 |
|--------|------|
| **Derby** | 嵌入式，单机模式默认 |
| **MySQL** | 生产环境集群 |
| **PostgreSQL** | 生产环境替代 |

关键类：`ExternalDataSourceProperties`（外部数据库配置）、`DatasourceConfiguration`（自动选择内嵌还是外联）

### `auth`
**一句话：谁可以用什么功能。**

认证（你是谁）和授权（你能干什么）：
- JWT token 签发与校验
- `@Secured` 注解的拦截处理
- 用户、角色、权限的增删改查
- 身份密钥配置（`nacos.core.auth.server.identity.key/value`）

---

## 4. 客户端层 —— 给 Java 开发者用的 SDK

### `api`
**一句话：接口契约定义。**

包含两部分：
1. **Java 接口** —— `ConfigService`、`NamingService` 等客户端要调用的方法声明
2. **gRPC proto 文件** —— `api/src/main/proto/` 下的 `.proto`，定义客户端和服务端通信的消息格式，编译后生成 Java 类

### `client`
**一句话：完整的客户端 SDK 实现。**

当你调用 `NacosFactory.createConfigService()` 时，实际运行的就是这里的代码：
- gRPC 连接管理（和服务端保持长连接）
- 配置监听与缓存（本地缓存一份，变更时自动更新）
- 服务发现与负载均衡

### `client-basic`
**一句话：客户端基础工具。**

从 `client` 中抽出的底层设施——连接重试、心跳、地址解析。单独成模块是为了避免循环依赖，让其他模块也能复用。

---

## 5. 公共设施层 —— 被所有人依赖

### `common`
**一句话：全项目共享的工具箱。**

- **`NotifyCenter`** —— 事件总线，模块之间靠事件解耦。比如 config 改了配置，发事件通知其他模块
- **HTTP 客户端** —— 发起 HTTP 请求的工具封装
- **SPI 加载器** —— `NacosServiceLoader`，插件机制的底层支持
- **工具类** —— JSON 处理、字符串、编码等

### `sys`
**一句话：系统环境工具。**

JVM 参数解析、操作系统信息获取。当 Nacos 需要知道自己运行在什么环境时，问这个模块。

### `lock`
**一句话：分布式互斥锁。**

多个 Nacos 节点同时要修改同一份数据时，需要一把分布式锁来保证串行执行。

---

## 6. Web 控制台层 —— 你看到的界面

### `console`
**一句话：Web 后端的 Spring Boot 应用。**

处理前端页面的 HTTP 请求。API 按路径前缀分为三类：

| 前缀 | 用途 | 示例 |
|------|------|------|
| `/v3/client/...` | 开放 API（给 SDK 调用） | `/v3/client/ns/instance` |
| `/v3/admin/...` | 管理 API（给控制台页面用） | `/v3/admin/ns/service` |
| `/v3/console/...` | 控制台专用 API | `/v3/console/cs/config` |

### `console-ui`（老版前端）
### `console-ui-next`（新版前端）
**一句话：前端代码。**

React + TypeScript，新版用 Vite 构建。你浏览器里看到的 Nacos 管理页面代码就在这里。

---

## 7. 插件体系层 —— 可扩展能力

### `plugin`
**一句话：插件接口定义（SPI 契约）。**

只定义接口，不写实现。子目录按功能拆分：

| 子模块 | 插件类型 |
|--------|---------|
| `auth` | 鉴权插件 |
| `datasource` | 数据源方言插件 |
| `encryption` | 加密插件 |
| `trace` | 链路追踪插件 |
| `config` | 配置变更插件 |
| `control` | 控制插件 |
| `ai` | AI 管道/存储插件 |
| `visibility` | 可见性插件 |
| `environment` | 环境定制插件 |

### `plugin-default-impl`
**一句话：上面那些插件的默认实现。**

即插即用，按需引入：

| 模块 | 用途 |
|------|------|
| `nacos-default-auth-plugin` | 默认用户名密码认证 |
| `nacos-ldap-auth-plugin` | LDAP 认证 |
| `nacos-oidc-auth-plugin` | OIDC 认证 |
| `nacos-default-datasource-plugin` | Derby/MySQL/PostgreSQL 数据源实现 |
| `nacos-default-control-plugin` | 默认控制插件 |
| `nacos-default-ai-importer-plugin` | 默认 AI 导入插件 |
| `nacos-default-ai-pipeline-plugin` | 默认 AI 管道插件 |
| `nacos-default-ai-trace-plugin` | 默认 AI 追踪插件 |
| `nacos-default-plugin-all` | 全量聚合包 |

---

## 8. 集成与辅助层

| 模块 | 一句话职责 |
|------|-----------|
| **`ai`** | AI Agent 管理——Prompt、MCP Server、A2A Agent 的注册与发现 |
| **`copilot`** | AI Copilot 能力集成 |
| **`ai-registry-adaptor`** | 将 AI 资源（Prompt/Skill 等）适配到 Nacos 的注册模型 |
| **`istio`** | 和 Istio Service Mesh 对接 |
| **`k8s-sync`** | 同步 Kubernetes 的 Service/Endpoint 到 Nacos |
| **`prometheus`** | 暴露 Prometheus 监控指标 |
| **`cmdb`** | CMDB（配置管理数据库）集成 |
| **`address`** | 地址服务器——客户端先问它"Nacos 集群地址在哪"，再连接真正的集群 |
| **`maintainer-client`** | 运维客户端——内部维护工具 |
| **`distribution`** | 打包分发包——`bin/` 启动脚本、`conf/` 配置文件都在这里 |
| **`example`** | 示例代码，学习 SDK 用法的入口 |
| **`test`** | 集成测试（openapi-test、java-sdk-test、maintainer-sdk-test） |
| **`logger-adapter-impl`** | 日志适配器——让 Nacos 兼容 logback 和 log4j2 |

---

## 推荐阅读顺序

沿着启动链走，是最自然的代码阅读路径：

```
1. bootstrap  →  NacosBootstrap.main() 是万物起点
       ↓
2. server    →  两个入口类如何用 TypeFilter 分流 Bean 到不同 Context
       ↓
3. core      →  启动生命周期 NacosStartUpManager、集群管理
       ↓
4. persistence → 数据源配置，理解 Derby vs MySQL 的选择逻辑
       ↓
5. naming    →  服务注册、实例发现、Distro 协议同步
       ↓
6. consistency → 深入 JRaft 和 Distro 的实现细节
       ↓
7. config    →  配置管理、变更推送、长轮询机制
       ↓
8. common    →  NotifyCenter 事件总线如何解耦各模块
       ↓
9. client    →  客户端 SDK 如何与服务端交互
```

这条路径覆盖了 Nacos 最核心的链路，理解完这些后，再根据兴趣扩展到 auth、plugin、ai 等模块。
