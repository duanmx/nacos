# Nacos 鉴权模块深度解析

> **定位**：Nacos 的 Auth 模块是一个**双通道、可插拔的鉴权框架**——HTTP 走 Servlet Filter 模板方法链，gRPC 走 RequestFilter 责任链，两者共享同一套 SPI 可替换的认证后端。

---

## 一、为什么需要分层鉴权框架？

### 问题场景

Nacos 同时暴露 HTTP API 和 gRPC API，两种协议的请求结构完全不同：

```
  HTTP 请求                          gRPC 请求
  ┌──────────────────┐              ┌──────────────────┐
  │ GET /v3/client/ns │              │ ConnectionSetup  │
  │ Header: Identity  │              │ Header: Identity │
  │ Body: JSON        │              │ Body: Protobuf   │
  └──────────────────┘              └──────────────────┘
```

如果为两种协议各写一套鉴权代码，会出现大量重复逻辑。同时，Nacos 支持多种认证系统（内置 nacos、LDAP、OIDC），不能在 Filter 里硬编码认证逻辑。

### 解决方案

**三层抽象**：

1. **协议适配层**（HttpProtocolAuthService / GrpcProtocolAuthService）—— 把不同协议的请求解析成统一的 `Resource` + `IdentityContext`
2. **SPI 认证后端层**（AuthPluginService）—— 可插拔的认证实现（nacos / ldap / oidc）
3. **Filter 路由层**（AbstractWebAuthFilter 模板方法 / AbstractRequestFilter 责任链）—— 决定哪些 API 需要鉴权、怎么鉴权

---

## 二、整体架构

核心思想：**Filter 负责"要不要鉴权、如何拒绝"，ProtocolAuthService 负责"怎么解析请求"，AuthPluginService 负责"怎么验证身份"**。三层各司其职，通过接口解耦。

### 2.1 数据流转图（全貌）

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           HTTP 请求（Servlet Filter）                        │
│                                                                            │
│  AuthConfig @Configuration  L36-L53                                        │
│    ├─ AuthFilter       → urlPatterns="/*", order=6, 处理非 ADMIN_API        │
│    └─ AuthAdminFilter  → urlPatterns="/*", order=6, 处理 ADMIN_API          │
│         │                                                                  │
│         │ 每个 HTTP 请求进入 FilterChain                                     │
│         ▼                                                                  │
│  AbstractWebAuthFilter.doFilter()  L71-L146                                 │
│    │                                                                       │
│    ├─ Step 1: methodsCache.getMethod(req)          ← 反射找 Handler 方法     │
│    ├─ Step 2: 无 @Secured 注解 → chain.doFilter()    放行                    │
│    ├─ Step 3: isMatchFilter(secured)   AuthFilter: !ADMIN_API               │
│    │                                     AuthAdminFilter: ADMIN_API         │
│    ├─ Step 4: isAuthEnabled()           ← NacosAuthConfig.isAuthEnabled()   │
│    ├─ Step 5: checkServerIdentity()     ← 服务间身份白名单                    │
│    ├─ Step 6: parseResource + parseIdentity + validateIdentity              │
│    │           → HttpProtocolAuthService                                    │
│    ├─ Step 7: 仅身份 API (tag=ONLY_IDENTITY) → 跳过权限校验                  │
│    └─ Step 8: validateAuthority         ← AuthPluginService SPI             │
│                                                                            │
├────────────────────────────────────────────────────────────────────────────┤
│                           gRPC 请求（RequestFilter）                          │
│                                                                            │
│  RemoteRequestAuthFilter @Component  L50-L143                               │
│    │                                                                       │
│    ├─ @PostConstruct init() → requestFilters.registerFilter(this)  L44-L47 │
│    └─ RequestHandler.handleRequest()  L45-L56                               │
│          for (AbstractRequestFilter filter : filters)                       │
│            if filterResult != null && !success → return error               │
│         │                                                                  │
│         ├─ 无 @Secured → 放行                                                │
│         ├─ INNER_API + !innerApiAuthEnabled → 放行（升级兼容）                │
│         ├─ checkServerIdentity()         → GrpcProtocolAuthService          │
│         ├─ parseResource + parseIdentity → GrpcProtocolAuthService          │
│         └─ validateIdentity + validateAuthority → AuthPluginService         │
│                                                                            │
└───────────────────────────────────┬────────────────────────────────────────┘
                                    │ 委托
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                     AbstractProtocolAuthService（协议适配 + SPI 委托）        │
│                                                                            │
│  抽象基类  L47                                                               │
│    ├─ HttpProtocolAuthService   ——  从 HttpServletRequest 解析身份/资源      │
│    └─ GrpcProtocolAuthService  ——  从 gRPC Request 解析身份/资源             │
│         │                                                                  │
│         │ delegate to SPI                                                   │
│         ▼                                                                  │
│  AuthPluginManager.getInstance()                                            │
│    .findAuthServiceSpiImpl(authConfig.getNacosAuthSystemType())             │
│         │                                                                  │
│         ├─ nacos  → NacosAuthPluginService                                 │
│         ├─ ldap   → LdapAuthPluginService                                  │
│         └─ oidc   → OidcAuthPluginService                                  │
│                                                                            │
└───────────────────────────────────┬────────────────────────────────────────┘
                                    │ 读配置
                                    ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                     NacosAuthConfigHolder（SPI 配置中枢）                      │
│                                                                            │
│  NacosServiceLoader.load(NacosAuthConfig.class)  L38                        │
│    │                                                                       │
│    ├─ NacosServerAuthConfig       scope=OPEN_API    nacos.core.auth.enabled │
│    ├─ NacosServerAdminAuthConfig  scope=ADMIN_API   nacos.core.auth.admin   │
│    └─ NacosConsoleAuthConfig      scope=CONSOLE_API (console 模块)          │
│         │                                                                  │
│         按 scope 注册到 Map<String, NacosAuthConfig>                         │
│                                                                            │
│  AbstractDynamicConfig.getConfigFromEnv()                                   │
│    └─ EnvUtil.getProperty("nacos.core.auth.enabled", ...)                  │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

**分层解读**：

| 层 | 组件 | 职责 |
|----|------|------|
| Filter 路由层 | `AuthFilter` / `AuthAdminFilter` / `RemoteRequestAuthFilter` | 决定哪些 API 需要鉴权，拦截并返回 403 |
| 协议适配层 | `HttpProtocolAuthService` / `GrpcProtocolAuthService` | 从不同协议的请求中解析出统一的 `Resource` + `IdentityContext` |
| SPI 认证层 | `AuthPluginService`（nacos / ldap / oidc） | 实际的身份验证和权限校验 |
| 配置层 | `NacosAuthConfig` 三实现 | 提供 scope、启停标志、认证系统类型、服务间身份密钥 |

### 2.2 类关系图（UML）

```
                          ┌─────────────────────────────┐
                          │        <<interface>>        │
                          │       jakarta.servlet.Filter │
                          └──────────────┬──────────────┘
                                         △  实现
                          ┌──────────────┴──────────────┐
                          │  <<abstract>>               │
                          │  AbstractWebAuthFilter      │
                          │  ───────────────────────────│
                          │  + doFilter()    ← 模板方法  │
                          │  # isAuthEnabled()  ← 抽象   │
                          │  # isMatchFilter()  ← 钩子   │
                          │  # checkServerIdentity()     │
                          └──────────────┬──────────────┘
                                         △  继承
                   ┌──────────┬──────────┴──────────┬──────────┐
                   │          │                     │          │
      ┌────────────────┐ ┌───────────────┐ ┌─────────────────────────┐
      │   AuthFilter    │ │AuthAdminFilter│ │NacosConsoleAuthFilter   │
      │  !ADMIN_API     │ │   ADMIN_API   │ │  (console 模块)          │
      │  覆盖身份校验     │ │               │ │  跳过 server identity    │
      └───────┬────────┘ └───────┬───────┘ └────────────┬────────────┘
              │                  │                       │
              │  持有             │  持有                  │  持有
              ▼                  ▼                       ▼
  ┌───────────────────────────────────────────────────────────────────────┐
  │                    NacosAuthConfig <<interface>>                       │
  │  + getAuthScope() : String                                            │
  │  + isAuthEnabled() : boolean                                          │
  │  + getNacosAuthSystemType() : String                                  │
  │  + getServerIdentityKey/Value()                                       │
  └───────────────────────────────┬───────────────────────────────────────┘
                                  △  实现
          ┌───────────────────────┼───────────────────────────┐
          │                       │                           │
  ┌───────────────────┐ ┌────────────────────┐ ┌──────────────────────────┐
  │NacosServerAuth    │ │NacosServerAdmin    │ │NacosConsoleAuthConfig    │
  │Config             │ │AuthConfig          │ │extends Abstract-         │
  │scope=OPEN_API     │ │scope=ADMIN_API     │ │DynamicConfig              │
  │extends Abstract-  │ │extends Abstract-   │ │                          │
  │DynamicConfig      │ │DynamicConfig       │ │                          │
  └───────────────────┘ └────────────────────┘ └──────────────────────────┘
                    △                    △
                    │ 继承                │ 继承
          ┌─────────┴─────────┐        （同上）
          │ AbstractDynamicConfig
          │  + getConfigFromEnv()  ← 抽象
          │  + resetConfig()
          └─────────────────────┘

  ═══════════════════════════════════════════════════════════════════════
  gRPC 路径（独立的 Filter 层级）

  ┌─────────────────────────────────┐
  │   <<abstract>>                  │
  │   AbstractRequestFilter         │
  │  ────────────────────────────── │
  │  @PostConstruct init()          │
  │   → requestFilters.register()   │
  │  # filter(Request,Meta,Class)   │  ← 抽象方法
  └───────────────┬─────────────────┘
                  △  继承
    ┌─────────────┼─────────────────────────┐
    │             │                         │
  ┌─┴──────────────────┐  ┌──────────────────┴────────────┐
  │RemoteRequestAuth   │  │NamespaceValidationRequestFilter│
  │Filter              │  │TpsControlRequestFilter         │
  │(gRPC 鉴权)          │  │RemoteParamCheckFilter          │
  └────────────────────┘  └───────────────────────────────┘
             │
             │ 委托
             ▼
  GrpcProtocolAuthService → AuthPluginService SPI

  ═══════════════════════════════════════════════════════════════════════
  图例：
    ────▶  委托/调用
    ──△    继承/实现（子类指向父类）
    <<X>>  构造型标注
```

### 2.3 用了哪些设计模式？为什么这么设计？

| 模式 | 体现在哪 | 解决什么问题 | 好处 | 坏处 |
|------|---------|-------------|------|------|
| **模板方法** | `AbstractWebAuthFilter.doFilter()` L71-L146 定义 8 步骨架，`isAuthEnabled()` 抽象，`isMatchFilter()` / `checkServerIdentity()` 为可重写钩子 | OPEN_API 和 ADMIN_API 的鉴权流程完全一致，只有"管谁"和"启停开关"不同 | 骨架复用，两个子类各约 20 行 | 骨架内步骤耦合强，新增步骤（如增加 IP 白名单）需改基类 |
| **策略/路由** | `AuthFilter.isMatchFilter()` 返回 `!ADMIN_API`，`AuthAdminFilter.isMatchFilter()` 返回 `ADMIN_API`，互斥分工 | 不同 API 类型用不同 Filter 实例处理，避免一个 Filter 内部大量 if-else | 职责单一，新增 API 类型只需加新子类 | 两个 Filter 都注册 `/*`，每次请求都会触发两个 Filter，靠运行时 `isMatchFilter` 跳过一个，有性能浪费 |
| **SPI 插件化（双层）** | 第一层：`NacosAuthConfigHolder` 通过 `NacosServiceLoader` 发现所有 `NacosAuthConfig`；第二层：`AuthPluginManager` 发现 `AuthPluginService`（nacos/ldap/oidc） | 配置可分散在不同模块（core/console），认证系统可热插拔 | 新增模块只需加 SPI 文件；切换认证系统只改配置 | 双层 SPI 学习成本高，新人容易混淆两个 SPI 的职责边界 |
| **责任链** | `AbstractRequestFilter.init()` L44-L47 自注册到 `RequestFilters`，`RequestHandler.handleRequest()` L45-L56 逐个迭代 | gRPC 请求需依次经过鉴权、限流、参数校验等多个过滤器 | 每个 Filter 只做一件事，可独立替换 | 过滤器顺序由 `@PostConstruct` 隐式决定，无显式优先级声明 |
| **门面模式** | `AbstractProtocolAuthService` 封装了 `AuthPluginManager` + `ServerIdentityChecker` + `AuthPluginService` 的复杂协作 | Filter 不需要知道认证系统是 nacos 还是 ldap，只调 `validateIdentity()` | 调用方代码清晰，切换认证系统零改动 | 调用链深：Filter → ProtocolAuthService → AuthPluginManager → AuthPluginService |
| **单例 + SPI 注册表** | `NacosAuthConfigHolder.getInstance()` + `AuthPluginManager.getInstance()` | 全局共享同一套配置和认证后端 | 避免重复加载 SPI；配置一致 | 单例持有全局状态，难 mock；单例初始化顺序依赖 SPI 文件存在 |

### 2.4 整体设计的优点

- **双协议统一后端**：HTTP 走 Servlet Filter，gRPC 走 RequestFilter，两条路径共用 `ProtocolAuthService` + `AuthPluginService`，认证逻辑不重复
- **认证系统可插拔**：通过 `AuthPluginManager` SPI 支持 nacos / ldap / oidc，切换只需改 `nacos.core.auth.system.type` 一行配置
- **API 类型精细控制**：四种 API 类型（OPEN / ADMIN / CONSOLE / INNER）各自独立的启停开关、身份校验策略
- **升级平滑**：`InnerApiAuthEnabled` 定时检查集群版本，升级完成前内网 API 保持与 2.x 兼容

### 2.5 潜在问题

- **运行时 isMatchFilter 分流**：两个 Filter 都拦截 `/*`，每个请求都触发两次 doFilter，再靠 `isMatchFilter()` 跳过——本质上"先拦截再判断"，不如按路径前缀分流高效
- **双层 SPI 混淆风险**：`NacosAuthConfig` SPI（配置在哪、启停）和 `AuthPluginService` SPI（怎么认证）职责不同但命名相近，新人容易混淆
- **两个独立启停开关**：OPEN_API 默认不启用鉴权，ADMIN_API 默认启用——运维必须理解这两个开关的差异，误关 ADMIN 鉴权会导致安全问题
- **gRPC Filter 顺序不可控**：`AbstractRequestFilter.init()` 通过 `@PostConstruct` 自注册，执行顺序取决于 Spring 初始化顺序

---

## 三、核心概念

### 3.1 四种 API 类型（ApiType）

| 类型 | 鉴权 Filter | 默认启停 | 用途 |
|------|------------|---------|------|
| `OPEN_API` | AuthFilter | 不启用（`nacos.core.auth.enabled`=false） | 客户端 SDK 调用的 API（服务发现、配置获取） |
| `ADMIN_API` | AuthAdminFilter | 启用（`nacos.core.auth.admin.enabled`=true） | 管理后台 API（创建服务、修改配置） |
| `CONSOLE_API` | NacosConsoleAuthFilter | — | 控制台 UI 调用的 API |
| `INNER_API` | AuthFilter（兼容逻辑） | 版本检查后启用 | 集群内部 RPC 调用 |

### 3.2 @Secured 注解

每个需要鉴权的 API 方法上标注：

```java
@Secured(action = ActionTypes.READ,   // READ 或 WRITE
         signType = SignType.CONFIG,  // CONFIG / NAMING / AI
         resource = "config",         // 资源名（可选，不填则从请求参数动态解析）
         apiType = ApiType.ADMIN_API, // API 类型
         tags = {"operation=delete"}) // 自定义标签，注入到 Resource.properties
```

Filter 会反射获取方法的 `@Secured` 注解，提取其中的 `action`、`signType`、`resource`、`apiType`、`tags` 用于鉴权决策。

### 3.3 Nacos 中的"资源"具体是什么

Nacos 管理的三类实体就是"资源"——鉴权插件最终控制的就是"谁能对这些实体做什么"：

| 模块 | 资源是什么 | Resource 对象 | 举例 |
|------|----------|--------------|------|
| Config | 一条配置 | `{type=CONFIG, namespaceId, group, resourceName}` | `dev/DEFAULT_GROUP/application.yml` |
| Naming | 一个服务 | `{type=NAMING, namespaceId, group, resourceName}` | `prod/DEFAULT_GROUP/user-service` |
| AI | Prompt / Skill / Agent / MCP Server | `{type=AI, namespaceId, resourceName}` | `dev/chat-assistant` |

**`resource()` 字段的作用**：
- **填写了**：静态资源名，`parseSpecifiedResource()` 直接使用
- **不填（默认空）**：运行时由 `signType` 对应的 Parser（如 `ConfigHttpResourceParser`、`AiHttpResourceParser`）从请求 URL 参数中动态提取

```java
// HttpProtocolAuthService.parseResource() 的分流逻辑
if (StringUtils.isNotBlank(secured.resource())) {
    return parseSpecifiedResource(secured);           // 静态资源
}
return resourceParserMap.get(signType).parse(request, secured);  // 动态解析
```

### 3.4 ServerIdentity（服务间身份）

Nacos 集群内部通信时，节点之间通过固定 key-value 头互相识别，不走完整的用户认证流程。

配置项：
```properties
nacos.core.auth.server.identity.key=serverIdentity
nacos.core.auth.server.identity.value=security
```

校验逻辑在 `ServerIdentityChecker.check()`：如果请求携带的身份头与配置匹配，直接放行。

---

## 四、数据流转全景

```
┌──────────────────── 启动阶段 ────────────────────┐
│                                                    │
│  NacosAuthConfigHolder 初始化  L36-L41             │
│    ├─ NacosServiceLoader.load(NacosAuthConfig)     │
│    │   ├─ NacosServerAuthConfig()                  │
│    │   │     └─ getConfigFromEnv()                 │
│    │   │          ├─ authEnabled = false (默认)     │
│    │   │          ├─ nacosAuthSystemType = ""      │
│    │   │          └─ serverIdentity key/value       │
│    │   ├─ NacosServerAdminAuthConfig()             │
│    │   │     └─ authEnabled = true (默认)           │
│    │   └─ NacosConsoleAuthConfig()（console 模块）   │
│    └─ 按 scope 注册到 Map                           │
│                                                    │
│  AuthConfig（@Configuration）                       │
│    ├─ authFilter(authConfig, methodsCache)          │
│    │     → new AuthFilter(OPEN_API config)          │
│    ├─ authAdminFilter(authConfig, methodsCache)     │
│    │     → new AuthAdminFilter(ADMIN_API config)    │
│    └─ 注册为 FilterRegistrationBean（/*, order=6）  │
│                                                    │
│  RemoteRequestAuthFilter（@Component）               │
│    └─ @PostConstruct → self-register to RequestFilters│
│                                                    │
├──────────────────── HTTP 请求处理 ─────────────────┤
│                                                    │
│  请求到达 Nacos Server                              │
│    └─ FilterChain                                  │
│         ├─ AuthFilter.doFilter()                    │
│         │    ├─ getMethod(req)                     │
│         │    ├─ 无 @Secured → 放行                  │
│         │    ├─ isMatchFilter → ADMIN_API → 跳过    │
│         │    ├─ OPEN/CONSOLE/INNER API → 鉴权       │
│         │    └─ 鉴权流程（8 步）                     │
│         └─ AuthAdminFilter.doFilter()               │
│              ├─ isMatchFilter → !ADMIN_API → 跳过   │
│              └─ ADMIN_API → 鉴权                    │
│                                                    │
├──────────────────── gRPC 请求处理 ─────────────────┤
│                                                    │
│  gRPC 请求到达                                      │
│    └─ RequestHandler.handleRequest()               │
│         ├─ RemoteRequestAuthFilter.filter()         │
│         │    ├─ 无 @Secured → null（放行）           │
│         │    ├─ INNER_API + 未启用 → null            │
│         │    ├─ checkServerIdentity                 │
│         │    └─ validateIdentity + validateAuthority│
│         ├─ NamespaceValidationRequestFilter         │
│         ├─ TpsControlRequestFilter                  │
│         └─ RemoteParamCheckFilter                   │
│                                                    │
└────────────────────────────────────────────────────┘
```

---

## 五、核心类详解

### 5.1 AbstractWebAuthFilter

**定位**：HTTP 鉴权的模板方法基类，实现 `jakarta.servlet.Filter`。

**8 步鉴权管道**（[源码 L71-L146](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/auth/AbstractWebAuthFilter.java#L71-L146)）：

```
doFilter(request, response, chain)
  ├─ Step 1: methodsCache.getMethod(req)        ← 反射找到 Handler 方法
  ├─ Step 2: !@Secured → chain.doFilter()        ← 无注解的 API 不放行
  ├─ Step 3: !isMatchFilter(secured) → skip      ← 子类决定是否处理此 API 类型
  ├─ Step 4: !isAuthEnabled() → chain.doFilter() ← 鉴权总开关关闭则放行
  ├─ Step 5: checkServerIdentity()               ← 服务间身份白名单
  │    ├─ FAIL → 403
  │    └─ MATCHED → 直接放行
  ├─ Step 6: parseResource + parseIdentity + validateIdentity
  │    ├─ 失败 → 403
  │    └─ 成功 → 继续
  ├─ Step 7: ONLY_IDENTITY tag → chain.doFilter() ← 仅需身份不需权限
  └─ Step 8: validateAuthority
       ├─ 失败 → 403
       └─ 成功 → chain.doFilter()
```

**子类必须实现**：`isAuthEnabled()` —— 是否启用鉴权

**子类可选重写**：`isMatchFilter()`（默认 true）、`checkServerIdentity()`（默认委托给 protocolAuthService）

### 5.2 AuthFilter vs AuthAdminFilter

| | AuthFilter | AuthAdminFilter |
|---|---|---|
| **处理的 API** | OPEN_API / CONSOLE_API / INNER_API | ADMIN_API |
| **isMatchFilter** | `!ADMIN_API` | `ADMIN_API` |
| **checkServerIdentity** | INNER_API + 未升级 → 跳过校验 | 默认委托 |
| **持有 NacosAuthConfig** | NacosServerAuthConfig (scope=OPEN_API) | NacosServerAdminAuthConfig (scope=ADMIN_API) |

两个 Filter 通过"互斥的 isMatchFilter"实现 API 类型分流，而非在 Filter 内部 if-else。

### 5.3 RemoteRequestAuthFilter

**定位**：gRPC 协议的鉴权过滤器，继承 `AbstractRequestFilter`。

**自注册机制**（[源码 L44-L47](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/AbstractRequestFilter.java#L44-L47)）：

```java
@PostConstruct
public void init() {
    requestFilters.registerFilter(this);  // 把自己注入到 RequestFilters 列表
}
```

**Filter 链调用**（[RequestHandler L45-L56](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/RequestHandler.java#L45-L56)）：

```java
for (AbstractRequestFilter filter : requestFilters.filters) {
    Response filterResult = filter.filter(request, meta, this.getClass());
    if (filterResult != null && !filterResult.isSuccess()) {
        return filterResult;  // 任一 Filter 拒绝则短路
    }
}
```

### 5.4 AbstractProtocolAuthService

**定位**：协议适配层基类，负责从请求中解析身份/资源，委托给 `AuthPluginService` SPI。

**核心方法**（[源码 L47-L152](file:///Users/mmhm/IdeaProjects/nacos/auth/src/main/java/com/alibaba/nacos/auth/AbstractProtocolAuthService.java#L47-L152)）：

| 方法 | 职责 |
|------|------|
| `enableAuth(secured)` | 查 SPI：当前认证系统是否要求对此 API 鉴权 |
| `validateIdentity(ctx, resource)` | 查 SPI → `AuthPluginService.validateIdentity()` |
| `validateAuthority(ctx, permission)` | 查 SPI → `AuthPluginService.validateAuthority()` |
| `checkServerIdentity(request, secured)` | 检查服务间身份 key-value 是否匹配 |

两个子类：`HttpProtocolAuthService` 和 `GrpcProtocolAuthService`，各自实现 `parseResource()` 和 `parseIdentity()` 适配不同协议。

### 5.5 NacosAuthConfigHolder

**定位**：通过 SPI 发现所有 `NacosAuthConfig` 实现，按 scope 建立配置注册表。

**初始化**（[源码 L36-L41](file:///Users/mmhm/IdeaProjects/nacos/auth/src/main/java/com/alibaba/nacos/auth/config/NacosAuthConfigHolder.java#L36-L41)）：

```java
NacosAuthConfigHolder() {
    this.nacosAuthConfigMap = new HashMap<>();
    for (NacosAuthConfig each : NacosServiceLoader.load(NacosAuthConfig.class)) {
        nacosAuthConfigMap.put(each.getAuthScope(), each);
    }
}
```

SPI 文件位置：
- `core/src/main/resources/META-INF/services/com.alibaba.nacos.auth.config.NacosAuthConfig` → 注册 `NacosServerAuthConfig` 和 `NacosServerAdminAuthConfig`
- `console` 模块同样注册 `NacosConsoleAuthConfig`

---

## 六、扩展点

### 6.1 AuthPluginService（SPI）

```java
public interface AuthPluginService {
    boolean enableAuth(ActionType action, SignType signType);    // 此接口是否启用鉴权
    AuthResult validateIdentity(IdentityContext context, Resource resource);  // 身份校验
    AuthResult validateAuthority(IdentityContext context, Permission permission);  // 权限校验
    boolean isAdminRequest();   // 是否需要初始化 admin 用户
    String getAuthServiceName(); // 插件名称（与 nacos.core.auth.system.type 对应）
}
```

- 当前实现：`nacos`（内置）、`ldap`、`oidc`
- 新增认证系统：实现接口 + 添加 SPI 文件即可
- 详细实战指南见「第九章：SPI 鉴权插件实战」

### 6.2 ResourceParser（协议解析扩展）

- `AbstractHttpResourceParser`：HTTP 请求的资源解析器
- `AbstractGrpcResourceParser`：gRPC 请求的资源解析器
- 新增协议类型只需实现对应的 Parser 接口

### 6.3 InnerApiAuthEnabled（升级兼容）

`@Scheduled(fixedRate = 3000)` 定时检查所有集群成员版本是否 ≥ 3.0，全部升级完成后启用内网 API 的身份校验。这是 2.x → 3.x 升级期间的临时兼容机制。

---

## 七、关键设计决策

1. **Filter 互斥分工而非内部分支**：AuthFilter 和 AuthAdminFilter 通过 `isMatchFilter()` 互斥分流，而非在一个 Filter 里 if-else，保证每个 Filter 职责单一
2. **ProtocolAuthService 作为协议抽象层**：Http 和 gRPC 共用同一套 `validateIdentity/Authority`，只有 `parseResource/parseIdentity` 不同——通过模板方法 + 子类实现差异部分
3. **双层 SPI**：配置层（`NacosAuthConfig`）和执行层（`AuthPluginService`）分离，模块可独立提供配置，认证系统可独立切换
4. **OPEN_API 默认不鉴权**：客户端 SDK 调用的 API 默认不强制鉴权，降低接入门槛，生产环境需显式开启
5. **ADMIN_API 默认鉴权**：管理 API 默认启用鉴权，防止未授权操作

---

## 八、相关文件清单

| 文件 | 模块 | 角色 |
|------|------|------|
| `AbstractWebAuthFilter.java` | core | HTTP 鉴权模板方法基类 |
| `AuthFilter.java` | core | 非 ADMIN API 鉴权 |
| `AuthAdminFilter.java` | core | ADMIN API 鉴权 |
| `AuthConfig.java` | core | Spring 配置，注册 Filter Bean |
| `RemoteRequestAuthFilter.java` | core | gRPC 鉴权 |
| `NacosServerAuthConfig.java` | core | OPEN_API 鉴权配置 |
| `NacosServerAdminAuthConfig.java` | core | ADMIN_API 鉴权配置 |
| `InnerApiAuthEnabled.java` | core | 升级期间内网 API 兼容开关 |
| `AuthModuleStateBuilder.java` | core | 鉴权模块状态上报 |
| `NacosAuthConfig.java` | auth | 鉴权配置接口 |
| `NacosAuthConfigHolder.java` | auth | SPI 发现 + 配置注册表 |
| `AbstractProtocolAuthService.java` | auth | 协议适配 + SPI 委托基类 |
| `HttpProtocolAuthService.java` | auth | HTTP 协议适配 |
| `GrpcProtocolAuthService.java` | auth | gRPC 协议适配 |
| `AbstractRequestFilter.java` | core | gRPC Filter 基类（自注册） |
| `RequestHandler.java` | core | gRPC 请求处理器（迭代 Filter 链） |
| `RequestFilters.java` | core | gRPC Filter 链容器 |
| `NacosAuthPluginService.java` | plugin-default-impl | 内置 Nacos 认证实现 |
| `LdapAuthPluginService.java` | plugin-default-impl | LDAP 认证实现 |
| `OidcWebSecurityConfig.java` | plugin-default-impl | OIDC 认证实现 |

---

## 九、SPI 鉴权插件实战

### 9.1 三种内置插件分别控制什么

配置 `nacos.core.auth.system.type` 决定用哪种插件，每种插件的核心差异：

| 插件 | `getAuthServiceName()` | 身份校验方式 | 用户存储在哪 | 典型场景 |
|------|----------------------|------------|----------|--------|
| **nacos** | `"nacos"` | JWT token 或 用户名+密码，BCrypt 加密 | Nacos 自己的数据库（users 表） | 独立部署、小团队 |
| **ldap** | `"ldap"` | 用户名+密码，委托给企业 LDAP/AD 服务器验证 | 企业 LDAP 目录 | 企业内网统一身份 |
| **oidc** | `"oidc"` | 验证 OIDC 提供商签发的 JWT 签名和有效期 | OIDC 提供商（Google/GitHub/Keycloak） | SSO 单点登录 |

插件继承关系：

```
AuthPluginService  <<interface>>
  │
  ├─ NacosAuthPluginService      ← 默认插件，完整实现
  │     └─ LdapAuthPluginService  ← 继承 Nacos 插件，只替换 authenticationManager 为 LDAP 版本
  │
  └─ OidcAuthPluginService       ← 独立实现，不继承 Nacos 插件
```

### 9.2 插件在鉴权管道中控制哪些东西

当请求到达插件时，插件拿到的是**统一格式的数据**，而不是原始 HTTP/gRPC 请求：

```
请求进入鉴权管道
  │
  ├─ ProtocolAuthService.parseResource()     已提取好：Resource {type, namespace, group, name}
  ├─ ProtocolAuthService.parseIdentity()     已提取好：IdentityContext {username, token}
  │
  └─ 插件收到的是：
      ├─ validateIdentity(IdentityContext, Resource)   → 决定"你是谁，你是否合法"
      └─ validateAuthority(IdentityContext, Permission) → 决定"你能对这个资源做什么"
```

**插件控制的两个维度**：

| 方法 | 控制什么 | 返回什么 |
|------|---------|--------|
| `validateIdentity()` | 用户身份是否合法（密码对吗？token 有效吗？） | `AuthResult.successResult(user)` 或 `AuthResult.failureResult(401, "...")` |
| `validateAuthority()` | 用户对该资源是否有指定操作的权限 | `AuthResult.successResult()` 或 `AuthResult.failureResult(403, "...")` |

### 9.3 插件能看到什么信息

当你的插件收到 `Permission` 时，包含：

```java
Permission {
    Resource resource {
        String type           // "config" / "naming" / "ai"    ← 来自 @Secured.signType
        String namespaceId    // "dev" / "prod" / null        ← 从请求参数动态提取
        String group          // "DEFAULT_GROUP"              ← 从请求参数动态提取
        String resourceName   // "application.yml" / "user-service"  ← 从请求参数动态提取
        Properties properties // 包含 @Secured.tags 注入的键值对
    }
    ActionTypes action        // READ 或 WRITE               ← 来自 @Secured.action
}
```

### 9.4 实现自定义插件的步骤

**Step 1**：实现 `AuthPluginService` 接口

```java
public class MyAuthPluginService implements AuthPluginService {

    @Override
    public String getAuthServiceName() {
        return "my-auth";  // 对应配置 nacos.core.auth.system.type=my-auth
    }

    @Override
    public Collection<String> identityNames() {
        // 告诉框架从请求的哪些字段提取身份信息
        return List.of("Authorization", "X-Custom-Token");
    }

    @Override
    public boolean enableAuth(ActionTypes action, String type) {
        return true;  // 对所有操作都启用鉴权
    }

    @Override
    public AuthResult validateIdentity(IdentityContext ctx, Resource resource) {
        // 自定义：验证用户身份（查你的用户系统）
        String token = (String) ctx.getParameter("X-Custom-Token");
        if (isValidToken(token)) {
            return AuthResult.successResult();
        }
        return AuthResult.failureResult(401, "Invalid token");
    }

    @Override
    public AuthResult validateAuthority(IdentityContext ctx, Permission permission) {
        // 自定义：验证权限（查你的权限系统）
        String userId = (String) ctx.getParameter("userId");
        String resourceType = permission.getResource().getType();    // "config" / "naming" / "ai"
        String action = permission.getAction().toString();           // "READ" / "WRITE"
        String namespace = permission.getResource().getNamespaceId();

        // 示例：只有 DBA 角色才能修改配置
        if ("config".equals(resourceType) && "WRITE".equals(action)) {
            if (!hasRole(userId, "DBA")) {
                return AuthResult.failureResult(403, "需要 DBA 角色");
            }
        }
        // 示例：生产环境只能读不能写
        if ("prod".equals(namespace) && "WRITE".equals(action)) {
            if (!hasRole(userId, "PROD_ADMIN")) {
                return AuthResult.failureResult(403, "生产环境禁止写入");
            }
        }
        return AuthResult.successResult();
    }

    @Override
    public boolean isLoginEnabled() { return false; }   // 不需要 Nacos 登录页

    @Override
    public boolean isAdminRequest() { return false; }   // 不需要初始化 admin 用户
}
```

**Step 2**：创建 SPI 注册文件

```
src/main/resources/META-INF/services/
  com.alibaba.nacos.plugin.auth.spi.server.AuthPluginService
  内容：com.yourcompany.MyAuthPluginService
```

**Step 3**：配置 Nacos 使用你的插件

```properties
nacos.core.auth.system.type=my-auth
nacos.core.auth.enabled=true
```

框架的调用链路会自动路由到你的插件：

```
请求到达
  → AbstractWebAuthFilter.doFilter()（9 步管道不变）
    → AuthPluginManager.findAuthServiceSpiImpl("my-auth")  ← 找到你的插件
      → MyAuthPluginService.validateIdentity()             ← 你的代码
      → MyAuthPluginService.validateAuthority()             ← 你的代码
```

### 9.5 细粒度控制：用 `@Secured.tags` 区分操作

同一个资源的多个 API（如 online / offline）共享相同的 `signType` + `action`，
插件无法直接区分。解决方法：在 `@Secured` 的 `tags` 中注入操作标识：

```java
// 下线 Skill 接口
@Secured(action = WRITE, signType = AI, apiType = ADMIN_API,
    tags = {"operation=offline"})

// 上线 Skill 接口
@Secured(action = WRITE, signType = AI, apiType = ADMIN_API,
    tags = {"operation=online"})
```

`tags` 会被注入到 `Resource.properties`，插件可以读取：

```java
@Override
public AuthResult validateAuthority(IdentityContext ctx, Permission perm) {
    String operation = perm.getResource().getProperties().getProperty("operation");
    // "offline" 或 "online"
    if ("offline".equals(operation) && !hasRole(userId, "OPERATOR")) {
        return AuthResult.failureResult(403, "只有运维角色可以下线");
    }
}
```

### 9.6 实际场景与选择建议

| 场景 | 推荐方案 | 为什么 |
|------|---------|--------|
| 开发/测试环境 | 关闭鉴权（默认） | 简单方便 |
| 生产内网，几个开发者 | 内置 nacos 插件（账号密码 + JWT） | 够用 |
| 大企业，员工 1000+ | ldap 或 oidc 插件 | 接入公司统一身份系统，不用单独管用户 |
| 需要控制到具体资源 + 操作 | 自定义插件 | 实现自己的权限模型（ABAC/RBAC） |
