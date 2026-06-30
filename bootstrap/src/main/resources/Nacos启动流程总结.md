# Nacos v3 启动流程总结

## 一、唯一入口：NacosBootstrap

整个项目唯一被 JVM 调用的 `main()` 方法，位于 `bootstrap` 模块。

```
bootstrap/src/main/java/com/alibaba/nacos/bootstrap/NacosBootstrap.java
```

### 为什么有多个 @SpringBootApplication 类

| 类 | 模块 | main() 是否被调用 |
|-----|------|:---:|
| `NacosBootstrap` | bootstrap | ✅ 唯一真正的入口 |
| `NacosServerBasicApplication` | core | ❌ 被 SpringApplicationBuilder 以编程方式启动 |
| `NacosServerWebApplication` | server | ❌ 同上 |
| `NacosConsole` | console | ❌ 同上（main() 仅为独立 debug 提供） |

SpringApplicationBuilder 不调用 main()，直接用 `.run(args)` 编程式启动 Context。

---

## 二、三种部署模式

JVM 参数 `nacos.deployment.type` 控制启动哪些 Spring Context：

| 模式 | 参数值 | 启动的 Context | 场景 |
|------|--------|:---:|------|
| `MERGED`（默认） | `merged` 或不设 | Core → Web → Console | IDEA 本地开发 |
| `SERVER` | `server` | Core → Web | 纯服务端，生产环境 |
| `CONSOLE` | `console` | 仅 Console | 控制台独立部署 |

```java
// 入口代码
String type = System.getProperty("nacos.deployment.type", "merged");
DeploymentType deploymentType = DeploymentType.getType(type);
// MERGED → startWithConsole(args)
// SERVER → startWithoutConsole(args)
// CONSOLE → startOnlyConsole(args)
```

---

## 三、SpringApplicationBuilder 与父子 Context

### 核心 API

```java
new SpringApplicationBuilder(目标类.class)
    .web(WebApplicationType.NONE)    // 控制是否启 Web 容器（Core 设为 NONE）
    .parent(parentContext)           // 设父上下文，子能 @Autowired 父的 Bean
    .banner(banner对象)              // 自定义启动横幅
    .run(args);                      // 最后调用，返回 ConfigurableApplicationContext
```

### Nacos 的父子 Context 链

```
CoreContext（父，非 Web 应用）
    │  NacosServerBasicApplication
    │  职责：集群管理、命名空间、数据源、启动生命周期
    │
    ├── WebContext（子）
    │     NacosServerWebApplication，.parent(coreContext)
    │     职责：Config 配置中心 + Naming 服务发现
    │
    └── ConsoleContext（子）
          NacosConsole，.parent(coreContext)
          职责：Web 控制台后端
```

### 父子规则

- 子 Context 可以 `@Autowired` 注入父 Context 的 Bean
- 父 Context **不能**访问子 Context 的 Bean
- 各自拥有独立的 BeanFactory，互不污染
- 不设 `.parent()` 则子 Context 找不到父的 Bean → `NoSuchBeanDefinitionException`

### 为什么用父子 Context？真正的好处

> ⚠️ **常见误区**：MERGER 模式下三个 Context 全启动，Naming 和 Config 的 Bean 一个不少，父
> 子 Context 在 MERGER 模式下**并不节省内存**。内存节省只在 CONSOLE-only 独立部署时才发生。

**① 强制启动顺序**

NacosStartUpManager 是一个状态机，必须按 CORE → WEB → CONSOLE 顺序启动。跳过任何阶段都会抛 `IllegalStateException`。这保证了"数据源一定在 Config 之前初始化好"。如果使用单一大 Context，Spring 的组件扫描顺序不可控。

**② 编译级别的依赖边界隔离**

Core Context 能注入：DataSource ✓ ClusterInfo ✓ NotifyCenter ✓
Core Context 不能注入：ConfigController ✗ InstanceService ✗ ConsoleController ✗

这是编译层面无法引用的硬约束。如果是单一大 Context，开发人员随时可能在 Core 代码里 `@Autowired` 一个 Console 的 Bean——MERGER 模式能跑，切到 SERVER 模式就炸。父子 Context 杜绝了这类跨模式 bug。

**③ 子 Context 可以有自己的覆盖配置**

Console Context 有独立的 `@PropertySource`，可以覆盖父 Context 的同名配置，而不影响 Web Context 中该配置的含义。同一个 JVM 内，不同模块的配置可以隔离。

**④ 部署灵活性**

同一个代码库，运行时通过 `nacos.deployment.type` 参数组装不同的 Context 组合：

| 模式 | 启动的 Context | Bean 数量 |
|------|:---:|:---:|
| MERGED | Core + Web + Console | 全部加载 |
| SERVER | Core + Web | 不加载 Console 的 Bean |
| CONSOLE | 仅 Console | 不加载 Core / Web 的 Bean |

SERVER 和 CONSOLE 模式才是真正能跳过不必要 Bean 的场景。

### 底层实现原理

`AbstractApplicationContext` 持有 `parent` 引用，`@Autowired` 解析时查找链是单向的：

```
child.getBean("dataSource")
    → 先在 child 的 BeanFactory 找
    → 找不到 → 去 parent 的 BeanFactory 找
    → 找到了 ✓

parent.getBean("configController")
    → 只在 parent 的 BeanFactory 找
    → 找不到 ✗（不会向下查找，parent 不知道自己的孩子是谁）
```

Spring 官方把这种设计称为 **Hierarchical ApplicationContexts**。Spring MVC 本身就是这么做的：`ContextLoaderListener` 创建的 root context 是父，`DispatcherServlet` 的 servlet context 是子。

### 事件传播规则：子→父 通，父→子 不通

Spring 父子 Context 的事件传播是**单向**的，由 `AbstractApplicationContext.publishEvent()` 控制：

```
子 Context 发布事件
    → 子自己的监听器收到 ✓
    → 自动调 parent.publishEvent()
    → 父的监听器也收到 ✓

父 Context 发布事件
    → 父自己的监听器收到 ✓
    → 子监听器收不到 ✗（父没有持有子引用，无法反向传播）
```

所以不能说"事件不跨 Context"——**子→父是通的**，只是**父→子不通**。

### Nacos 为什么不用 Spring 事件，而自建 NotifyCenter？

`NotifyCenter` 在 `common` 模块中，这个模块**不依赖 Spring**。它绕开 Spring 事件的原因与父子 Context 无关：

**① common 模块被不依赖 Spring 的模块使用**

`client`、`client-basic` 等客户端模块也依赖 `common`，这些模块根本没有 Spring Context，无法使用 `@EventListener`。`NotifyCenter` 提供了 Spring 无关的事件机制。

**② 高性能异步事件**

NotifyCenter 底层用 RingBuffer（类似 Disruptor），事件发布是异步、带背压控制的。Spring 的 `ApplicationEventMulticaster` 默认同步广播，所有监听器在发布线程里串行执行。高吞吐场景下性能差距显著。

**③ 插件系统的 ClassLoader 隔离**

Nacos 的插件可能用独立 ClassLoader 加载。Spring 的父子 Context 事件机制在跨 ClassLoader 时不可靠，但 `NotifyCenter` 是静态全局 Map，同一 ClassLoader 层内始终有效。

所以 `NotifyCenter` 的定位是**一个高性能、Spring 无关、跨模块的全局事件总线**，而非父子 Context 的"填坑"方案。

### 父子 Context 的真正坏处

**① 事务不能跨 Context**

`@Transactional` 的事务管理器是每个 Context 独立的。无法在一个事务里同时操作 Core 的数据源和 Console 的数据。

**② 同类型 Bean 遮蔽（shadowing）**

父和子各自定义了同类型的 Bean（如各自的 `ObjectMapper`），子 Context 用子的，父 Context 用父的。行为不一致但都合法，排查困难。

**③ 调试时 Bean 来源不透明**

报 `NoSuchBeanDefinitionException` 时堆栈不告诉你是"父 Context 有，但当前子 Context 取不到"。得手动查 BeanFactory 层级。

**④ 启动故障级联**

Core 启动失败 → Web 和 Console 全挂。三个 Context 串行依赖，错误堆栈难以直观看出根因是父 Context 没起来。

**⑤ Spring Boot Actuator 只监控一个 Context**

`/actuator/beans`、`/actuator/health` 等端点只能看到当前 Context 状态。想看全部 Bean 得自定义端点。

### 好处与坏处对比

| 维度 | 好处 | 坏处 |
|------|------|------|
| 事务 | — | 不能跨 Context 事务 |
| Bean 隔离 | 编译级别硬约束，杜绝跨模式 bug | 同类型 Bean 可能遮蔽，调试时 Bean 来源不透明 |
| 启动 | 强制顺序保证数据源先于业务初始化 | 单点失败级联 |
| 配置 | 子可独立覆盖父配置 | 同名配置混淆 |
| 部署 | 同一代码库按模式组装组合 | — |
| 运维 | — | Actuator 只监控一个 Context |

本质上是"用一个复杂度换另一个复杂度"——避免了单一大 Context 的依赖混乱（大泥球），但引入了事务隔离、启动级联、运维可见性等成本。

---

## 四、完整启动链路（MERGED 模式）

```
NacosBootstrap.main()
    │
    ├── 读取 nacos.deployment.type → MERGED
    │
    ├── startCoreContext()
    │   │  SpringApplicationBuilder(NacosServerBasicApplication.class)
    │   │  .web(WebApplicationType.NONE)   ← 不启动 Tomcat
    │   │  .banner(core-banner.txt)
    │   │  .run(args)
    │   └──→ CoreContext 就绪
    │
    ├── startServerWebContext(args, coreContext)
    │   │  SpringApplicationBuilder(NacosServerWebApplication.class)
    │   │  .parent(coreContext)            ← 能注入 Core 的 Bean
    │   │  .banner(nacos-server-web-banner.txt)
    │   │  .run(args)
    │   └──→ WebContext 就绪
    │
    └── startConsoleContext(args, coreContext)
        │  SpringApplicationBuilder(NacosConsole.class)
        │  .parent(coreContext)
        │  .banner(nacos-console-banner.txt)
        │  .run(args)
        └──→ ConsoleContext 就绪
```

---

## 五、Banner 文件

启动时的 ASCII Art 来自各模块 resources/ 目录下的 txt 文件：

| Banner 文件 | 模块位置 | 对应 Context |
|---|---|---|
| `core-banner.txt` | `core/src/main/resources/` | CoreContext |
| `nacos-server-web-banner.txt` | `server/src/main/resources/` | WebContext |
| `nacos-console-banner.txt` | `console/src/main/resources/` | ConsoleContext |

通过 `ClassPathResource` 从 classpath 根路径加载（resources 目录编译后在 classpath 根）：

```java
private static Banner getBanner(String bannerFileName) {
    return new ResourceBanner(new ClassPathResource(bannerFileName));
}
```

---

## 六、CoreContext 的 resources 目录结构

`core/src/main/resources/` 下除了 `core-banner.txt`，还有：

| 路径 | 作用 |
|------|------|
| `core-banner.txt` | Core Context 启动横幅 |
| `META-INF/spring.factories` | Spring Boot 自动配置声明 |
| `META-INF/logback/nacos.xml` | 日志配置 |
| `META-INF/services/` | **Java SPI 服务注册**，文件名是接口名，内容是实现类 |

`META-INF/services/` 是 Nacos 插件机制的基石，启动时 `NacosServiceLoader` 扫描这些文件完成 SPI 加载。
