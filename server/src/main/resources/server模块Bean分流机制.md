# server 模块：Bean 分流机制

## 模块定位

`server` 模块不仅仅是打包聚合，它是 **Bean 分流的核心**——通过 `@ComponentScan` 过滤器，把同一个 classpath 上的所有类分派到不同的 Context。

两个入口类都定义在这里：

| 类 | 对应 Context | 启动方式 |
|------|:---:|------|
| `NacosServerBasicApplication` | CoreContext | `SpringApplicationBuilder(.web(NONE))` |
| `NacosServerWebApplication` | WebContext | `SpringApplicationBuilder(.parent(coreContext))` |

---

## 依赖

```
pom.xml 依赖：
  ├── nacos-naming      （服务发现）
  ├── nacos-config      （配置中心）
  ├── nacos-istio       （Istio 集成）
  ├── nacos-prometheus  （监控指标）
  └── nacos-default-plugin-all（dev profile）
```

---

## Bean 分流流程图

```
classpath 上的所有类
         │
         │  @ComponentScan + TypeFilter
         │
    ┌────┴────┐
    │         │
    ▼         ▼
NacosWebBeanTypeFilter    NacosNormalBeanTypeFilter
(给 CoreContext 用)        (给 WebContext 用)
    │                          │
    │  是 Web Bean → 排除       │  不是 Web Bean → 排除
    │                          │
    ▼                          ▼
CoreContext                WebContext
纯 Service / 基础设施      只有 Controller / @NacosWebBean
```

---

## 四个类的职责

### AbstractNacosWebBeanTypeFilter（基类）

定义"什么是 Web Bean"：类上标了以下四种注解之一就是 Web Bean。

```java
private static final Set<String> WEB_BEAN_ANNOTATIONS = new HashSet<>();
static {
    WEB_BEAN_ANNOTATIONS.add(RestController.class.getCanonicalName());   // REST 接口
    WEB_BEAN_ANNOTATIONS.add(Controller.class.getCanonicalName());       // MVC 接口
    WEB_BEAN_ANNOTATIONS.add(ControllerAdvice.class.getCanonicalName()); // 全局异常处理
    WEB_BEAN_ANNOTATIONS.add(NacosWebBean.class.getCanonicalName());     // 自定义标记
}
```

### NacosWebBeanTypeFilter → CoreContext 用

```java
public boolean match(...) {
    return super.isWebBean(...);  // 是 Web Bean → true → 被排除
}
```

效果：CoreContext 里没有 Controller、没有 REST 端点、没有 `@NacosWebBean` 类。只有 Service、数据源、集群管理等纯服务类。

### NacosNormalBeanTypeFilter → WebContext 用

```java
public boolean match(...) {
    return !super.isWebBean(...);  // 不是 Web Bean → true → 被排除
}
```

效果：WebContext 里只有 Controller 和 `@NacosWebBean` 类。Service 层的 Bean 只能通过 `.parent(coreContext)` 从 CoreContext 获取。

### NacosWebBeanPostProcessorConfiguration → 仅 WebContext 加载

```java
@Configuration
@NacosWebBean  // ← 标记自己是 Web Bean，逃过 NacosNormalBeanTypeFilter 的排除
public class NacosWebBeanPostProcessorConfiguration {

    @Bean
    public InstantiationAwareBeanPostProcessor nacosDuplicateSpringBeanPostProcessor(...) {
        return new NacosDuplicateSpringBeanPostProcessor(context);
    }

    @Bean
    public InstantiationAwareBeanPostProcessor nacosDuplicateConfigurationBeanPostProcessor(...) {
        return new NacosDuplicateConfigurationBeanPostProcessor(context);
    }
}
```

这个类标记了 `@NacosWebBean`，否则 `@Configuration` 不在 Web Bean 注解集合里，会被 `NacosNormalBeanTypeFilter` 排除。

职责：两个 `BeanPostProcessor` 在 Bean 实例化前检测——这个 Bean 是否同时存在于父 Context 和子 Context？如果存在，说明有重复定义，应该只保留父 Context 中的版本。

---

## 配套文件

### nacos-server.properties

WebContext 专属配置：

```properties
server.port=${nacos.server.main.port:8848}
server.servlet.contextPath=${nacos.server.contextPath:/nacos}
spring.sql.init.mode=never
spring.web.resources.add-mappings=false
server.servlet.encoding.enabled=true
server.servlet.encoding.force=true
server.servlet.encoding.charset=UTF-8
```

只在 WebContext 生效，不影响 CoreContext（Core 是非 Web 应用，根本不需要这些配置）。

### nacos-server-web-banner.txt

WebContext 启动时的 ASCII Art 横幅。

---

## 为什么这样设计？

| 好处 | 说明 |
|------|------|
| 编译级别硬隔离 | Core 的代码不可能注入到 Controller，因为 Controller 在 CoreContext 里根本不存在 |
| 同一代码库多模式 | 改一行 `nacos.deployment.type` 就能切换 SERVER/MERGED/CONSOLE |
| 配置隔离 | WebContext 的 `server.port` 不会污染 CoreContext |
| 强制启动顺序 | NacosStartUpManager 保证 Core → Web 的依赖顺序 |
