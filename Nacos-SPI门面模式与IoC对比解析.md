# Nacos SPI 门面模式与 IoC 对比深度解析

> 以 `JsonUtils` 为切入点，从源码层面理解 Java SPI 机制、门面模式，以及与 Spring IoC 的本质对比。

---

## 一、整体架构总览

Nacos 客户端 SDK 的 JSON 工具体系由四层构成：

```
┌───────────────────────────────────────────────────────────┐
│  第一层：门面层（JsonUtils）               ← api 模块      │
│  静态方法入口，屏蔽底层实现细节                              │
├───────────────────────────────────────────────────────────┤
│  第二层：选择器层（JsonAdapterSelector）   ← api 模块      │
│  SPI 扫描 + 可用性检查 + 优先级选择 + DCL 缓存              │
├───────────────────────────────────────────────────────────┤
│  第三层：接口层（NacosJsonAdapter）         ← api 模块      │
│  定义 toJson / toObj / isAvailable 等契约                   │
├───────────────────────────────────────────────────────────┤
│  第四层：实现层                            ← common 模块    │
│  Jackson2JsonAdapter（com.fasterxml.jackson）              │
│  Jackson3JsonAdapter（tools.jackson）                      │
└───────────────────────────────────────────────────────────┘
```

**设计原则**：接口和实现在不同模块。`api` 模块只定义接口和选择逻辑（Java 8+，零依赖），`common` 模块提供具体实现。用户只需依赖 `api`，实现通过 SPI 自动发现。

---

## 二、第一层：JsonUtils — 静态门面

### 2.1 什么是门面模式

门面模式（Facade）：为子系统中的一组接口提供一个统一入口，屏蔽子系统的复杂性。

```
没有门面：
  用户代码 → new ObjectMapper().writeValueAsString(obj)     // 绑死 Jackson 2
  用户代码 → new Jsonb().toJson(obj)                          // 绑死 Yasson
  用户代码 → new Gson().toJson(obj)                           // 绑死 Gson

有门面：
  用户代码 → JsonUtils.toJson(obj)    // 不关心底层用哪个库
```

### 2.2 JsonUtils 源码结构

```java
// api 模块，Java 8+ 兼容，零第三方依赖
public final class JsonUtils {

    // 系统属性名，用户可通过 -Dnacos.client.json.adapter=jackson2 指定适配器
    public static final String ADAPTER_PROPERTY_NAME = "nacos.client.json.adapter";

    // 已注册的子类型列表（多态序列化用）
    private static final List<NacosJsonSubtype> SUBTYPES = new ArrayList<>();
    // 已回放到适配器上的子类型键（防止重复注册）
    private static final Set<String> REPLAYED_SUBTYPES = new HashSet<>();

    // 适配器选择器，volatile 保证可见性
    private static volatile JsonAdapterSelector selector = new JsonAdapterSelector();

    private JsonUtils() {}  // 私有构造，纯静态工具类

    // —— 预加载 ——
    public static void preload() { adapter(); }

    // —— 序列化 ——
    public static String toJson(Object obj) { return adapter().toJson(obj); }
    public static byte[] toJsonBytes(Object obj) { return adapter().toJsonBytes(obj); }
    public static String toCanonicalJson(Object obj) { return adapter().toCanonicalJson(obj); }

    // —— 反序列化 ——
    public static <T> T toObj(String json, Class<T> cls) { return adapter().toObj(json, cls); }
    public static <T> T toObj(byte[] json, Class<T> cls) { return adapter().toObj(json, cls); }
    public static <T> T toObj(InputStream is, Class<T> cls) { return adapter().toObj(is, cls); }
    // ... 更多重载

    // 核心方法：获取适配器实例
    private static NacosJsonAdapter adapter() {
        NacosJsonAdapter adapter = selector.select();  // 选择
        replaySubtypes(adapter);                         // 回放子类型
        return adapter;
    }
}
```

### 2.3 JsonUtils 如何使用

#### 基本使用

```java
// 序列化
Instance instance = new Instance();
instance.setIp("192.168.1.1");
instance.setPort(8080);
String json = JsonUtils.toJson(instance);
// {"ip":"192.168.1.1","port":8080,...}

// 反序列化
Instance parsed = JsonUtils.toObj(json, Instance.class);

// 字节数组反序列化
byte[] bytes = JsonUtils.toJsonBytes(instance);
Instance fromBytes = JsonUtils.toObj(bytes, Instance.class);

// 输入流反序列化（如 gRPC 响应）
InputStream is = response.getInputStream();
ServiceInfo info = JsonUtils.toObj(is, ServiceInfo.class);

// 泛型类型反序列化
List<Instance> list = JsonUtils.toObj(jsonStr,
    new NacosTypeReference<List<Instance>>(){}.getType());
```

#### 预加载（在 NacosNamingService.init 中）

```java
// PreInitUtils.java
static void preLoadCostComponent() {
    JsonUtils.preload();              // 触发 SPI 扫描 + 适配器选择 + ObjectMapper 初始化
    JsonAdapterLogUtils.logSelectedAdapter();  // 日志记录选中了哪个适配器
}
```

`preload()` 的作用：把首次调用时的 SPI 扫描和 ObjectMapper 初始化开销（数百毫秒）提前到后台线程消化，后续 `toJson()` / `toObj()` 直接走缓存。

---

## 三、第二层：JsonAdapterSelector — SPI 选择器

### 3.1 SPI 是什么

**SPI（Service Provider Interface）** 是 Java 标准的插件发现机制。核心 API 就一个类：

```java
java.util.ServiceLoader<T>
```

工作原理：

```
1. 定义接口
   com.alibaba.nacos.api.utils.json.NacosJsonAdapter

2. 实现接口（在另一个模块/JAR）
   com.alibaba.nacos.common.json.Jackson2JsonAdapter
   com.alibaba.nacos.common.json.Jackson3JsonAdapter

3. 注册实现（在实现模块的 resources 下）
   META-INF/services/com.alibaba.nacos.api.utils.json.NacosJsonAdapter
   文件内容：
     com.alibaba.nacos.common.json.Jackson2JsonAdapter
     com.alibaba.nacos.common.json.Jackson3JsonAdapter

4. 运行时发现
   ServiceLoader<NacosJsonAdapter> loader = ServiceLoader.load(NacosJsonAdapter.class);
   for (NacosJsonAdapter adapter : loader) {
       // 拿到了 Jackson2JsonAdapter 和 Jackson3JsonAdapter 的实例
   }
```

SPI 注册文件位置：

```
common/
└── src/main/resources/
    └── META-INF/
        └── services/
            └── com.alibaba.nacos.api.utils.json.NacosJsonAdapter   ← 文件名=接口全名
                ↑
                com.alibaba.nacos.common.json.Jackson2JsonAdapter      ← 内容=实现类全名
                com.alibaba.nacos.common.json.Jackson3JsonAdapter
```

`ServiceLoader` 内部做的事：

```
1. 读取 classpath 上所有 JAR/目录中的 META-INF/services/{接口全名} 文件
2. 逐行读取实现类全名
3. Class.forName(实现类名)  → 加载类
4. clazz.newInstance()      → 反射实例化
5. 返回所有实例
```

### 3.2 JsonAdapterSelector 源码逐行解析

```java
final class JsonAdapterSelector {

    // 系统属性名
    static final String ADAPTER_PROPERTY_NAME = "nacos.client.json.adapter";

    // SPI 加载到的所有适配器（构造时加载）
    private final List<NacosJsonAdapter> adapters;

    // 加载失败的诊断信息
    private final List<String> loadFailures;

    // 选中的适配器（volatile + DCL）
    private volatile NacosJsonAdapter selectedAdapter;

    // 构造器：触发 SPI 扫描
    JsonAdapterSelector() {
        this(loadAdapters());   // ← 首次构造时扫描 classpath
    }

    // —— ① SPI 扫描 ——
    private static LoadedAdapters loadAdapters() {
        List<NacosJsonAdapter> result = new ArrayList<>();
        List<String> failures = new ArrayList<>();

        // Java 标准 SPI
        ServiceLoader<NacosJsonAdapter> loader = ServiceLoader.load(NacosJsonAdapter.class);
        Iterator<NacosJsonAdapter> iterator = loader.iterator();

        while (true) {
            try {
                if (!iterator.hasNext()) break;
                result.add(iterator.next());  // 反射实例化每个实现类
            } catch (ServiceConfigurationError | LinkageError e) {
                failures.add("provider: " + e.getClass().getName() + ": " + e.getMessage());
                break;  // 某个实现加载失败不影响其他实现
            }
        }
        return new LoadedAdapters(result, failures);
    }

    // —— ② 选择入口（DCL 双重检查锁）——
    NacosJsonAdapter select() {
        NacosJsonAdapter adapter = selectedAdapter;
        if (adapter != null) {           // 第一次检查（无锁，快速路径）
            return adapter;
        }
        synchronized (this) {
            if (selectedAdapter == null) { // 第二次检查（加锁，防重复）
                selectedAdapter = doSelect();
            }
            return selectedAdapter;
        }
    }

    // —— ③ 选择逻辑 ——
    private NacosJsonAdapter doSelect() {
        // 读取用户配置
        String configuredName = configuredAdapterName();
        // 过滤出可用适配器
        List<NacosJsonAdapter> available = availableAdapters();

        if (!"auto".equals(configuredName)) {
            return selectExplicit(configuredName, available);  // 用户指定
        }
        return selectAuto(available);                          // 自动选择
    }

    // 读取系统属性
    private String configuredAdapterName() {
        String configured = System.getProperty("nacos.client.json.adapter");
        return StringUtils.isBlank(configured) ? "auto" : configured.trim().toLowerCase();
    }

    // 过滤可用适配器
    private List<NacosJsonAdapter> availableAdapters() {
        List<NacosJsonAdapter> result = new ArrayList<>();
        for (NacosJsonAdapter adapter : adapters) {
            if (isAdapterAvailable(adapter)) {   // 调用 adapter.isAvailable()
                result.add(adapter);
            }
        }
        return result;
    }

    // 自动选择优先级：Jackson3 > Jackson2 > 唯一 > 抛异常
    private NacosJsonAdapter selectAuto(List<NacosJsonAdapter> available) {
        if (available.isEmpty()) throw unavailable("No available JSON adapter found.");

        NacosJsonAdapter jackson3 = findByName(available, "jackson3");
        if (jackson3 != null) return jackson3;

        NacosJsonAdapter jackson2 = findByName(available, "jackson2");
        if (jackson2 != null) return jackson2;

        if (available.size() == 1) return available.get(0);

        throw unavailable("Multiple JSON adapters but none is preferred.");
    }
}
```

### 3.3 选择流程图

```
JsonUtils.toJson(obj)
  │
  ▼
JsonUtils.adapter()
  │
  ▼
selector.select()
  │
  ├─ selectedAdapter != null? ──→ YES → 返回缓存（快速路径）
  │                                 NO ↓
  │                         synchronized (this)
  │                              │
  │                              ▼
  │                         doSelect()
  │                              │
  │              ┌───────────────┼───────────────┐
  │              ▼               ▼               ▼
  │      configuredAdapterName()  availableAdapters()  selectAuto/Explicit
  │      读 -Dnacos.client.       遍历所有适配器      Jackson3 > Jackson2
  │      json.adapter 属性        调 isAvailable()    > 唯一 > 抛异常
  │                              过滤出可用的
  │                              │
  │                              ▼
  │                         selectedAdapter = 选中
  │                              │
  └──────────────────────────────┘
                 │
                 ▼
           replaySubtypes()
           把已注册的子类型应用到适配器
                 │
                 ▼
           adapter.toJson(obj)
                 │
                 ▼
           Jackson2JsonAdapter / Jackson3JsonAdapter
                 │
                 ▼
           ObjectMapper.writeValueAsString(obj)
```

---

## 四、第三层：NacosJsonAdapter — 接口契约

```java
public interface NacosJsonAdapter {

    /** 适配器名称（"jackson2" / "jackson3"） */
    String name();

    /** 检查当前运行环境是否可用（classpath 是否有对应 Jackson 库） */
    boolean isAvailable();

    // —— 序列化 ——
    String toJson(Object obj);
    byte[] toJsonBytes(Object obj);
    String toCanonicalJson(Object obj);  // 字段排序的规范化 JSON

    // —— 反序列化（多种输入源 + 多种类型指定方式）——
    <T> T toObj(byte[] json, Class<T> cls);
    <T> T toObj(byte[] json, Type type);
    <T> T toObj(byte[] json, NacosTypeReference<T> typeReference);
    <T> T toObj(String json, Class<T> cls);
    <T> T toObj(String json, Type type);
    <T> T toObj(String json, NacosTypeReference<T> typeReference);
    <T> T toObj(InputStream inputStream, Class<T> cls);
    <T> T toObj(InputStream inputStream, Type type);

    /** 注册多态子类型（序列化时按 typeName 标注） */
    void registerSubtype(NacosJsonSubtype subtype);
}
```

**为什么需要 `isAvailable()`**：Nacos 不知道用户项目的 classpath 上是 Jackson 2 还是 Jackson 3。两个适配器都注册到 SPI，运行时通过 `isAvailable()` 判断哪个真正可用。

---

## 五、第四层：具体实现

### 5.1 Jackson2JsonAdapter

```java
public class Jackson2JsonAdapter implements NacosJsonAdapter {

    // 两个 ObjectMapper：普通 + 规范化（字段排序）
    private final ObjectMapper mapper = createObjectMapper();
    private final ObjectMapper canonicalMapper = createCanonicalObjectMapper();

    @Override
    public String name() { return "jackson2"; }

    @Override
    public boolean isAvailable() { return true; }  // Jackson 2 一定存在（Nacos 基础依赖）

    @Override
    public String toJson(Object obj) {
        return write(obj, () -> mapper.writeValueAsString(obj));
    }

    // ObjectMapper 配置：
    // - 忽略未知字段（向前兼容）
    // - 不序列化 null 值（减少传输量）
    // - canonicalMapper 额外按字母排序（用于签名/比较）
}
```

### 5.2 Jackson3JsonAdapter

```java
public class Jackson3JsonAdapter implements NacosJsonAdapter {

    // 延迟加载的委托对象
    private volatile NacosJsonAdapter delegate;

    @Override
    public String name() { return "jackson3"; }

    @Override
    public boolean isAvailable() {
        try {
            // Jackson 3 的包名是 tools.jackson（不同于 Jackson 2 的 com.fasterxml.jackson）
            Class.forName("tools.jackson.databind.ObjectMapper", false, classLoader);
            Class.forName("tools.jackson.core.JacksonException", false, classLoader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;  // classpath 上没有 Jackson 3
        }
    }

    // 委托模式：首次调用时才创建真正的 Jackson3 适配器
    private NacosJsonAdapter delegate() {
        if (delegate == null) {
            synchronized (this) {
                if (delegate == null) {
                    delegate = new Jackson3JsonAdapterDelegate();  // 真正用 Jackson 3 API
                }
            }
        }
        return delegate;
    }
}
```

### 5.3 为什么不能只用一个适配器

| 问题 | Jackson 2 | Jackson 3 |
|------|-----------|-----------|
| **包名** | `com.fasterxml.jackson.databind.ObjectMapper` | `tools.jackson.databind.ObjectMapper` |
| **API** | `mapper.writeValueAsString(obj)` | API 有变化 |
| **Maven 坐标** | `com.fasterxml.jackson.core:jackson-databind` | `tools.jackson:jackson-databind` |
| **兼容性** | Java 8+ | Java 17+ |

如果直接 `import com.fasterxml.jackson.databind.ObjectMapper`，当用户项目只有 Jackson 3 时会 `ClassNotFoundException`。所以必须用两个适配器分别 import 不同包名，通过 `isAvailable()` 在运行时判断。

---

## 六、SPI 原理深度解析

### 6.1 ServiceLoader 的工作机制

```
ServiceLoader.load(NacosJsonAdapter.class)
  │
  ▼
1. 构造 ServiceLoader 对象
  │
  ▼
2. lazy 迭代（不是立即加载，调用 iterator.hasNext()/next() 时才加载）
  │
  ▼
3. 扫描 classpath 上所有 JAR 和目录的：
     META-INF/services/com.alibaba.nacos.api.utils.json.NacosJsonAdapter
  │
  ▼
4. 逐行读取文件内容（每行一个实现类全名）：
     com.alibaba.nacos.common.json.Jackson2JsonAdapter
     com.alibaba.nacos.common.json.Jackson3JsonAdapter
  │
  ▼
5. Class.forName(实现类全名)     ← 类加载
  │
  ▼
6. clazz.newInstance()           ← 反射实例化（无参构造）
  │
  ▼
7. 返回实例
```

### 6.2 SPI 文件格式

```
文件路径：META-INF/services/{接口全限定名}
文件内容：每行一个实现类全限定名，# 开头为注释
```

Nacos 的 SPI 文件：

```
# 文件路径：common/src/main/resources/META-INF/services/
#           com.alibaba.nacos.api.utils.json.NacosJsonAdapter
# 文件内容：

com.alibaba.nacos.common.json.Jackson2JsonAdapter
com.alibaba.nacos.common.json.Jackson3JsonAdapter
```

### 6.3 SPI 的关键特性

| 特性 | 说明 |
|------|------|
| **延迟加载** | `ServiceLoader` 是懒加载，`iterator.next()` 时才实例化 |
| **按需扫描** | 只扫描 classpath 上实际存在的 SPI 文件 |
| **多 JAR 合并** | 多个 JAR 都注册了同一接口的 SPI，实现全部合并 |
| **无依赖** | 纯 JDK API，不需要任何第三方库 |
| **无注解** | 不需要 `@Service` / `@Component`，纯配置文件 |
| **无容器** | 不需要 Spring / Guice 等 IoC 容器 |

---

## 七、SPI vs Spring IoC 深度对比

### 7.1 核心对比表

| 维度 | Java SPI | Spring IoC |
|------|----------|------------|
| **发现机制** | `META-INF/services/` 配置文件 | 包扫描 `@Component` / `@Service` |
| **实例化** | `Class.forName()` + `newInstance()` | 反射 + CGLIB 代理 |
| **选择** | 手写 if-else 优先级逻辑 | `@Primary` / `@Qualifier` / `@ConditionalOnProperty` |
| **生命周期** | 无（创建后不管） | 完整（`@PostConstruct` / `@PreDestroy` / scope） |
| **依赖注入** | 无（组件之间不能互相注入） | `@Autowired` 自动注入 |
| **配置覆盖** | `System.getProperty()` 手动读 | `@Value` / `@ConfigurationProperties` |
| **AOP** | 无 | `@Transactional` / `@Async` 等切面 |
| **启动开销** | 首次 `ServiceLoader.load()` 数百毫秒 | 容器启动几秒到几十秒 |
| **依赖体积** | 0 KB（JDK 自带） | Spring Core + Context + Beans 约 1.5 MB |
| **Java 版本** | Java 6+ | Java 8+（Spring 5）/ Java 17+（Spring 6） |
| **适用场景** | 客户端 SDK、库、插件 | 服务端应用、Web 应用 |

### 7.2 同一个需求，两种写法对比

#### 需求：选择一个可用的 JSON 序列化器

**Spring IoC 方式**（假想，如果 Nacos 是 Spring 应用）：

```java
// 接口
public interface JsonAdapter {
    String toJson(Object obj);
}

// 实现一（Jackson 2 在 classpath 时自动生效）
@ConditionalOnClass(name = "com.fasterxml.jackson.databind.ObjectMapper")
@ConditionalOnMissingClass("tools.jackson.databind.ObjectMapper")
@Component
@Primary
public class Jackson2JsonAdapter implements JsonAdapter { ... }

// 实现二（Jackson 3 在 classpath 时优先）
@ConditionalOnClass(name = "tools.jackson.databind.ObjectMapper")
@Component
@Primary
public class Jackson3JsonAdapter implements JsonAdapter { ... }

// 使用（容器自动注入选中的那个）
@Service
public class SomeService {
    @Autowired
    private JsonAdapter jsonAdapter;  // 容器自动决定注入哪个

    public String serialize(Object obj) {
        return jsonAdapter.toJson(obj);
    }
}
```

**Nacos SPI 方式**（实际代码）：

```java
// 接口（api 模块）
public interface NacosJsonAdapter {
    String name();
    boolean isAvailable();
    String toJson(Object obj);
}

// 实现一（common 模块，isAvailable 永远返回 true）
public class Jackson2JsonAdapter implements NacosJsonAdapter {
    public boolean isAvailable() { return true; }
    public String toJson(Object obj) { return mapper.writeValueAsString(obj); }
}

// 实现二（common 模块，isAvailable 检查 classpath）
public class Jackson3JsonAdapter implements NacosJsonAdapter {
    public boolean isAvailable() {
        try {
            Class.forName("tools.jackson.databind.ObjectMapper");
            return true;
        } catch (ClassNotFoundException e) { return false; }
    }
}

// 选择器（手写选择逻辑）
class JsonAdapterSelector {
    private NacosJsonAdapter doSelect() {
        // SPI 扫描
        List<NacosJsonAdapter> all = loadAdapters();
        // 过滤可用
        List<NacosJsonAdapter> available = filter(all, NacosJsonAdapter::isAvailable);
        // 优先级选择
        return findFirst(available, "jackson3")
               .orElse(findFirst(available, "jackson2")
               .orElseThrow());
    }
}

// 门面（静态方法）
public class JsonUtils {
    private static volatile JsonAdapterSelector selector = new JsonAdapterSelector();
    public static String toJson(Object obj) { return selector.select().toJson(obj); }
}
```

### 7.3 IoC 容器帮你做了什么（SPI 需要手写的部分）

```
IoC 容器自动完成：                    SPI 需要手写：

1. 发现实现类                        → ServiceLoader.load() 扫描 META-INF/services
2. 实例化                            → Class.forName() + newInstance()
3. 检查是否可用                       → adapter.isAvailable() 手写判断
4. 选择用哪个                         → if-else 优先级逻辑
5. 缓存单例                          → volatile + DCL 双重检查锁
6. 注入到使用方                       → 静态方法调用 selector.select()
7. 配置覆盖                          → System.getProperty() 手动读
8. 延迟初始化                         → lazy delegate() 方法
```

**一句话**：SPI 就是手写的迷你版 IoC — 发现、检查、选择、缓存全自己来。

### 7.4 什么时候用 SPI，什么时候用 IoC

```
                    需要依赖注入吗？
                   /              \
                 是                否
                 |                  |
            用 IoC            需要插件化吗？
           (Spring 等)       /            \
                           是              否
                           |                |
                        用 SPI          直接 new
                     (ServiceLoader)
```

| 场景 | 选择 | 原因 |
|------|------|------|
| 客户端 SDK（Nacos Client、JDBC Driver） | **SPI** | 不能引入重依赖，需轻量 |
| 日志门面（SLF4J） | **SPI** | 绑定日志实现，不能依赖 Spring |
| 服务端应用（Nacos Server） | **IoC** | 本身就是 Spring Boot，享受容器红利 |
| 插件系统（Nacos Auth Plugin） | **SPI** | 第三方插件只需实现接口 + 放 JAR，无需 Spring |
| 微服务业务代码 | **IoC** | 依赖注入、AOP、事务管理等需要容器 |

---

## 八、Nacos 中 SPI 的完整使用清单

Nacos 在以下所有领域都使用了 SPI（211 个 SPI 注册文件）：

| 领域 | SPI 接口 | 选择器/管理器 |
|------|----------|--------------|
| JSON 序列化 | `NacosJsonAdapter` | `JsonAdapterSelector` |
| 客户端鉴权 | `AbstractClientAuthService` | `ClientAuthPluginManager` |
| 服务端鉴权 | `AuthPluginService` | `AuthPluginManager` |
| 服务端身份校验 | `ServerIdentityChecker` | `ServerIdentityCheckerHolder` |
| 鉴权配置 | `NacosAuthConfig` | `NacosAuthConfigHolder` |
| 配置过滤 | `IConfigFilter` | `ConfigFilterChainManager` |
| 配置变更解析 | `ConfigChangeParser` | `ConfigChangeHandler` |
| 灰度规则 | `GrayRule` | 灰度发布引擎 |
| 日志适配 | `NacosLoggingAdapterBuilder` | `NacosLoggingFactory` |
| 故障转移数据源 | `FailoverDataSource` | `FailoverReactor` |
| 服务端列表提供 | `ServerListProvider` | `AbstractServerListManager` |
| 事件发布 | `EventPublisher` | `NotifyCenter` |
| gRPC 消息注册 | `Payload` | `PayloadRegistry` |
| 能力管理 | `AbstractAbilityControlManager` | `NacosAbilityManagerHolder` |
| 参数校验 | `AbstractParamChecker` | `ParamCheckerManager` |
| HTTP 参数提取 | `AbstractHttpParamExtractor` | 各 Controller |
| 标签收集 | `LabelsCollector` | `DefaultLabelsCollectorManager` |
| CMDB 服务 | `CmdbService` | `CmdbProvider` |
| AI 资源存储 | `AiResourceStorageBuilder` | `AiResourceStorageInitializer` |
| AI 资源导入 | `AiResourceImportServiceBuilder` | `AiResourceImportPluginManager` |
| AI 发布管道 | `PublishPipelineServiceBuilder` | `PublishPipelineManager` |
| 统一插件入口 | `PluginProvider` | 插件加载器 |

---

## 九、总结

### 9.1 JsonUtils 的设计模式

```
JsonUtils（门面模式）
  └→ JsonAdapterSelector（策略模式 + SPI 发现）
       └→ NacosJsonAdapter（策略接口）
            ├→ Jackson2JsonAdapter（具体策略 A）
            └→ Jackson3JsonAdapter（具体策略 B + 委托模式延迟加载）
```

### 9.2 SPI 三步走

```
1. 定义接口（api 模块）
2. 实现接口 + 写 META-INF/services 文件（common / plugin 模块）
3. 运行时 ServiceLoader.load() 发现 + 手动选择 + 缓存
```

### 9.3 与 IoC 的本质区别

| | SPI | IoC |
|---|---|---|
| **本质** | 手动版 IoC | 自动版 IoC |
| **发现** | 配置文件 `META-INF/services/` | 注解 `@Component` |
| **选择** | 手写 if-else | `@Primary` / `@Conditional` |
| **缓存** | 手写 DCL | 容器自动管理 |
| **注入** | 不支持 | `@Autowired` |
| **代价** | 代码复杂，但零依赖 | 代码简洁，但需容器 |

**Nacos 选择 SPI 的原因**：`api` 模块和 `client` 模块是纯 Java 库，不能引入 Spring 等重依赖，但又要支持插件化扩展。SPI 是 Java 原生的、零依赖的、标准化的插件发现机制，是客户端 SDK 的最佳选择。

---

## 十、线程安全分析：DCL 双重检查锁

### 10.1 源码回顾

```java
// JsonAdapterSelector.java

// volatile 修饰 —— 这是 DCL 安全的关键前提
private volatile NacosJsonAdapter selectedAdapter;

NacosJsonAdapter select() {
    NacosJsonAdapter adapter = selectedAdapter;      // ① 第一次检查（无锁）
    if (adapter != null) {
        return adapter;                               //    快速路径，直接返回缓存
    }
    synchronized (this) {
        if (selectedAdapter == null) {                // ② 第二次检查（加锁）
            selectedAdapter = doSelect();             // ③ 真正选择（只执行一次）
        }
        return selectedAdapter;
    }
}
```

### 10.2 为什么 `volatile` 是关键

`doSelect()` 内部会创建 `ObjectMapper` 等重量级对象。如果没有 `volatile`，JVM 可能将赋值重排序为：

```
正常顺序（期望）：                重排序后（危险）：
1. 分配内存                        1. 分配内存
2. 执行构造器（初始化字段）          2. selectedAdapter 指向内存  ← 还没初始化！
3. selectedAdapter 指向内存         3. 执行构造器
```

另一个线程在重排序后的步骤 2 和 3 之间进来，看到 `selectedAdapter != null`，直接返回了一个**半初始化的对象**。

`volatile` 通过内存屏障（Memory Barrier）禁止这种重排序，保证步骤 2 先于步骤 3 完成：

```
volatile 写屏障：doSelect() 的构造完成 → happens-before → selectedAdapter 赋值
volatile 读屏障：selectedAdapter 读取 → happens-before → 后续使用该对象
```

### 10.3 完整安全性分析

| 检查点 | 是否安全 | 原因 |
|--------|----------|------|
| `selectedAdapter` 可见性 | ✅ 安全 | `volatile` 保证所有线程看到最新值 |
| `selectedAdapter` 有序性 | ✅ 安全 | `volatile` 禁止指令重排序，防止半初始化对象泄露 |
| `doSelect()` 只执行一次 | ✅ 安全 | `synchronized` + 第二次 null 检查，保证只有一个线程执行初始化 |
| `adapters` 列表 | ✅ 安全 | `final` 修饰，构造后不可变，安全发布 |
| `loadFailures` 写入 | ✅ 安全 | 只在 `doSelect()` 内写入，而 `doSelect()` 在 `synchronized` 块内 |
| 快速路径（无锁读） | ✅ 安全 | `volatile` 读不需要加锁，保证读到完整对象 |

### 10.4 边界情况：`getSelectedAdapter()` 不加锁

```java
NacosJsonAdapter getSelectedAdapter() {
    return selectedAdapter;   // 裸读 volatile，不加锁
}
```

这个方法在 `JsonUtils.registerSubtype()` 中被调用：

```java
public static void registerSubtype(Class<?> baseType, Class<?> subtype, String typeName) {
    // ... 存入 SUBTYPES 列表 ...
    NacosJsonAdapter adapter = selector.getSelectedAdapter();  // 可能为 null
    if (adapter != null) {
        replaySubtypes(adapter);   // 适配器已选好 → 立即回放
    }
    // adapter 为 null → 不回放，先存在 SUBTYPES 待办列表里
    // 后续 adapter() 调用时 replaySubtypes() 会补注册
}
```

**这是故意的设计**，两种时序都安全：

```
时序 A：先注册子类型，后选适配器
  registerSubtype(Http, "HTTP")     → 存入 SUBTYPES，getSelectedAdapter() 返回 null
  JsonUtils.preload()                → select() 选中 Jackson2JsonAdapter
  adapter() 调用 replaySubtypes()   → 把 SUBTYPES 里的记录补注册到适配器 ✅

时序 B：先选适配器，后注册子类型
  JsonUtils.preload()                → select() 选中 Jackson2JsonAdapter
  registerSubtype(Http, "HTTP")     → 存入 SUBTYPES，getSelectedAdapter() 返回非 null
                                     → 立即 replaySubtypes() 注册到适配器 ✅
```

### 10.5 DCL 演进历史

| 时期 | DCL 是否安全 | 原因 |
|------|-------------|------|
| Java 1.4 及以前 | ❌ 不安全 | `volatile` 语义弱，不能完全禁止重排序 |
| Java 5+（JSR-133） | ✅ 安全 | 新内存模型强化了 `volatile` 语义，DCL 成为可靠模式 |
| Java 9+ | ✅ 安全 | 进一步优化了 `volatile` 读写的性能（VarHandle） |

Nacos 的 `api` 模块要求 Java 8+，完全在 JSR-133 生效范围内，DCL 是绝对安全的。

### 10.6 为什么不用更简单的方案

| 方案 | 代码 | 问题 |
|------|------|------|
| 全加锁 | `synchronized` 包整个 `select()` | 每次调用都加锁，性能差 |
| 饿汉式 | 构造器里直接 `doSelect()` | `JsonAdapterSelector` 构造时就会触发 SPI 扫描，无法延迟到 `preload()` |
| 静态内部类 | `static class Holder { static final NacosJsonAdapter INSTANCE = doSelect(); }` | 不能动态选择适配器，且无法支持 `setAdapterSelectorForTest()` 测试替换 |
| DCL | `volatile` + `synchronized` + 二次检查 | 首次加锁，后续无锁读，兼顾安全和性能 ✅ |

**DCL 是这里的最优解**：首次调用有锁但只执行一次（`preload()` 提前消化），后续所有 `toJson()` / `toObj()` 调用都走无锁的快速路径。
