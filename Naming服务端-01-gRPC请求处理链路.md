# Naming 服务端 gRPC 请求处理链路

> **原则声明**：本文档所有结论均来源于 Nacos 3.2.1-SNAPSHOT 源代码的实际引用，不含任何猜测性描述。
> 每一条关键结论都标注**源文件路径 + 行号**，可直接对照源码验证。

---

## 目录

1. [为什么需要一条统一的请求处理链路](#1-为什么需要一条统一的请求处理链路)
2. [四层链路总览](#2-四层链路总览)
3. [GrpcRequestAcceptor：唯一的 gRPC 入口](#3-grpcrequestacceptorgrpc-唯一入口)
4. [RequestHandlerRegistry：类型 → Handler 的路由表](#4-requesthandlerregistry类型--handler-的路由表)
5. [RequestHandler：过滤器链模板方法](#5-requesthandler过滤器链模板方法)
6. [InstanceRequestHandler：一个具体 Handler 的样子](#6-instancerequesthandler一个具体-handler-的样子)
7. [完整数据流转时间线](#7-完整数据流转时间线)
8. [关键设计决策](#8-关键设计决策)
9. [线程与异常边界分析](#9-线程与异常边界分析)
10. [总结](#10-总结)

---

## 1. 为什么需要一条统一的请求处理链路

客户端所有 gRPC 请求（注册实例、订阅、查询、批量注册……）本质上都是一个 `Payload`
Protobuf 消息。服务端面临三个共性问题：

1. **一个入口如何分发到几十种业务逻辑？** —— 不能为每种请求单开一个 gRPC 方法
2. **鉴权、限流、参数校验这些横切关注点如何复用？** —— 不能在每个业务方法里重复写
3. **服务未就绪、连接失效、解析失败等边界如何统一兜底？** —— 不能让每个 Handler 各写一套

Nacos 的答案是一条**四层链路**：

```
统一入口(GrpcRequestAcceptor) → 路由表(RequestHandlerRegistry)
    → 过滤器链模板(RequestHandler.handleRequest) → 具体业务(XxxRequestHandler.handle)
```

一句话概括：**"一个 gRPC 方法 + 一张类型路由表 + 一条过滤器链 + N 个单一职责 Handler"**。

---

## 2. 四层链路总览

```
┌──────────────────────────────────────────────────────────────────┐
│  第 1 层：GrpcRequestAcceptor（唯一 gRPC 入口）      ← core 模块    │
│  request(Payload, StreamObserver) 一个方法接所有请求               │
│  职责：反序列化 → 前置校验 → 找 Handler → 组装 Meta → 回写响应      │
├──────────────────────────────────────────────────────────────────┤
│  第 2 层：RequestHandlerRegistry（类型路由表）       ← core 模块    │
│  Map<String, RequestHandler>：请求类名 → Handler 实例             │
│  职责：启动时反射扫描所有 Handler Bean，建立路由表                  │
├──────────────────────────────────────────────────────────────────┤
│  第 3 层：RequestHandler.handleRequest（过滤器链模板）← core 模块   │
│  先跑 RequestFilters 过滤器链，再调子类 handle()                   │
│  职责：横切关注点（过滤器）与业务逻辑（handle）的分离               │
├──────────────────────────────────────────────────────────────────┤
│  第 4 层：InstanceRequestHandler.handle（具体业务）  ← naming 模块  │
│  @Secured/@TpsControl/@ExtractorManager 注解治理                   │
│  职责：解析领域对象 → 调用 ClientOperationService → 发 Trace 事件   │
└──────────────────────────────────────────────────────────────────┘
```

第 1~3 层在 `core` 模块（所有模块共享），第 4 层在 `naming` 模块（业务专属）。
这条链路是 config、naming、AI 等所有模块**共用**的骨架。

---

## 3. GrpcRequestAcceptor：gRPC 唯一入口

> 源码：[GrpcRequestAcceptor.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcRequestAcceptor.java) L56

```java
@Service
public class GrpcRequestAcceptor extends RequestGrpc.RequestImplBase {

    @Autowired
    RequestHandlerRegistry requestHandlerRegistry;

    @Autowired
    private ConnectionManager connectionManager;
```

`RequestGrpc.RequestImplBase` 是 Protobuf 生成的 gRPC 服务基类。整个服务端**只重写一个
方法** `request(Payload, StreamObserver)`，所有客户端请求都从这里进来。

### 3.1 request()：一个方法接所有请求

> 源码：[GrpcRequestAcceptor.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcRequestAcceptor.java) L84-88

```java
@Override
public void request(Payload grpcRequest, StreamObserver<Payload> responseObserver) {
    traceIfNecessary(grpcRequest, true);
    String type = grpcRequest.getMetadata().getType();   // ← 从元数据取"请求类型"
    long startTime = System.nanoTime();
```

**关键点**：请求的"类型"不在 body 里，而在 `Payload.metadata.type` 里，值就是请求类的
简单类名（如 `"InstanceRequest"`）。这个 `type` 是后续路由的唯一依据。

### 3.2 五道前置关卡（fail-fast）

进入业务逻辑前，`request()` 按顺序设置了五道关卡，任何一道不通过立即返回错误，
不会进入 Handler：

| 顺序 | 检查 | 不通过时返回 | 源码行号 |
|:---:|------|-------------|----------|
| ① | 服务端是否已启动 `ApplicationUtils.isStarted()` | `INVALID_SERVER_STATUS` | L92-103 |
| ② | 是否为 `ServerCheckRequest`（建连探活）| 直接回 `ServerCheckResponse` | L106-115 |
| ③ | 路由表能否找到 Handler | `NO_HANDLER` | L117-131 |
| ④ | 连接是否合法 `connectionManager.checkValid` | `UN_REGISTER` | L133-149 |
| ⑤ | Payload 能否解析成 `Request` | `BAD_GATEWAY` | L151-197 |

以第 ③ 道为例：

> 源码：[GrpcRequestAcceptor.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcRequestAcceptor.java) L117-131

```java
RequestHandler requestHandler = requestHandlerRegistry.getByRequestType(type);
//no handler found.
if (requestHandler == null) {
    Loggers.REMOTE_DIGEST
        .warn(String.format("[%s] No handler for request type : %s :", "grpc", type));
    Payload payloadResponse = GrpcUtils
        .convert(ErrorResponse.build(NacosException.NO_HANDLER, "RequestHandler Not Found"));
    traceIfNecessary(payloadResponse, false);
    responseObserver.onNext(payloadResponse);
    responseObserver.onCompleted();
    MetricsMonitor.recordGrpcRequestEvent(type, false,
        NacosException.NO_HANDLER, null, null, System.nanoTime() - startTime);
    return;
}
```

**为什么要 fail-fast？** gRPC 入口是所有请求的必经之路，这里的每一次多余处理都会放大
到全流量。把"服务没起来""没有对应处理器""连接非法"这些不可能成功的请求尽早挡掉，
避免污染下游业务逻辑，也让每个错误都有明确的 `NacosException` 错误码。

### 3.3 组装 RequestMeta 并调用 Handler

> 源码：[GrpcRequestAcceptor.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcRequestAcceptor.java) L199-226

```java
Request request = (Request) parseObj;
try {
    Connection connection =
        connectionManager.getConnection(GrpcServerConstants.CONTEXT_KEY_CONN_ID.get());
    RequestMeta requestMeta = new RequestMeta();
    requestMeta.setClientIp(connection.getMetaInfo().getClientIp());
    requestMeta.setConnectionId(GrpcServerConstants.CONTEXT_KEY_CONN_ID.get());
    requestMeta.setClientVersion(connection.getMetaInfo().getVersion());
    requestMeta.setLabels(connection.getMetaInfo().getLabels());
    requestMeta.setAbilityTable(connection.getAbilityTable());
    connectionManager.refreshActiveTime(requestMeta.getConnectionId());   // ← 刷新活跃时间=心跳
    prepareRequestContext(request, requestMeta, connection);
    Response response = requestHandler.handleRequest(request, requestMeta);  // ← 进入第 3 层
    Payload payloadResponse = GrpcUtils.convert(response);
    ...
```

三个值得注意的细节：

1. **`RequestMeta` 是业务逻辑拿不到网络层信息的桥梁**：clientIp、connectionId、
   clientVersion、能力表都从 `Connection` 拷进 `RequestMeta`，Handler 只依赖 `RequestMeta`，
   不直接碰 gRPC 连接对象。上一节 [`InstanceRequestHandler`](#6-instancerequesthandler一个具体-handler-的样子)
   里的 `meta.getConnectionId()` 就来自这里。
2. **`refreshActiveTime()` L209**：每收到一个请求就刷新连接活跃时间——这就是 gRPC 长连接
   "有请求即视为存活"的心跳本质（详见健康检查篇）。
3. **`OVER_THRESHOLD` 限流响应延迟 1 秒回写 L214-219**：被限流的请求不立即返回，故意
   delay 1s，起到"背压/惩罚"作用，减缓客户端重试风暴。

### 3.4 RequestContext 的设置与清理

> 源码：[GrpcRequestAcceptor.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcRequestAcceptor.java) L239-241

```java
} finally {
    RequestContextHolder.removeContext();   // ← 无论成败都清理 ThreadLocal
}
```

`prepareRequestContext()`（L245-263）把 requestId、协议、App、来源 IP 等塞进
`RequestContextHolder`（基于 ThreadLocal）。`finally` 块保证请求结束时一定清理，
**防止线程复用导致的上下文串台**——这是所有 ThreadLocal 用法的铁律。

---

## 4. RequestHandlerRegistry：类型 → Handler 的路由表

第 1 层靠 `requestHandlerRegistry.getByRequestType(type)` 找到 Handler。这张路由表是
怎么建起来的？

> 源码：[RequestHandlerRegistry.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/RequestHandlerRegistry.java) L45-59

```java
@Service
public class RequestHandlerRegistry implements ApplicationListener<ContextRefreshedEvent> {

    Map<String, RequestHandler> registryHandlers = new HashMap<>();

    public RequestHandler getByRequestType(String requestType) {
        return registryHandlers.get(requestType);
    }
```

`registryHandlers` 就是那张表：**key = 请求类简单名，value = Handler 实例**。

### 4.1 启动时反射建表

`RequestHandlerRegistry` 实现了 `ApplicationListener<ContextRefreshedEvent>`，Spring 容器
刷新完成后回调 `onApplicationEvent`，一次性把所有 Handler 注册进表：

> 源码：[RequestHandlerRegistry.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/RequestHandlerRegistry.java) L76-124

```java
@Override
public void onApplicationEvent(ContextRefreshedEvent event) {
    Map<String, RequestHandler> beansOfType =
        event.getApplicationContext().getBeansOfType(RequestHandler.class);   // ① 捞出所有 Handler Bean
    Collection<RequestHandler> values = beansOfType.values();
    for (RequestHandler requestHandler : values) {

        Class<?> clazz = requestHandler.getClass();
        boolean skip = false;
        // ② 向上找到直接继承 RequestHandler 的那一层（穿透中间抽象类）
        while (!clazz.getSuperclass().equals(RequestHandler.class)) {
            if (clazz.getSuperclass().equals(Object.class)) {
                skip = true;
                break;
            }
            clazz = clazz.getSuperclass();
        }
        if (skip) {
            continue;
        }
        // ③ 从泛型参数 RequestHandler<T,S> 中解析出 T（请求类型）
        Class tClass = (Class) ((ParameterizedType) clazz.getGenericSuperclass())
            .getActualTypeArguments()[0];
        ...
        // ④ 以"请求类简单名"为 key 注册
        registryHandlers.putIfAbsent(tClass.getSimpleName(), requestHandler);
    }
}
```

**核心技巧**：通过 `getGenericSuperclass()` 反射拿到 `RequestHandler<InstanceRequest,
InstanceResponse>` 里的第一个泛型参数 `InstanceRequest`，用它的简单名 `"InstanceRequest"`
作为路由 key。而客户端发请求时，`Payload.metadata.type` 填的正是 `"InstanceRequest"`，
两端天然对齐，无需任何手工映射配置。

### 4.2 顺带完成的两件治理注册

同一个循环里还做了两件横切治理的注册（都是"读注解 → 注册到控制中心"）：

| 治理 | 依据注解 | 动作 | 源码行号 |
|------|---------|------|----------|
| TPS 限流点 | 方法上的 `@TpsControl` | 向 `ControlManagerCenter` 注册限流点 | L95-106 |
| 调用来源限制 | 类上的 `@InvokeSource` | 记录该类型允许的调用来源 | L112-119 |

这解释了为什么 [`InstanceRequestHandler`](#6-instancerequesthandler一个具体-handler-的样子)
的 `handle` 方法上标 `@TpsControl` 就能生效——注册动作在启动时已经悄悄完成。

---

## 5. RequestHandler：过滤器链模板方法

找到 Handler 后，第 1 层调的是 `handleRequest()`（不是 `handle()`）。这两个方法的分工
是整条链路的设计精华。

> 源码：[RequestHandler.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/RequestHandler.java) L32-68

```java
public abstract class RequestHandler<T extends Request, S extends Response> {

    @Autowired
    private RequestFilters requestFilters;

    // 模板方法：固定"先过滤器链、后业务"的骨架
    public Response handleRequest(T request, RequestMeta meta) throws NacosException {
        for (AbstractRequestFilter filter : requestFilters.filters) {
            try {
                Response filterResult = filter.filter(request, meta, this.getClass());
                if (filterResult != null && !filterResult.isSuccess()) {
                    return filterResult;      // ← 任一过滤器返回失败，短路，不进业务
                }
            } catch (Throwable throwable) {
                Loggers.REMOTE.error("filter error", throwable);
            }
        }
        return handle(request, meta);          // ← 过滤器全通过，交给子类业务实现
    }

    // 抽象方法：留给每个具体 Handler 实现自己的业务
    public abstract S handle(T request, RequestMeta meta) throws NacosException;
}
```

这是标准的**模板方法模式**：
- `handleRequest()` 是 `final` 级别的骨架（先过滤器链、再业务），所有 Handler 共享；
- `handle()` 是抽象钩子，每个 Handler 填自己的业务。

### 5.1 过滤器链是怎么组装的

> 源码：[RequestFilters.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/RequestFilters.java) L30-38

```java
@Service
public class RequestFilters {
    List<AbstractRequestFilter> filters = new ArrayList<>();

    public void registerFilter(AbstractRequestFilter requestFilter) {
        filters.add(requestFilter);
    }
}
```

过滤器自己"上门登记"——每个 `AbstractRequestFilter` 子类在 `@PostConstruct` 时把自己
注册进链：

> 源码：[AbstractRequestFilter.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/AbstractRequestFilter.java) L36-47

```java
public abstract class AbstractRequestFilter {

    @Autowired
    private RequestFilters requestFilters;

    @PostConstruct
    public void init() {
        requestFilters.registerFilter(this);   // ← Bean 初始化时自注册
    }
```

**这是一个"自注册"的责任链**：新增一个过滤器，只要写个 `@Component` 继承
`AbstractRequestFilter`，Spring 一初始化它就自动进链，无需改动 `RequestHandler`
或任何注册代码——典型的开闭原则。

### 5.2 过滤器能拿到 Handler 的类型信息

注意 `filter.filter(request, meta, this.getClass())` 把**当前 Handler 的 Class** 传给了
过滤器。`AbstractRequestFilter` 提供了两个工具方法，让过滤器能反射出 Handler 的
`handle` 方法（读其注解）和默认响应实例：

> 源码：[AbstractRequestFilter.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/AbstractRequestFilter.java) L49-68

```java
protected Method getHandleMethod(Class handlerClazz) throws NacosException {
    Method method = handlerClazz.getMethod("handle", Request.class, RequestMeta.class);
    return method;
}

protected <T> Response getDefaultResponseInstance(Class handlerClazz) throws NacosException {
    ParameterizedType parameterizedType = (ParameterizedType) handlerClazz.getGenericSuperclass();
    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
    return (Response) Class.forName(actualTypeArguments[1].getTypeName()).newInstance();
}
```

这就是鉴权过滤器如何读到 `handle` 方法上的 `@Secured` 注解、并在拒绝时构造出正确类型
响应（`InstanceResponse` 而非泛泛的 `Response`）的底层机制。

---

## 6. InstanceRequestHandler：一个具体 Handler 的样子

前面三层都是共享骨架，到第 4 层才是 naming 的业务。以最典型的注册/注销 Handler 为例。

> 源码：[InstanceRequestHandler.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/remote/rpc/handler/InstanceRequestHandler.java) L47-78

```java
@Since("2.0.0")
@Component
public class InstanceRequestHandler extends RequestHandler<InstanceRequest, InstanceResponse> {

    private final EphemeralClientOperationServiceImpl clientOperationService;

    public InstanceRequestHandler(EphemeralClientOperationServiceImpl clientOperationService) {
        this.clientOperationService = clientOperationService;
    }

    @Override
    @NamespaceValidation                                          // 命名空间合法性校验
    @TpsControl(pointName = "RemoteNamingInstanceRegisterDeregister",
        name = "RemoteNamingInstanceRegisterDeregister")         // 流量控制
    @Secured(action = ActionTypes.WRITE)                         // 权限校验(写)
    @ExtractorManager.Extractor(rpcExtractor = InstanceRequestParamExtractor.class)  // 参数提取
    public InstanceResponse handle(InstanceRequest request, RequestMeta meta)
        throws NacosException {
        Service service = Service.newService(request.getNamespace(), request.getGroupName(),
            request.getServiceName(), true);
        InstanceUtil.setInstanceIdIfEmpty(request.getInstance(), service.getGroupedServiceName());
        switch (request.getType()) {                              // ← 二级分发
            case NamingRemoteConstants.REGISTER_INSTANCE:
                return registerInstance(service, request, meta);
            case NamingRemoteConstants.DE_REGISTER_INSTANCE:
                return deregisterInstance(service, request, meta);
            default:
                throw new NacosException(NacosException.INVALID_PARAM,
                    String.format("Unsupported request type %s", request.getType()));
        }
    }
```

### 6.1 两级分发

整条链路其实做了**两级分发**：

```
一级分发(路由表)：type="InstanceRequest" → InstanceRequestHandler
二级分发(switch)：request.getType()=REGISTER_INSTANCE → registerInstance()
                                     =DE_REGISTER_INSTANCE → deregisterInstance()
```

注册和注销共用一个 Handler、一个限流点、一套鉴权，仅用 `switch` 区分——因为它们的
横切治理（写权限、同一限流维度）完全一致，没必要拆成两个类。`default` 分支抛
`INVALID_PARAM`，满足"switch 必须有 default"的规约。

### 6.2 Handler 里只有三件事

> 源码：[InstanceRequestHandler.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/remote/rpc/handler/InstanceRequestHandler.java) L80-90

```java
private InstanceResponse registerInstance(Service service, InstanceRequest request,
    RequestMeta meta) throws NacosException {
    // ① 调用业务服务（真正改数据模型的地方，见"客户端操作服务"篇）
    clientOperationService.registerInstance(service, request.getInstance(),
        meta.getConnectionId());
    // ② 发布 Trace 事件（可观测，异步，不影响主流程）
    NotifyCenter.publishEvent(new RegisterInstanceTraceEvent(System.currentTimeMillis(),
        NamingRequestUtil.getSourceIpForGrpcRequest(meta), true, service.getNamespace(),
        service.getGroup(), service.getName(),
        request.getInstance().getIp(), request.getInstance().getPort()));
    // ③ 返回响应
    return new InstanceResponse(NamingRemoteConstants.REGISTER_INSTANCE);
}
```

Handler 层永远是"薄"的：**解析领域对象 → 委托业务服务 → 发事件 → 返回响应**。真正的
状态变更（改 Client 模型、发 ClientRegisterServiceEvent）在
`EphemeralClientOperationServiceImpl` 里，这是下一篇的主题。

---

## 7. 完整数据流转时间线

以一次 `registerInstance` 为例，串联四层：

```
客户端 rpcClient.request(InstanceRequest)  →  gRPC 网络
   │  Payload.metadata.type = "InstanceRequest"
   ▼
[第1层] GrpcRequestAcceptor.request(Payload, observer)   core L85
   ├─ 关卡① 服务已启动?           L92
   ├─ 关卡② ServerCheckRequest?  L106（否，跳过）
   ├─ 关卡③ getByRequestType("InstanceRequest") → InstanceRequestHandler   L117
   ├─ 关卡④ 连接合法?             L135
   ├─ 关卡⑤ GrpcUtils.parse → InstanceRequest 对象   L153
   ├─ 组装 RequestMeta(connId/clientIp/version/abilityTable)   L203-208
   ├─ refreshActiveTime(connId)  ← 长连接心跳   L209
   └─ requestHandler.handleRequest(request, meta)   L211
        │
        ▼
   [第3层] RequestHandler.handleRequest()   core L45
        ├─ for filter : filters → filter.filter(req, meta, InstanceRequestHandler.class)
        │     任一失败 → 短路返回   L46-56
        └─ handle(request, meta)   L57
             │
             ▼
        [第4层] InstanceRequestHandler.handle()   naming L63
             ├─ Service.newService(ns, group, name, ephemeral=true)   L65
             ├─ switch(REGISTER_INSTANCE) → registerInstance()   L70
             │     ├─ clientOperationService.registerInstance(...)  ← 改数据模型
             │     └─ NotifyCenter.publishEvent(RegisterInstanceTraceEvent)  ← 可观测
             └─ return InstanceResponse(REGISTER_INSTANCE)   L89
        ▲
        │ Response
   [第1层] GrpcUtils.convert(response) → Payload   L212
        ├─ OVER_THRESHOLD? → delay 1s 回写  L214
        └─ observer.onNext(payload) + onCompleted()   L221-223
        finally: RequestContextHolder.removeContext()   L240
   ▼
客户端收到 InstanceResponse
```

---

## 8. 关键设计决策

### 8.1 为什么用"一个 gRPC 方法 + 类型路由表"而不是"一个请求一个 gRPC 方法"

**决策理由**：
1. **协议稳定**：新增业务请求只需加一个 Handler + 一个 Request 类，`.proto` 不用改、
   不用重新生成 stub、不用升级双端协议。
2. **横切统一**：所有请求走同一个 `request()`，服务状态检查、连接校验、限流、trace、
   context 清理只写一遍。
3. **能力协商友好**：客户端可以发服务端"不认识"的新请求类型，服务端统一回 `NO_HANDLER`，
   而不是 gRPC 层面的 `UNIMPLEMENTED`，错误语义更清晰。

### 8.2 为什么路由 key 用"请求类的简单名"

**决策理由**：`RequestHandlerRegistry` 用反射从 `RequestHandler<T,S>` 解析出 `T` 的简单名
作 key（L108-124），客户端 `Payload.metadata.type` 也填这个简单名。两端都以类名为契约，
**省掉一张手工维护的 type→handler 映射表**，也杜绝了映射写错的可能。代价是请求类不能重名。

### 8.3 为什么 handleRequest / handle 要拆成两个方法

**决策理由**：模板方法模式。`handleRequest` 固化"过滤器链 → 业务"的顺序（横切与业务分离），
`handle` 只写业务。过滤器通过 `@PostConstruct` 自注册（开闭原则），加鉴权/限流/校验过滤器
不用碰任何 Handler。

### 8.4 为什么 Handler 里还要再 switch 一次

**决策理由**：当多个操作**共享同一套横切治理**（同样的写权限、同一个限流维度）时，合成一个
Handler 用 `switch` 二级分发，比拆成多个 Handler 更省——少一份 `@Secured`/`@TpsControl`
声明，限流也天然按"注册+注销"合并计量。注册/注销就是这种关系。

---

## 9. 线程与异常边界分析

### 9.1 异常兜底：Handler 抛什么都不会漏

`request()` 的业务调用被包在 `try/catch(Throwable)` 里：

> 源码：[GrpcRequestAcceptor.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcRequestAcceptor.java) L227-238

```java
} catch (Throwable e) {
    Loggers.REMOTE_DIGEST.error("[{}] Fail to handle request from connection [{}]...", ...);
    Payload payloadResponse = GrpcUtils.convert(ErrorResponse.build(e));  // 转成 ErrorResponse
    responseObserver.onNext(payloadResponse);
    responseObserver.onCompleted();
    MetricsMonitor.recordGrpcRequestEvent(type, false, ResponseCode.FAIL.getCode(), ...);
}
```

任何 Handler 内部异常（含 `RuntimeException`）都被转成 `ErrorResponse` 正常回写客户端，
**gRPC 流不会被异常打断**，客户端总能拿到一个结构化的错误码，而不是连接层面的 `UNKNOWN`。

### 9.2 过滤器异常被吞

第 3 层过滤器循环里 `catch(Throwable)` 只打日志、不中断（`RequestHandler` L52-54）。
**含义**：某个过滤器自身抛异常时，视为"该过滤器未拦截"，请求继续向下走。这是可用性
优先的取舍——过滤器 bug 不应导致所有请求瘫痪，但也意味着过滤器逻辑必须自己保证健壮。

### 9.3 ThreadLocal 必须清理

`RequestContextHolder` 基于 ThreadLocal，gRPC 线程是**复用**的。`finally` 块
（L239-241）无条件 `removeContext()`，防止上一个请求的 requestId/App/来源 IP 泄漏到
复用该线程的下一个请求。

### 9.4 限流响应的"延迟惩罚"

被限流（`OVER_THRESHOLD`）的响应通过 `RpcScheduledExecutor.CONTROL_SCHEDULER` 延迟
1 秒回写（L214-219）。这是有意的**背压**：不立即告诉客户端"被限流了"，拖慢它的重试
节奏，避免限流触发瞬间的重试风暴把服务端压垮。

---

## 10. 总结

### 10.1 一条链路，四层职责

```
GrpcRequestAcceptor   （唯一入口 / 5 道关卡 / 组装 Meta / 回写 / 清理 context）
      │  getByRequestType(type)
      ▼
RequestHandlerRegistry（启动反射建表：请求类简单名 → Handler）
      │  handleRequest()
      ▼
RequestHandler        （模板方法：过滤器链 → handle）
      │  handle()
      ▼
InstanceRequestHandler（注解治理 + switch 二级分发 + 委托业务服务 + 发 Trace 事件）
```

### 10.2 贯穿全链路的三个"不"

- 业务 Handler **不碰** gRPC 连接对象（只依赖 `RequestMeta`）
- 业务 Handler **不写** 横切逻辑（鉴权/限流/校验交给注解 + 过滤器链）
- 协议层 **不因** 业务异常中断（`Throwable` 统一转 `ErrorResponse`）

### 10.3 关键文件索引

| 类 | 相对路径 | 角色 |
|----|---------|------|
| GrpcRequestAcceptor | `core/.../remote/grpc/GrpcRequestAcceptor.java` | gRPC 唯一入口，5 道关卡 |
| RequestHandlerRegistry | `core/.../remote/RequestHandlerRegistry.java` | 类型 → Handler 路由表 |
| RequestHandler | `core/.../remote/RequestHandler.java` | 过滤器链模板方法 |
| RequestFilters | `core/.../remote/RequestFilters.java` | 过滤器链容器 |
| AbstractRequestFilter | `core/.../remote/AbstractRequestFilter.java` | 过滤器基类（自注册） |
| InstanceRequestHandler | `naming/.../remote/rpc/handler/InstanceRequestHandler.java` | 注册/注销业务 Handler |

> 下一篇：**客户端操作服务（注册/注销/订阅）**——`EphemeralClientOperationServiceImpl`
> 如何把一次注册变成"改 Client 模型 → 发事件"，以及事件如何驱动索引、推送、集群同步。
