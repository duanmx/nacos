# Nacos gRPC 服务器启动流程源码解析

> **版本**: Nacos 3.2.1-SNAPSHOT | **模块**: core / bootstrap  
> **技术栈**: gRPC (shaded Netty) + Spring Boot  
> **分析原则**: 每句话都有源码路径和行号支撑

---

## 一、概述

Nacos 服务端启动时会同时启动 **两个** gRPC 服务器，分别服务于不同场景：

| 服务器 | 用途 | 端口偏移 | 默认端口 |
|--------|------|---------|---------|
| **GrpcSdkServer** | SDK 客户端通信（服务发现、配置管理等） | `+1000` | 9848 |
| **GrpcClusterServer** | 集群节点间内部通信（数据同步等） | `+1001` | 9849 |

两者均基于 gRPC shaded Netty（`io.grpc.netty.shaded`）构建，通过 Spring 的 `@PostConstruct` 自动启动。

---

## 二、类层次结构

```
BaseRpcServer                              (抽象类, 定义 @PostConstruct 启动入口)
  ├── start() @PostConstruct               ← Spring 容器启动时自动调用
  ├── getServicePort() = server.port + rpcPortOffset()
  └── startServer() [抽象]                  ← 模板方法, 子类实现

BaseGrpcServer extends BaseRpcServer       (抽象类, gRPC NettyServer 构建逻辑)
  ├── startServer()                         ← 构建并启动 NettyServerBuilder
  ├── addServices()                         ← 注册 gRPC 服务方法
  ├── Server server                         ← io.grpc.Server 实例
  └── getRpcExecutor() [抽象]               ← 线程池

├── GrpcSdkServer extends BaseGrpcServer    (@Service, SDK 客户端通信)
│     rpcPortOffset() = 1000
│     getSource() = "sdk"
│     协议协商器: SdkProtocolNegotiatorBuilderSingleton
│
└── GrpcClusterServer extends BaseGrpcServer (@Service, 集群内部通信)
      rpcPortOffset() = 1001
      getSource() = "cluster"
      协议协商器: ClusterProtocolNegotiatorBuilderSingleton
```

**源码位置**：

- [BaseRpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/BaseRpcServer.java#L33-L126)
- [BaseGrpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/BaseGrpcServer.java#L64-L275)
- [GrpcSdkServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcSdkServer.java#L45-L138)
- [GrpcClusterServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcClusterServer.java#L45-L140)

---

## 三、启动入口：NacosBootstrap

**文件**: [NacosBootstrap.java](file:///Users/mmhm/IdeaProjects/nacos/bootstrap/src/main/java/com/alibaba/nacos/bootstrap/NacosBootstrap.java#L43-L65)

```java
@SpringBootApplication
public class NacosBootstrap {
    public static void main(String[] args) {
        String type = System.getProperty(Constants.NACOS_DEPLOYMENT_TYPE,
            Constants.NACOS_DEPLOYMENT_TYPE_MERGED);
        DeploymentType deploymentType = DeploymentType.getType(type);
        switch (deploymentType) {
            case MERGED:   startWithConsole(args);   break;
            case SERVER:   startWithoutConsole(args); break;
            case CONSOLE:  startOnlyConsole(args);   break;
        }
    }
}
```

以 **SERVER 模式**为例，进入 `startWithoutConsole`：

```java
private static void startWithoutConsole(String[] args) {
    ConfigurableApplicationContext coreContext = startCoreContext(args);    // ① 启动 Core 上下文
    prepareCoreContext(coreContext);
    ConfigurableApplicationContext webContext = startServerWebContext(args, coreContext); // ② 启动 Web 上下文
    // ...
}
```

### 为什么 gRPC 服务器在 Core 上下文中？

`startCoreContext` 创建的是 `NacosServerBasicApplication` 上下文：

```java
private static ConfigurableApplicationContext startCoreContext(String[] args) {
    NacosStartUpManager.start(NacosStartUp.CORE_START_UP_PHASE);
    return new SpringApplicationBuilder(NacosServerBasicApplication.class)
        .web(WebApplicationType.NONE)
        .banner(getBanner("core-banner.txt")).run(args);
}
```

`GrpcSdkServer` 和 `GrpcClusterServer` 都标注了 `@Service`，且都在 `core` 模块下，因此它们随 Core 上下文初始化而实例化。**这保证了 gRPC 服务器在 REST API 之前就准备好接收请求**。

---

## 四、启动触发：BaseRpcServer.start() — @PostConstruct

**文件**: [BaseRpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/BaseRpcServer.java#L42-L69)

```java
@PostConstruct
public void start() throws Exception {
    String serverName = getClass().getSimpleName();
    Loggers.REMOTE.info("Nacos {} Rpc server starting at port {}", serverName, getServicePort());

    startServer();  // ① 模板方法, 由 BaseGrpcServer 实现

    // ② 注册 SSL/TLS 上下文刷新器
    if (RpcServerSslContextRefresherHolder.getSdkInstance() != null) {
        RpcServerSslContextRefresherHolder.getSdkInstance().refresh(this);
    }
    if (RpcServerSslContextRefresherHolder.getClusterInstance() != null) {
        RpcServerSslContextRefresherHolder.getClusterInstance().refresh(this);
    }

    Loggers.REMOTE.info("Nacos {} Rpc server started at port {}", serverName, getServicePort());

    // ③ 注册 JVM ShutdownHook, 保证优雅关闭
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        Loggers.REMOTE.info("Nacos {} Rpc server stopping", serverName);
        try {
            BaseRpcServer.this.stopServer();
            Loggers.REMOTE.info("Nacos {} Rpc server stopped successfully...", serverName);
        } catch (Exception e) {
            Loggers.REMOTE.error("Nacos {} Rpc server stopped fail...", serverName, e);
        }
    }));
}
```

### 端口计算

**文件**: [BaseRpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/BaseRpcServer.java#L107-L109)

```java
public int getServicePort() {
    return EnvUtil.getPort() + rpcPortOffset();
}
```

端口偏移常量定义在 [Constants.java](file:///Users/mmhm/IdeaProjects/nacos/api/src/main/java/com/alibaba/nacos/api/common/Constants.java#L102-L104)：

```java
public static final Integer SDK_GRPC_PORT_DEFAULT_OFFSET     = 1000;
public static final Integer CLUSTER_GRPC_PORT_DEFAULT_OFFSET  = 1001;
```

| 服务器 | `server.port`=8848 | `server.port`=7848 |
|--------|--------------------|--------------------|
| GrpcSdkServer | 9848 | 8848 |
| GrpcClusterServer | 9849 | 8849 |

---

## 五、核心启动逻辑：BaseGrpcServer.startServer()

**文件**: [BaseGrpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/BaseGrpcServer.java#L91-L120)

```java
@Override
public void startServer() throws Exception {
    // 步骤 1: 注册 gRPC 服务方法
    final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();
    addServices(handlerRegistry, getSeverInterceptors().toArray(new ServerInterceptor[0]));

    // 步骤 2: 确定监听地址
    String grpcListenIp = InetUtils.getGrpcListenIp();
    InetSocketAddress inetSocketAddress = StringUtils.isNotBlank(grpcListenIp)
        ? new InetSocketAddress(grpcListenIp, getServicePort())
        : new InetSocketAddress(getServicePort());

    // 步骤 3: 构建 NettyServerBuilder
    NettyServerBuilder builder = NettyServerBuilder.forAddress(inetSocketAddress)
        .executor(getRpcExecutor());

    // 步骤 4: 配置协议协商器 (TLS/SSL)
    Optional<InternalProtocolNegotiator.ProtocolNegotiator> negotiator = newProtocolNegotiator();
    if (negotiator.isPresent()) {
        builder.protocolNegotiator(negotiator.get());
    }

    // 步骤 5: 添加传输过滤器
    for (ServerTransportFilter each : getServerTransportFilters()) {
        builder.addTransportFilter(each);
    }

    // 步骤 6: 设置 gRPC 参数并构建
    server = builder
        .maxInboundMessageSize(getMaxInboundMessageSize())
        .fallbackHandlerRegistry(handlerRegistry)
        .compressorRegistry(CompressorRegistry.getDefaultInstance())
        .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
        .keepAliveTime(getKeepAliveTime(), TimeUnit.MILLISECONDS)
        .keepAliveTimeout(getKeepAliveTimeout(), TimeUnit.MILLISECONDS)
        .permitKeepAliveTime(getPermitKeepAliveTime(), TimeUnit.MILLISECONDS)
        .build();

    // 步骤 7: 启动
    server.start();
}
```

---

## 六、六步详解

### 6.1 注册 gRPC 服务方法 — addServices()

**文件**: [BaseGrpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/BaseGrpcServer.java#L215-L259)

`addServices` 注册了 **两个** gRPC 方法：

#### 方法一：一元调用（Unary）

| 属性 | 值 |
|------|-----|
| 服务名 | `Request` |
| 方法名 | `request` |
| 类型 | `UNARY` |
| Marshaller | `Payload` (Protobuf) |
| 处理器 | `ServerCalls.asyncUnaryCall` → `handleCommonRequest()` |

```java
final MethodDescriptor<Payload, Payload> unaryPayloadMethod = MethodDescriptor
    .<Payload, Payload>newBuilder()
    .setType(MethodDescriptor.MethodType.UNARY)
    .setFullMethodName(MethodDescriptor.generateFullMethodName(
        GrpcServerConstants.REQUEST_SERVICE_NAME,    // "Request"
        GrpcServerConstants.REQUEST_METHOD_NAME))     // "request"
    .setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
    .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
    .build();

final ServerCallHandler<Payload, Payload> payloadHandler = ServerCalls.asyncUnaryCall(
    (request, responseObserver) -> handleCommonRequest(request, responseObserver));
```

**请求路由逻辑** — `handleCommonRequest`：

```java
protected void handleCommonRequest(Payload grpcRequest,
    StreamObserver<Payload> responseObserver) {
    if (!invokeSourceAllowCheck(grpcRequest)) {
        // 拒绝：返回 BAD_GATEWAY 错误
        responseObserver.onNext(GrpcUtils.convert(ErrorResponse.build(...)));
        responseObserver.onCompleted();
    } else {
        // 允许：委托给 GrpcRequestAcceptor → RequestHandlerRegistry 分发
        grpcCommonRequestAcceptor.request(grpcRequest, responseObserver);
    }
}
```

`invokeSourceAllowCheck` 通过 `RequestHandlerRegistry.checkSourceInvokeAllowed()` 验证请求来源（SDK 或 Cluster）是否有权限调用该类型的请求。

#### 方法二：双向流（Bidi-streaming）

| 属性 | 值 |
|------|-----|
| 服务名 | `BiRequestStream` |
| 方法名 | `requestBiStream` |
| 类型 | `BIDI_STREAMING` |
| 处理器 | `GrpcBiStreamRequestAcceptor.requestBiStream()` |

```java
final ServerCallHandler<Payload, Payload> biStreamHandler =
    ServerCalls.asyncBidiStreamingCall(
        (responseObserver) -> grpcBiStreamRequestAcceptor.requestBiStream(responseObserver));

final MethodDescriptor<Payload, Payload> biStreamMethod =
    MethodDescriptor.<Payload, Payload>newBuilder()
        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(
            GrpcServerConstants.REQUEST_BI_STREAM_SERVICE_NAME,   // "BiRequestStream"
            GrpcServerConstants.REQUEST_BI_STREAM_METHOD_NAME))   // "requestBiStream"
        .setRequestMarshaller(ProtoUtils.marshaller(Payload.newBuilder().build()))
        .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
        .build();
```

双向流主要用于**服务变更推送**：服务端可以持续向客户端推送变更事件，客户端也可以持续发送请求，无需反复建立连接。

### 6.2 确定监听地址

- 优先使用 `-Dnacos.remote.server.grpc.ip=xxx` 指定的 IP
- 否则监听 `0.0.0.0`（所有网卡）
- 端口 = `server.port + rpcPortOffset()`

### 6.3 构建 NettyServerBuilder

使用 **gRPC shaded Netty**，线程池由子类提供：

| 服务器 | 线程池 |
|--------|--------|
| GrpcSdkServer | `GlobalExecutor.sdkRpcExecutor` |
| GrpcClusterServer | `GlobalExecutor.clusterRpcExecutor`（`allowCoreThreadTimeOut(true)`）|

Cluster 线程池允许核心线程超时，因为集群间通信流量相对较低，空闲时可以回收线程。

### 6.4 协议协商器 — TLS/SSL

两个服务器的区别：

- **GrpcSdkServer**: 使用 `SdkProtocolNegotiatorBuilderSingleton.getSingleton().build()`
- **GrpcClusterServer**: 使用 `ClusterProtocolNegotiatorBuilderSingleton.getSingleton().build()`

返回 `NacosGrpcProtocolNegotiator`（实现 `InternalProtocolNegotiator.ProtocolNegotiator`），支持 TLS 证书的动态刷新（通过 `reloadNegotiator()`）。

如果未启用 TLS，`newProtocolNegotiator()` 返回 `Optional.empty()`。

### 6.5 传输过滤器 — TransportFilter

默认添加 `AddressTransportFilter`，关联 `ConnectionManager` 用于连接追踪。

两个服务器各自通过 SPI 加载额外 filter：

```java
// GrpcSdkServer
NacosGrpcServerTransportFilterServiceLoader.loadServerTransportFilters(SDK_FILTER)

// GrpcClusterServer
NacosGrpcServerTransportFilterServiceLoader.loadServerTransportFilters(CLUSTER_FILTER)
```

拦截器也通过类似 SPI 方式加载：

```java
// GrpcSdkServer
NacosGrpcServerInterceptorServiceLoader.loadServerInterceptors(SDK_INTERCEPTOR)

// GrpcClusterServer
NacosGrpcServerInterceptorServiceLoader.loadServerInterceptors(CLUSTER_INTERCEPTOR)
```

### 6.6 gRPC 参数配置

**文件**: [GrpcServerConstants.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcServerConstants.java#L63-L110)

| 参数 | 默认值 | 覆盖配置项 |
|------|--------|-----------|
| MaxInboundMessageSize | **10MB** | `nacos.remote.server.grpc.sdk.max-inbound-message-size` / `nacos.remote.server.grpc.cluster.max-inbound-message-size` |
| KeepAliveTime | gRPC 默认 | `nacos.remote.server.grpc.sdk.keep-alive-time` / `nacos.remote.server.grpc.cluster.keep-alive-time` |
| KeepAliveTimeout | gRPC 默认 | `nacos.remote.server.grpc.sdk.keep-alive-timeout` / `nacos.remote.server.grpc.cluster.keep-alive-timeout` |
| PermitKeepAliveTime | **5 分钟** | `nacos.remote.server.grpc.sdk.permit-keep-alive-time` / `nacos.remote.server.grpc.cluster.permit-keep-alive-time` |

SDK 和 Cluster 各自使用独立的配置前缀，可独立调优。

---

## 七、优雅关闭

**文件**: [BaseRpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/BaseRpcServer.java#L116-L124)

```java
@PreDestroy
public abstract void shutdownServer();
```

`BaseGrpcServer` 的实现：

```java
@Override
public void shutdownServer() {
    if (server != null) {
        server.shutdownNow();   // 立即关闭, 拒绝新请求并中断正在处理的请求
    }
}
```

两种关闭触发路径：
1. **Spring `@PreDestroy`**：容器关闭时自动调用
2. **JVM ShutdownHook**：`start()` 中注册的 hook 调用 `stopServer()` → `shutdownServer()`

---

## 八、完整启动时序图

```
NacosBootstrap.main()
  │
  └─ startWithoutConsole(args)
       │
       └─ startCoreContext(args)
            │
            └─ SpringApplicationBuilder(NacosServerBasicApplication.class).run(args)
                 │
                 ├─ 扫描 @Service 注解
                 │    ├─ 实例化 GrpcSdkServer
                 │    └─ 实例化 GrpcClusterServer
                 │
                 └─ @PostConstruct 触发
                      │
                      └─ BaseRpcServer.start()
                           │
                           ├─ ① BaseGrpcServer.startServer()
                           │    ├─ addServices(handlerRegistry, interceptors)
                           │    │    ├─ 注册 Unary:  Request/request
                           │    │    │     → GrpcRequestAcceptor.request()
                           │    │    └─ 注册 Bidi:   BiRequestStream/requestBiStream
                           │    │       → GrpcBiStreamRequestAcceptor.requestBiStream()
                           │    │
                           │    ├─ 确定监听地址 (IP:port)
                           │    │    ├─ GrpcSdkServer:    port = 8848 + 1000 = 9848
                           │    │    └─ GrpcClusterServer: port = 8848 + 1001 = 9849
                           │    │
                           │    ├─ NettyServerBuilder.forAddress(addr).executor(...)
                           │    │
                           │    ├─ newProtocolNegotiator() (TLS 可选)
                           │    │    ├─ SdkProtocolNegotiatorBuilderSingleton
                           │    │    └─ ClusterProtocolNegotiatorBuilderSingleton
                           │    │
                           │    ├─ getServerTransportFilters()
                           │    │    └─ AddressTransportFilter + SPI filters
                           │    │
                           │    ├─ .maxInboundMessageSize(10MB)
                           │    ├─ .keepAliveTime(...)
                           │    ├─ .keepAliveTimeout(...)
                           │    └─ .permitKeepAliveTime(5min)
                           │
                           ├─ ② server.start()
                           │    └─ gRPC 服务开始监听指定端口
                           │
                           ├─ ③ 注册 SSL 上下文刷新器
                           │    ├─ RpcServerSslContextRefresherHolder.getSdkInstance().refresh(this)
                           │    └─ RpcServerSslContextRefresherHolder.getClusterInstance().refresh(this)
                           │
                           └─ ④ Runtime.addShutdownHook( → server.shutdownNow() )
```

---

## 九、关键设计决策

### 9.1 为什么分两个 gRPC 服务器？

1. **隔离性**：SDK 客户端流量和集群内部流量使用独立端口，互不影响
2. **安全控制**：集群端口可配置防火墙仅允许集群节点访问，SDK 端口对外暴露
3. **独立调优**：两类流量的线程池、keepAlive、消息大小限制可独立配置
4. **来源校验**：`getSource()` 返回 `"sdk"` 或 `"cluster"`，`RequestHandlerRegistry` 通过 `checkSourceInvokeAllowed()` 确保集群内部的 Handler 不会被 SDK 客户端调用

### 9.2 为什么用 @PostConstruct 而不是显式调用？

Spring 容器管理的 Bean 通过 `@PostConstruct` 自动启动，简化了生命周期管理。Core 上下文初始化完成后 gRPC 服务自动就绪，且 Spring 会在容器关闭时自动触发 `@PreDestroy`。

### 9.3 为什么 Payload 是一元类型？

gRPC 的 `Payload` 是一个通用封装，实际业务类型通过 `Payload.metadata.type` 字段标识（使用类的简单类名）。这种方式避免了为每种业务请求单独定义 proto service，减少 proto 文件数量，同时通过 `RequestHandlerRegistry` 实现灵活的路由分发。

---

## 十、相关源码文件索引

| 文件 | 作用 |
|------|------|
| [NacosBootstrap.java](file:///Users/mmhm/IdeaProjects/nacos/bootstrap/src/main/java/com/alibaba/nacos/bootstrap/NacosBootstrap.java) | 启动入口, 决定部署模式 |
| [BaseRpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/BaseRpcServer.java) | @PostConstruct 启动 + ShutdownHook |
| [BaseGrpcServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/BaseGrpcServer.java) | gRPC NettyServerBuilder 构建与启动 |
| [GrpcSdkServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcSdkServer.java) | SDK gRPC 服务器, 端口 +1000 |
| [GrpcClusterServer.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcClusterServer.java) | 集群 gRPC 服务器, 端口 +1001 |
| [GrpcServerConstants.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/remote/grpc/GrpcServerConstants.java) | gRPC 常量与默认配置 |
| [Constants.java](file:///Users/mmhm/IdeaProjects/nacos/api/src/main/java/com/alibaba/nacos/api/common/Constants.java) | 端口偏移常量定义 |
| [PayloadRegistry.java](file:///Users/mmhm/IdeaProjects/nacos/common/src/main/java/com/alibaba/nacos/common/remote/PayloadRegistry.java) | Payload 类型注册（static init） |
