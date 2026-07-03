/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.remote.grpc;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.api.remote.response.ErrorResponse;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.grpc.GrpcUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.monitor.MetricsMonitor;
import com.alibaba.nacos.core.remote.BaseRpcServer;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.remote.RequestHandlerRegistry;
import com.alibaba.nacos.core.remote.grpc.negotiator.NacosGrpcProtocolNegotiator;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.InetUtils;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerTransportFilter;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Nacos gRPC 服务器基类 —— 基于 NettyServerBuilder 构建 gRPC 服务的抽象骨架。
 *
 * <h2>定位</h2>
 * <p>类似于一个"gRPC 服务的装配流水线"：定义服务注册、端口绑定、拦截器链、
 * 连接管理的标准流程，子类只需提供差异化参数（端口偏移、线程池、来源标签）。</p>
 *
 * <h2>核心职责</h2>
 * <ul>
 *   <li><b>服务注册</b>：{@link #addServices} 注册两个 gRPC 方法——
 *       Unary 的 Request/request 和 Bidi-streaming 的 BiRequestStream/requestBiStream</li>
 *   <li><b>请求路由</b>：{@link #handleCommonRequest} 做来源校验后委托给
 *       {@link GrpcRequestAcceptor}，由其通过 {@link RequestHandlerRegistry} 分发到具体 Handler</li>
 *   <li><b>连接管理</b>：通过 {@link AddressTransportFilter} 和 {@link GrpcConnectionInterceptor}
 *       在连接建立/断开时回调 {@link ConnectionManager}</li>
 * </ul>
 *
 * <h2>注册的两个 gRPC 方法</h2>
 * <pre>{@code
 *   ┌──────────────────────────────────────────────────────┐
 *   │  MutableHandlerRegistry                             │
 *   │                                                     │
 *   │  [1] Request/request  (UNARY)                       │
 *   │      └─ handleCommonRequest()                       │
 *   │           ├─ invokeSourceAllowCheck()  ← 来源校验    │
 *   │           └─ GrpcRequestAcceptor.request()           │
 *   │                └─ RequestHandlerRegistry             │
 *   │                     .getByRequestType(type)          │
 *   │                     └─ handler.handleRequest()       │
 *   │                                                     │
 *   │  [2] BiRequestStream/requestBiStream (BIDI_STREAMING)│
 *   │      └─ GrpcBiStreamRequestAcceptor                  │
 *   │           .requestBiStream()                         │
 *   │           └─ 处理 ConnectionSetupRequest（客户端注册）│
 *   │              和 Response（ACK 回调）                  │
 *   └──────────────────────────────────────────────────────┘
 * }</pre>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建</b>：Spring 扫描 {@code @Service} 注解自动实例化
 *       （子类 {@link GrpcSdkServer} 和 {@link GrpcClusterServer}）</li>
 *   <li><b>启动</b>：父类 {@link BaseRpcServer#start()} 通过 {@code @PostConstruct} 调用
 *       → {@link #startServer()}  → {@link #addServices} 注册方法
 *       → {@link NettyServerBuilder#build()} → {@link Server#start()}</li>
 *   <li><b>关闭</b>：{@link #shutdownServer()} → {@link Server#shutdownNow()}
 *       （由 {@code @PreDestroy} 和 JVM ShutdownHook 双重触发）</li>
 * </ul>
 *
 * <h2>子类差异点（模板方法模式）</h2>
 * <table>
 *   <tr><th>方法</th><th>GrpcSdkServer</th><th>GrpcClusterServer</th></tr>
 *   <tr><td>rpcPortOffset()</td><td>+1000 → 端口 9848</td><td>+1001 → 端口 9849</td></tr>
 *   <tr><td>getSource()</td><td>"sdk"</td><td>"cluster"</td></tr>
 *   <tr><td>getRpcExecutor()</td><td>sdkRpcExecutor</td><td>clusterRpcExecutor</td></tr>
 * </table>
 *
 * @author liuzunfei
 * @version $Id: BaseGrpcServer.java, v 0.1 2020年07月13日 3:42 PM liuzunfei Exp $
 */
public abstract class BaseGrpcServer extends BaseRpcServer {
    
    /**
     * The ProtocolNegotiator instance used for communication.
     */
    protected NacosGrpcProtocolNegotiator protocolNegotiator;
    
    private Server server;
    
    @Autowired
    private GrpcRequestAcceptor grpcCommonRequestAcceptor;
    
    @Autowired
    private GrpcBiStreamRequestAcceptor grpcBiStreamRequestAcceptor;
    
    @Autowired
    private ConnectionManager connectionManager;
    
    @Autowired
    private RequestHandlerRegistry requestHandlerRegistry;
    
    @Override
    public ConnectionType getConnectionType() {
        return ConnectionType.GRPC;
    }
    
    @Override
    public void startServer() throws Exception {
        final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();
        addServices(handlerRegistry, getSeverInterceptors().toArray(new ServerInterceptor[0]));
        String grpcListenIp = InetUtils.getGrpcListenIp();
        InetSocketAddress inetSocketAddress = StringUtils.isNotBlank(grpcListenIp)
            ? new InetSocketAddress(grpcListenIp, getServicePort())
            : new InetSocketAddress(getServicePort());
        NettyServerBuilder builder =
            NettyServerBuilder.forAddress(inetSocketAddress).executor(getRpcExecutor());
        Optional<InternalProtocolNegotiator.ProtocolNegotiator> negotiator =
            newProtocolNegotiator();
        if (negotiator.isPresent()) {
            InternalProtocolNegotiator.ProtocolNegotiator actual = negotiator.get();
            Loggers.REMOTE.info("Add protocol negotiator {}", actual.getClass().getCanonicalName());
            builder.protocolNegotiator(actual);
        }
        
        for (ServerTransportFilter each : getServerTransportFilters()) {
            builder.addTransportFilter(each);
        }
        server = builder.maxInboundMessageSize(getMaxInboundMessageSize())
            .fallbackHandlerRegistry(handlerRegistry)
            .compressorRegistry(CompressorRegistry.getDefaultInstance())
            .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
            .keepAliveTime(getKeepAliveTime(), TimeUnit.MILLISECONDS)
            .keepAliveTimeout(getKeepAliveTimeout(), TimeUnit.MILLISECONDS)
            .permitKeepAliveTime(getPermitKeepAliveTime(), TimeUnit.MILLISECONDS).build();
        
        server.start();
    }
    
    @Override
    public void reloadProtocolContext() {
        reloadProtocolNegotiator();
    }
    
    /**
     * Build new one protocol negotiator.
     *
     * <p>Such as support tls, proxy protocol and so on</p>
     *
     * @return ProtocolNegotiator
     */
    protected Optional<InternalProtocolNegotiator.ProtocolNegotiator> newProtocolNegotiator() {
        return Optional.empty();
    }
    
    /**
     * reload protocol negotiator If necessary.
     */
    public void reloadProtocolNegotiator() {
        if (protocolNegotiator != null) {
            try {
                protocolNegotiator.reloadNegotiator();
            } catch (Throwable throwable) {
                Loggers.REMOTE.info("Nacos {} Rpc server reload negotiator fail at port {}.",
                    this.getClass().getSimpleName(), getServicePort());
                throw throwable;
            }
        }
    }
    
    protected long getPermitKeepAliveTime() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_PERMIT_KEEP_ALIVE_TIME;
    }
    
    protected long getKeepAliveTime() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_KEEP_ALIVE_TIME;
    }
    
    protected long getKeepAliveTimeout() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_KEEP_ALIVE_TIMEOUT;
    }
    
    protected int getMaxInboundMessageSize() {
        Integer property =
            EnvUtil.getProperty(GrpcServerConstants.GrpcConfig.MAX_INBOUND_MSG_SIZE_PROPERTY,
                Integer.class);
        if (property != null) {
            return property;
        }
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE;
    }
    
    protected List<ServerInterceptor> getSeverInterceptors() {
        List<ServerInterceptor> result = new LinkedList<>();
        result.add(new GrpcConnectionInterceptor());
        return result;
    }
    
    protected List<ServerTransportFilter> getServerTransportFilters() {
        return Collections.singletonList(new AddressTransportFilter(connectionManager));
    }
    
    /**
     * get source for the request.
     *
     * @return
     */
    protected abstract String getSource();
    
    private boolean invokeSourceAllowCheck(Payload grpcRequest) {
        return requestHandlerRegistry.checkSourceInvokeAllowed(grpcRequest.getMetadata().getType(),
            getSource());
    }
    
    /**
     * 一元调用（Unary）请求的统一入口 —— 做来源校验后委托给 {@link GrpcRequestAcceptor}。
     *
     * <p>这是 gRPC 一元调用 Request/request 的第一层处理器。
     * 职责单一：校验请求来源（SDK 或 Cluster）是否有权限调用该类型的 Request，
     * 通过后全权委托给 GrpcRequestAcceptor 完成后续的解析、路由、执行。</p>
     *
     * <h3>来源校验机制</h3>
     * <p>{@link #invokeSourceAllowCheck} 调用
     * {@link RequestHandlerRegistry#checkSourceInvokeAllowed(String, String)}，
     * 检查该 Request 类型是否允许从当前来源（{@link #getSource()}）调用。
     * 例如：{@code ConfigChangeClusterSyncRequest} 只允许 {@code "cluster"} 来源调用，
     * SDK 客户端通过 GrpcSdkServer（source = "sdk"）发送此请求会被拒绝。</p>
     *
     * <h3>调用方</h3>
     * <ul>
     *   <li>{@link #addServices} 第 229 行 —— 作为 Unary 方法的 Handler，
     *       每个一元请求到达时由 gRPC 框架回调</li>
     * </ul>
     *
     * @param grpcRequest     客户端发来的 Protobuf Payload，
     *                        metadata.type 标识具体业务类型（如 "InstanceRequest"）
     * @param responseObserver gRPC 响应流，用于回写 Payload 响应或错误
     */
    protected void handleCommonRequest(Payload grpcRequest,
        StreamObserver<Payload> responseObserver) {
        // Step 1: 来源校验 —— 检查该请求类型是否允许从当前 gRPC 服务（SDK/Cluster）调用
        if (!invokeSourceAllowCheck(grpcRequest)) {
            // 拒绝：构造 BAD_GATEWAY 错误响应，记录监控指标后关闭流
            Payload payloadResponse =
                GrpcUtils.convert(ErrorResponse.build(NacosException.BAD_GATEWAY,
                    String.format(" invoke %s from %s is forbidden",
                        grpcRequest.getMetadata().getType(),
                        this.getSource())));
            responseObserver.onNext(payloadResponse);
            
            responseObserver.onCompleted();
            MetricsMonitor.recordGrpcRequestEvent(grpcRequest.getMetadata().getType(), false,
                NacosException.BAD_GATEWAY, null, null, 0);
        } else {
            // Step 2: 校验通过，全权委托给 GrpcRequestAcceptor
            //   后续链路：GrpcRequestAcceptor.request()
            //     → Payload 反序列化（GrpcUtils.parse）
            //     → RequestHandlerRegistry.getByRequestType(type) 查找 Handler
            //     → RequestHandler.handleRequest(request, requestMeta) 执行业务
            grpcCommonRequestAcceptor.request(grpcRequest, responseObserver);
        }
    }
    
    /**
     * 向 gRPC Handler 注册表注册两个核心服务方法 —— 这是 gRPC 请求的入口注册点。
     *
     * <p>每个 gRPC 服务方法由三要素组成：
     * {@link MethodDescriptor}（方法元数据） + {@link ServerCallHandler}（业务处理器）
     * + {@link ServerInterceptor}（拦截器链）。这三者组装成
     * {@link ServerServiceDefinition} 后注册到 {@link MutableHandlerRegistry}，
     * 再由 {@link NettyServerBuilder#fallbackHandlerRegistry} 绑定到端口。</p>
     *
     * <h3>注册的两个方法</h3>
     *
     * <h4>方法一：Request/request（一元调用 UNARY）</h4>
     * <p>客户端的所有普通业务请求（注册实例、发布配置、查询服务等）都走这个入口。
     * 请求被封装为统一的 {@link Payload}（Protobuf 消息），
     * 经 {@link #handleCommonRequest} 做来源校验后，
     * 委托给 {@link GrpcRequestAcceptor#request}。
     * GrpcRequestAcceptor 内部通过 {@link RequestHandlerRegistry#getByRequestType}
     * 根据 {@code Payload.metadata.type} 字段（类的简单类名）查找对应的
     * {@link com.alibaba.nacos.core.remote.RequestHandler}，最终执行
     * {@code handler.handleRequest(request, requestMeta)}。</p>
     *
     * <h4>方法二：BiRequestStream/requestBiStream（双向流 BIDI_STREAMING）</h4>
     * <p>用于客户端与服务端之间的长连接双向通信，主要承载两种消息：
     * <ul>
     *   <li><b>ConnectionSetupRequest</b> —— 客户端首次建立连接时发送，
     *       携带客户端版本、标签（labels）、能力协商表（ability table）。
     *       服务端创建 {@link com.alibaba.nacos.core.remote.Connection}
     *       并注册到 {@link ConnectionManager}</li>
     *   <li><b>Response</b> —— 客户端对服务端推送请求的 ACK 响应，
     *       通过 RpcAckCallbackSynchronizer.ackNotify()
     *       通知等待中的回调</li>
     * </ul>
     * </p>
     *
     * <h3>调用方</h3>
     * <ul>
     *   <li>{@link #startServer()} 第 93 行 —— 在 gRPC 服务器构建阶段调用，
     *       将注册好的 handlerRegistry 传给 NettyServerBuilder</li>
     * </ul>
     *
     * @param handlerRegistry  gRPC 的可变 Handler 注册表，服务定义最终注册到这里
     * @param serverInterceptor 拦截器数组，包裹在每个服务定义外面，
     *                          每次请求先经过拦截器链再进入 Handler
     */
    private void addServices(MutableHandlerRegistry handlerRegistry,
        ServerInterceptor... serverInterceptor) {
        
        // ==================== 方法一：一元调用 Request/request ====================
        
        // Step 1: 构建 MethodDescriptor —— 描述方法的元数据
        //   - setType(UNARY): 一元调用，一问一答模式
        //   - setFullMethodName: 生成全限定方法名 "Request/request"
        //   - setRequestMarshaller/setResponseMarshaller: 使用 Protobuf 的 Payload 序列化器
        final MethodDescriptor<Payload, Payload> unaryPayloadMethod = MethodDescriptor
            .<Payload, Payload>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY).setFullMethodName(
                MethodDescriptor.generateFullMethodName(GrpcServerConstants.REQUEST_SERVICE_NAME,
                    GrpcServerConstants.REQUEST_METHOD_NAME))
            .setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
        
        // Step 2: 构建 ServerCallHandler —— 异步一元调用的处理器
        //   ServerCalls.asyncUnaryCall 将同步风格的 (request, observer) 回调适配为 gRPC 异步处理
        //   每个请求到达时，gRPC 框架取出 Payload，调用 handleCommonRequest 做来源校验后分发
        final ServerCallHandler<Payload, Payload> payloadHandler = ServerCalls.asyncUnaryCall(
            (request, responseObserver) -> {
                handleCommonRequest(request, responseObserver);
            });
        
        // Step 3: 组装 ServerServiceDefinition —— 服务定义 = 服务名 + 方法列表
        final ServerServiceDefinition serviceDefOfUnaryPayload = ServerServiceDefinition.builder(
            GrpcServerConstants.REQUEST_SERVICE_NAME).addMethod(unaryPayloadMethod, payloadHandler)
            .build();
        
        // Step 4: 包裹拦截器链后注册到 HandlerRegistry
        //   ServerInterceptors.intercept 将拦截器织入服务定义：
        //   请求到达 → 拦截器链（GrpcConnectionInterceptor + SPI 拦截器）→ payloadHandler
        handlerRegistry
            .addService(ServerInterceptors.intercept(serviceDefOfUnaryPayload, serverInterceptor));
        
        // ==================== 方法二：双向流 BiRequestStream/requestBiStream ====================
        
        // Step 5: 构建双向流的 ServerCallHandler
        //   ServerCalls.asyncBidiStreamingCall 创建一个适配器：
        //   客户端发起流式连接时，gRPC 框架回调 lambda，
        //   lambda 调用 GrpcBiStreamRequestAcceptor.requestBiStream(responseObserver)
        //   返回一个 StreamObserver<Payload> 作为"客户端→服务端"方向的监听器
        final ServerCallHandler<Payload, Payload> biStreamHandler =
            ServerCalls.asyncBidiStreamingCall(
                (responseObserver) -> grpcBiStreamRequestAcceptor
                    .requestBiStream(responseObserver));
        
        // Step 6: 构建双向流 MethodDescriptor
        //   注意 setRequestMarshaller 使用 Payload.newBuilder().build()（而非 getDefaultInstance()）
        //   因为双向流的首个请求需要可变实例来反序列化 ConnectionSetupRequest
        final MethodDescriptor<Payload, Payload> biStreamMethod =
            MethodDescriptor.<Payload, Payload>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING).setFullMethodName(
                    MethodDescriptor.generateFullMethodName(
                        GrpcServerConstants.REQUEST_BI_STREAM_SERVICE_NAME,
                        GrpcServerConstants.REQUEST_BI_STREAM_METHOD_NAME))
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.newBuilder().build()))
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();
        
        // Step 7: 组装双向流的 ServiceDefinition 并注册
        final ServerServiceDefinition serviceDefOfBiStream = ServerServiceDefinition.builder(
            GrpcServerConstants.REQUEST_BI_STREAM_SERVICE_NAME)
            .addMethod(biStreamMethod, biStreamHandler).build();
        handlerRegistry
            .addService(ServerInterceptors.intercept(serviceDefOfBiStream, serverInterceptor));
        
    }
    
    @Override
    public void shutdownServer() {
        if (server != null) {
            server.shutdownNow();
        }
    }
    
    /**
     * get rpc executor.
     *
     * @return executor.
     */
    public abstract ThreadPoolExecutor getRpcExecutor();
    
}
