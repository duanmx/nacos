/*
 * Copyright 1999-2025 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.demo;

import com.alibaba.nacos.api.grpc.auto.Metadata;
import com.alibaba.nacos.api.grpc.auto.Payload;
import com.google.protobuf.Any;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.ForwardingServerCallListener;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Echo 服务器 — 接收客户端 Payload 请求并返回 Echo 响应.
 *
 * <p>本类演示了 gRPC 服务器端的核心组件：
 * <ul>
 *   <li>{@link Server} — gRPC 服务器实例，管理生命周期和请求分发</li>
 *   <li>{@link NettyServerBuilder} — 基于 Netty 的服务器构建器（与 Nacos 生产环境一致）</li>
 *   <li>{@link ServerServiceDefinition} — 手动注册服务定义（非 protoc 自动生成）</li>
 *   <li>{@link ServerInterceptor} — 服务端拦截器，在请求到达业务逻辑前插入横切逻辑</li>
 *   <li>{@link MethodDescriptor} — 方法描述符，定义 RPC 方法的请求/响应类型和编组方式</li>
 * </ul>
 *
 * <h3>与 Nacos 生产环境的对应关系：</h3>
 * <p>Nacos 使用 {@code GrpcServerBuilder.newBuilder()} 构建 gRPC 服务器，
 * 底层同样基于 {@code NettyServerBuilder}，监听 9848/9849 端口。
 * 本 Demo 简化为直接监听 50051 端口。
 *
 * <h3>运行方式：</h3>
 * <p>先运行本类的 {@link #main(String[])} 启动服务器，
 * 再运行 {@link GrpcEchoClient#main(String[])} 发送请求。
 *
 * @author demo
 * @see GrpcEchoClient
 */
public class GrpcEchoServer {

    /**
     * 默认监听端口.
     */
    private static final int DEFAULT_PORT = 50051;

    /**
     * gRPC 服务名（完整路径格式：packageName.ServiceName）.
     */
    static final String SERVICE_NAME = "nacos.demo.EchoService";

    /**
     * gRPC 方法名.
     */
    static final String METHOD_NAME = "echo";

    /**
     * 方法描述符 — 定义 RPC 方法的完整契约.
     *
     * <p>{@link MethodDescriptor} 是 gRPC 中最核心的抽象之一，它描述了：
     * <ul>
     *   <li>方法类型：{@code UNARY}（一元调用）— 客户端发一个请求，服务端返一个响应</li>
     *   <li>Marshaller：使用 {@link ProtoUtils#marshaller} 将 Protobuf 生成的
     *       {@link Payload} 类自动适配为 gRPC 的序列化策略</li>
     *   <li>完整方法名：格式为 {@code {serviceName}/{methodName}}</li>
     * </ul>
     */
    static final MethodDescriptor<Payload, Payload> ECHO_METHOD =
            MethodDescriptor.<Payload, Payload>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(SERVICE_NAME + "/" + METHOD_NAME)
                    .setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
                    .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
                    .build();

    private final int port;
    private Server server;

    /**
     * 构造 gRPC Echo 服务器.
     *
     * @param port 监听端口
     */
    public GrpcEchoServer(int port) {
        this.port = port;
    }

    /**
     * 启动 gRPC 服务器.
     *
     * <p>使用 {@link NettyServerBuilder} 构建基于 Netty 的 gRPC 服务器，
     * 这与 Nacos 生产环境使用的传输层一致（Nacos 使用 {@code grpc-netty-shaded}）。
     *
     * <p>注册的服务和拦截器按以下顺序处理请求：
     * <pre>
     *   客户端请求 → ServerInterceptor → EchoService.startCall → onMessage → 响应
     * </pre>
     *
     * @throws IOException 如果端口绑定失败
     */
    public void start() throws IOException {
        server = NettyServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", port))
                // 注册 Echo 服务实现
                .addService(buildEchoService())
                // 服务端拦截器：在请求到达业务逻辑前打印日志
                .intercept(new LoggingServerInterceptor())
                .build()
                .start();
        System.out.println("[Server] gRPC 服务器已启动, 监听端口: " + port);
        System.out.println("[Server] 服务名: " + SERVICE_NAME);
        System.out.println("[Server] 方法名: " + METHOD_NAME);
        System.out.println("[Server] 等待客户端连接...");
        System.out.println();

        // 注册 JVM 关闭钩子，确保服务器优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[Server] JVM 正在关闭, 开始优雅停止 gRPC 服务器...");
            try {
                GrpcEchoServer.this.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    /**
     * 优雅停止服务器，等待已有请求处理完成.
     *
     * @throws InterruptedException 如果等待过程中被中断
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            System.out.println("[Server] gRPC 服务器已关闭");
        }
    }

    /**
     * 阻塞等待服务器终止（用于 main 方法中保持进程运行）.
     *
     * @throws InterruptedException 如果等待过程中被中断
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * 构建 Echo 服务定义.
     *
     * <p>使用 {@link ServerServiceDefinition#builder(String)} 手动注册服务，
     * 而非依赖 protoc-gen-grpc-java 自动生成的 Stub 类。这种方式更底层，
     * 能清晰展示 gRPC 服务注册的本质过程：
     *
     * <pre>
     *   1. ServerServiceDefinition.builder(serviceName)  — 创建服务定义构建器
     *   2. .addMethod(methodDescriptor, callHandler)     — 注册方法和对应的处理器
     *   3. callHandler.startCall() 返回 Listener          — 每个 RPC 调用创建一个 Listener
     *   4. Listener.onMessage(request)                   — 接收到反序列化后的请求对象
     *   5. call.sendMessage(response) + call.close()     — 发送响应并关闭调用
     * </pre>
     *
     * @return 服务定义对象
     */
    private ServerServiceDefinition buildEchoService() {
        return ServerServiceDefinition.builder(SERVICE_NAME)
                .addMethod(ECHO_METHOD, new ServerCallHandler<Payload, Payload>() {
                    @Override
                    public ServerCall.Listener<Payload> startCall(
                            ServerCall<Payload, Payload> call,
                            io.grpc.Metadata headers) {
                        return new ServerCall.Listener<Payload>() {

                            @Override
                            public void onMessage(Payload request) {
                                // ===== 1. 读取请求内容 =====
                                String requestType = request.getMetadata().getType();
                                String clientIp = request.getMetadata().getClientIp();
                                String requestBody = request.getBody().getValue().toStringUtf8();
                                System.out.println("[Server] 收到请求:");
                                System.out.println("  type     = " + requestType);
                                System.out.println("  clientIp = " + clientIp);
                                System.out.println("  body     = " + requestBody);

                                // ===== 2. 构造响应 =====
                                String responseBody = "Echo: " + requestBody;
                                Payload response = Payload.newBuilder()
                                        .setMetadata(Metadata.newBuilder()
                                                .setType("EchoResponse")
                                                .setClientIp("127.0.0.1")
                                                .build())
                                        .setBody(Any.newBuilder()
                                                .setValue(UnsafeByteOperations.unsafeWrap(
                                                        responseBody.getBytes(StandardCharsets.UTF_8)))
                                                .build())
                                        .build();

                                // ===== 3. 发送响应并关闭调用 =====
                                call.sendHeaders(new io.grpc.Metadata());
                                call.sendMessage(response);
                                call.close(io.grpc.Status.OK, new io.grpc.Metadata());

                                System.out.println("[Server] 响应已发送: " + responseBody);
                                System.out.println();
                            }

                            @Override
                            public void onHalfClose() {
                                // 客户端已发送完所有消息（一元调用中正常触发）
                            }

                            @Override
                            public void onCancel() {
                                System.out.println("[Server] 客户端取消了请求");
                            }

                            @Override
                            public void onComplete() {
                                // 调用已完成
                            }
                        };
                    }
                })
                .build();
    }

    /**
     * 服务端日志拦截器.
     *
     * <p>在每个 RPC 调用到达业务逻辑之前，拦截并打印方法名信息。
     * 对应 Nacos 中的 {@code GrpcRequestAcceptor} 接收请求时的 trace 日志。
     */
    private static class LoggingServerInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                io.grpc.Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
            System.out.println("[Interceptor] 收到 RPC 调用: "
                    + call.getMethodDescriptor().getFullMethodName());
            // 委托给下一个处理器（最终到达 Service 实现的 startCall）
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
                    next.startCall(call, headers)) {
            };
        }
    }

    /**
     * 启动 gRPC Echo 服务器.
     *
     * <p>默认监听端口 {@value #DEFAULT_PORT}，可通过命令行参数指定端口。
     *
     * @param args 可选：args[0] 为监听端口号
     * @throws Exception 如果启动失败
     */
    public static void main(String[] args) throws Exception {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        GrpcEchoServer server = new GrpcEchoServer(port);
        server.start();
        server.blockUntilShutdown();
    }
}
