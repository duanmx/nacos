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
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Echo 客户端 — 向服务器发送 Payload 请求并接收响应.
 *
 * <p>本类演示了 gRPC 客户端的核心组件：
 * <ul>
 *   <li>{@link ManagedChannel} — gRPC 客户端通道，管理与服务器的连接（连接池、重连、负载均衡）</li>
 *   <li>{@link NettyChannelBuilder} — 基于 Netty 的通道构建器（与 Nacos 客户端一致）</li>
 *   <li>{@link ClientCalls#blockingUnaryCall} — 同步一元调用工具，内部封装了 StreamObserver 异步回调</li>
 *   <li>{@link ClientInterceptor} — 客户端拦截器，在请求发送前插入横切逻辑</li>
 * </ul>
 *
 * <h3>与 Nacos 客户端的对应关系：</h3>
 * <p>Nacos 客户端使用 {@code GrpcSdkClient} 创建 gRPC 通道，底层同样基于
 * {@code NettyChannelBuilder}，连接到服务端的 9848 端口。本 Demo 连接到 localhost:50051。
 *
 * <h3>运行方式：</h3>
 * <p>先运行 {@link GrpcEchoServer#main(String[])} 启动服务器，
 * 再运行本类的 {@link #main(String[])} 发送请求。
 *
 * @author demo
 * @see GrpcEchoServer
 */
public class GrpcEchoClient {

    /**
     * 默认服务器端口.
     */
    private static final int DEFAULT_PORT = 50051;

    /**
     * 默认服务器地址.
     */
    private static final String DEFAULT_HOST = "localhost";

    private final ManagedChannel channel;

    /**
     * 构造 gRPC Echo 客户端，连接到指定的服务器地址和端口.
     *
     * <p>使用 {@link NettyChannelBuilder} 构建基于 Netty 的通道，
     * 并注册一个 {@link ClientInterceptor} 在每次发送请求时打印日志。
     *
     * @param host 服务器地址
     * @param port 服务器端口
     */
    public GrpcEchoClient(String host, int port) {
        channel = NettyChannelBuilder.forAddress(new InetSocketAddress(host, port))
                // 禁用 TLS，使用明文传输（Demo 不需要加密）
                .usePlaintext()
                // 客户端拦截器：在每次发送请求前打印日志
                .intercept(new LoggingClientInterceptor())
                .build();
        System.out.println("[Client] 已连接到 gRPC 服务器: " + host + ":" + port);
    }

    /**
     * 发送一个 Echo 请求并返回服务器响应.
     *
     * <p>使用 {@link ClientCalls#blockingUnaryCall} 发起同步一元调用。
     * 该方法内部完成了以下流程：
     * <pre>
     *   1. channel.newCall(methodDescriptor, callOptions) — 创建 ClientCall
     *   2. clientCall.start(listener, headers)            — 启动调用
     *   3. clientCall.sendMessage(request)                — 发送序列化的请求（经 Marshaller 编码）
     *   4. clientCall.halfClose()                         — 标记请求发送完毕
     *   5. 阻塞等待 onMessage(response) 回调              — 接收反序列化后的响应
     * </pre>
     *
     * <p>其中 {@link GrpcEchoServer#ECHO_METHOD} 定义了请求/响应的 {@link MethodDescriptor}，
     * 包含 Protobuf Marshaller，负责将 {@link Payload} 对象与字节流互相转换。
     *
     * @param message 要发送的消息内容
     * @return 服务器返回的响应 Payload
     */
    public Payload sendEcho(String message) {
        // 1. 构造请求 Payload（与 Nacos GrpcUtils.convert 类似）
        Payload request = Payload.newBuilder()
                .setMetadata(Metadata.newBuilder()
                        .setType("EchoRequest")
                        .setClientIp("192.168.1.100")
                        .build())
                .setBody(Any.newBuilder()
                        .setValue(UnsafeByteOperations.unsafeWrap(
                                message.getBytes(StandardCharsets.UTF_8)))
                        .build())
                .build();

        System.out.println("[Client] 发送请求: type=" + request.getMetadata().getType()
                + ", body=" + message);

        // 2. 发起同步一元调用
        //    等价于 Nacos 中 GrpcClient.requestServer() 的底层调用
        Payload response = ClientCalls.blockingUnaryCall(
                channel,
                GrpcEchoServer.ECHO_METHOD,
                CallOptions.DEFAULT,
                request);

        // 3. 解析响应（与 Nacos GrpcUtils.parse 类似）
        String responseType = response.getMetadata().getType();
        String responseBody = response.getBody().getValue().toStringUtf8();
        System.out.println("[Client] 收到响应: type=" + responseType + ", body=" + responseBody);

        return response;
    }

    /**
     * 关闭客户端通道.
     *
     * @throws InterruptedException 如果等待关闭过程中被中断
     */
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        System.out.println("[Client] 通道已关闭");
    }

    /**
     * 客户端日志拦截器.
     *
     * <p>在每次 RPC 调用发送请求前拦截并打印方法名。
     * 对应 Nacos 客户端中用于链路追踪和调试的拦截器逻辑。
     */
    private static class LoggingClientInterceptor implements ClientInterceptor {

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions,
                Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    next.newCall(method, callOptions)) {
                @Override
                public void sendMessage(ReqT message) {
                    System.out.println("[Interceptor] 发送 RPC 请求: "
                            + method.getFullMethodName());
                    super.sendMessage(message);
                }
            };
        }
    }

    /**
     * 启动 gRPC Echo 客户端，发送测试请求.
     *
     * <p>默认连接 localhost:{@value #DEFAULT_PORT}，可通过命令行参数指定。
     *
     * <p>用法：{@code GrpcEchoClient [host] [port]}
     *
     * @param args 可选：args[0] 为服务器地址，args[1] 为端口号
     * @throws Exception 如果连接或调用失败
     */
    public static void main(String[] args) throws Exception {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;
        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            port = Integer.parseInt(args[1]);
        }

        System.out.println("========== gRPC Echo Client ==========");
        System.out.println();

        GrpcEchoClient client = new GrpcEchoClient(host, port);
        try {
            // 发送三条测试消息
            client.sendEcho("Hello gRPC!");
            System.out.println();
            client.sendEcho("Nacos uses gRPC 1.78.0 + protobuf 3.25.5");
            System.out.println();
            client.sendEcho("This is a demo message");
        } finally {
            client.shutdown();
        }

        System.out.println();
        System.out.println("========== 测试完成 ==========");
    }
}
