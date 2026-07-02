/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.common.remote;

import com.alibaba.nacos.api.remote.Payload;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * gRPC 通信的全局请求/响应类型注册表——{@code Payload} 协议层的关键基础设施。
 *
 * <h2>一句话定位</h2>
 * <p>在 gRPC 通道上传输 Java 对象之前，必须知道"这个字节流对应哪个具体类"。
 * PayloadRegistry 通过 Java SPI 全量扫描所有 {@link com.alibaba.nacos.api.remote.Payload}
 * 实现类，按简单类名建立类型注册表，供序列化（{@code convert}）和反序列化
 * （{@code parse}）双向使用。</p>
 *
 * <h2>核心职责</h2>
 * <ul>
 *   <li><b>类型发现（discovery）</b>：通过 Java SPI {@link java.util.ServiceLoader} 扫描
 *       classpath 下所有 {@code META-INF/services/com.alibaba.nacos.api.remote.Payload}
 *       文件，发现全部请求/响应类</li>
 *   <li><b>类型注册（registration）</b>：将每个 Payload 子类的简单类名（如
 *       {@code "InstanceRequest"}）映射到对应的 {@link Class} 对象，存入全局 HashMap</li>
 *   <li><b>类型查询（lookup）</b>：在 gRPC 反序列化时，通过 Payload Metadata 中携带的
 *       {@code type} 字段（简单类名）查表获取目标 Class，供 Jackson 反序列化 JSON body</li>
 * </ul>
 *
 * <h2>数据流转全景</h2>
 * <pre>{@code
 *   ┌────────────────── 初始化（JVM 启动时）──────────────────┐
 *   │                                                          │
 *   │  BaseRpcServer static 块 或 RpcClient static 块          │
 *   │    └─ PayloadRegistry.init()                              │
 *   │         └─ scan()                                        │
 *   │              └─ ServiceLoader.load(Payload.class)         │
 *   │                   └─ 读取所有 META-INF/services 下的      │
 *   │                      Payload 实现类列表                    │
 *   │                        └─ register(name, class)           │
 *   │                             └─ REGISTRY_REQUEST.put()      │
 *   │                                                          │
 *   └──────────────────────────────────────────────────────────┘
 *
 *   ┌─────────── 序列化（客户端发请求）────────────┐
 *   │                                                │
 *   │  InstanceRequest instance = new InstanceRequest() │
 *   │    └─ GrpcUtils.convert(request)                │
 *   │         └─ Metadata.setType("InstanceRequest")  │  ← 类名写入 gRPC header
 *   │         └─ body = JSON.toBytes(request)          │  ← 对象序列化为 JSON
 *   │         └─ 返回 Payload(metadata + body)         │
 *   │              └─ 通过 gRPC 字节流发送到服务端      │
 *   │                                                │
 *   └────────────────────────────────────────────────┘
 *
 *   ┌─────────── 反序列化（服务端收请求）──────────────┐
 *   │                                                   │
 *   │  GrpcRequestAcceptor.request(grpcRequest)          │
 *   │    └─ GrpcUtils.parse(payload)                     │
 *   │         └─ type = payload.getMetadata().getType()  │
 *   │         └─ Class<?> clz = PayloadRegistry         │
 *   │              .getClassByType("InstanceRequest")    │  ← 查表获取类型
 *   │         └─ JsonUtils.toObj(bodyBytes, clz)         │  ← Jackson 反序列化
 *   │         └─ return InstanceRequest 对象              │
 *   │                                                   │
 *   └───────────────────────────────────────────────────┘
 * }</pre>
 *
 * <h2>SPI 注册文件分布</h2>
 * <p>Payload 实现类分散在不同模块的 SPI 文件中，ServiceLoader 会合并所有 jar 中的配置：</p>
 * <ul>
 *   <li><b>api/src/main/resources/META-INF/services/...</b> — 核心 Request/Response
 *       （95 行，含 InstanceRequest、ConfigPublishRequest 等）</li>
 *   <li><b>core/src/main/resources/META-INF/services/...</b> — 集群管理类型
 *       （MemberReportRequest、MemberReportResponse）</li>
 *   <li><b>naming/src/main/resources/META-INF/services/...</b> — 一致性同步类型
 *       （DistroDataRequest、DistroDataResponse）</li>
 * </ul>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>{@link com.alibaba.nacos.common.remote.client.grpc.GrpcUtils}</b> — convert()
 *       在序列化时将 simpleName 写入 Metadata.type，parse() 在反序列化时通过
 *       {@code getClassByType()} 查表并调用 Jackson 反序列化</li>
 *   <li><b>BaseRpcServer（core 模块）</b> — 在 static 块中
 *       调用 {@code init()}，确保服务端启动前类型已就绪</li>
 *   <li><b>{@link com.alibaba.nacos.common.remote.client.RpcClient}</b> — 同样在 static
 *       块中调用 {@code init()}，确保客户端建立连接前类型已就绪</li>
 *   <li><b>{@link java.util.ServiceLoader}</b> — Java 标准 SPI 机制，负责发现所有
 *       Payload 实现类</li>
 * </ul>
 *
 * <h2>关键设计决策</h2>
 * <ul>
 *   <li><b>为什么用简单类名而非全限定名做 key？</b>——简单类名（如 {@code "InstanceRequest"}）
 *       比全限定名短得多，减少 gRPC Metadata 的字节开销。同时配合
 *       RequestHandlerRegistry（core 模块）也用简单类名做 key，
 *       Handler 查找可以直接复用相同的 type 值</li>
 *   <li><b>为什么拒绝抽象类注册？</b>——{@code Request} 和 {@code Response} 都是抽象类，
 *       它们出现在 SPI 实现列表中时会被 {@code Modifier.isAbstract()} 过滤掉，
 *       防止反序列化到无意义的抽象类型</li>
 *   <li><b>为什么用 DCL（双重检查锁）？</b>——{@code scan()} 方法在 ServiceLoader 遍历
 *       阶段持有 synchronized，但之后的 {@code register()} 每次 put 不需要加锁（因为
 *       只有 {@code scan()} 在调用它）。幂等保护：{@code initialized} 标志确保多次
 *       调用 {@code init()} 不会重复扫描</li>
 *   <li><b>为什么不用 Spring 扫描？</b>——common 模块不依赖 Spring，且客户端（无 Spring）
 *       和测试代码也需要类型注册。SPI 是纯 Java 标准机制，零框架依赖</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>初始化时机</b>：
 *     <ul>
 *       <li>服务端 — BaseRpcServer（core 模块）static 块
 *           （JVM 加载类时自动执行）</li>
 *       <li>客户端 — {@link com.alibaba.nacos.common.remote.client.RpcClient} static 块
 *           （SDK 首次引用时自动执行）</li>
 *     </ul>
 *   </li>
 *   <li><b>生命周期</b>：JVM 级别，注册后永久有效，无需清理</li>
 * </ul>
 *
 * @author liuzunfei
 * @author hujun
 * @see com.alibaba.nacos.common.remote.client.grpc.GrpcUtils
 */

public class PayloadRegistry {
    
    /**
     * 全局类型注册表。Key = 类简单名（如 {@code "InstanceRequest"}），
     * Value = 对应的 Class 对象。线程安全由 scan() 的 synchronized 保证写入有序。
     */
    private static final Map<String, Class<?>> REGISTRY_REQUEST = new HashMap<>();
    
    /**
     * 是否已完成 SPI 扫描。DCL（双重检查锁）的标志位，
     * 确保 {@code init()} 被多次调用时不会重复扫描。
     */
    static boolean initialized = false;
    
    /**
     * 执行 SPI 扫描并构建类型注册表。
     *
     * <p>调用方（来自源码证据）：</p>
     * <ul>
     *   <li>BaseRpcServer static 块第 36 行 — 服务端启动前初始化</li>
     *   <li>RpcClient static 块第 108 行 — 客户端建立连接前初始化</li>
     *   <li>GrpcUtilsTest.setup() 第 47 行 — 测试用例手动初始化</li>
     * </ul>
     */
    public static void init() {
        scan();
    }
    
    /**
     * SPI 扫描入口，幂等安全（DCL 模式）。
     *
     * <p>通过 Java SPI 加载所有 classpath 下实现了 {@link com.alibaba.nacos.api.remote.Payload}
     * 接口的类，将每个具体类的简单类名注册到 {@link #REGISTRY_REQUEST} 中。</p>
     *
     * <p>synchronized 保护的是 ServiceLoader 遍历过程（ServiceLoader 本身非线程安全），
     * 但 register() 在 synchronized 块内执行，写入操作是线程安全的。</p>
     */
    private static synchronized void scan() {
        if (initialized) {
            return;
        }
        // Step 1: 通过 SPI 加载所有 Payload 实现类
        ServiceLoader<Payload> payloads = ServiceLoader.load(Payload.class);
        // Step 2: 遍历每个实现类，按简单类名注册
        for (Payload payload : payloads) {
            register(payload.getClass().getSimpleName(), payload.getClass());
        }
        // Step 3: 标记完成，后续 init() 调用将直接返回
        initialized = true;
    }
    
    /**
     * 将一个 Payload 子类注册到全局类型表中。
     *
     * <p>只注册具体类，跳过抽象类（如 Request、Response 本身）。
     * 如果同一个类名被注册两次，直接抛 RuntimeException——这是不可恢复的 SPI 配置错误，
     * 在启动阶段暴露问题比运行时反序列化出错更好。</p>
     *
     * @param type  简单类名，作为查询 key（如 {@code "InstanceRequest"}），
     *              与 gRPC Metadata.type 字段一致
     * @param clazz 对应的 Class 对象，后续用于 Jackson 反序列化
     * @throws RuntimeException 当同名类已注册时抛出（重复的 SPI 声明）
     */
    static void register(String type, Class<?> clazz) {
        // 过滤抽象类：Request 和 Response 本身不应注册
        if (Modifier.isAbstract(clazz.getModifiers())) {
            return;
        }
        // 重复注册检测：防止同名类被多个模块声明
        if (REGISTRY_REQUEST.containsKey(type)) {
            throw new RuntimeException(
                String.format("Fail to register, type: %s, clazz: %s", type, clazz.getName()));
        }
        REGISTRY_REQUEST.put(type, clazz);
    }
    
    /**
     * 根据类型名反查 Class 对象。
     *
     * <p>调用方（来自源码证据）：</p>
     * <ul>
     *   <li>GrpcUtils.parse() 第 123 行 — gRPC 反序列化时查表获取目标 Class：
     *       {@code Class classType = PayloadRegistry.getClassByType(payload.getMetadata().getType())}
     *   </li>
     * </ul>
     *
     * @param type 简单类名（来自 gRPC Payload Metadata 的 type 字段），如 {@code "InstanceRequest"}
     * @return 对应的 Class 对象，如果未找到返回 null
     */
    public static Class<?> getClassByType(String type) {
        return REGISTRY_REQUEST.get(type);
    }
}
