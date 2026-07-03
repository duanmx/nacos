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

package com.alibaba.nacos.core.remote;

import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.core.control.TpsControl;
import com.alibaba.nacos.core.control.TpsControlConfig;
import com.alibaba.nacos.core.remote.grpc.InvokeSource;
import com.alibaba.nacos.plugin.control.ControlManagerCenter;
import com.google.common.collect.Sets;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * gRPC 请求的 Handler 路由注册表 —— gRPC 请求分发到具体业务逻辑的"电话簿"。
 *
 * <h2>一句话定位</h2>
 * <p>当客户端发来一个 {@code InstanceRequest}，gRPC 框架只知道这是一个 {@code Payload}
 * 字节流。这个类负责根据 {@code metadata.type}（如 {@code "InstanceRequest"}）
 * 找到对应的 {@link RequestHandler}，然后由 Handler 执行业务逻辑。</p>
 *
 * <h2>核心职责</h2>
 * <ul>
 *   <li><b>Handler 索引</b>：维护 {@code Map<String, RequestHandler>}，
 *       key = Request 泛型类型的简单类名，value = Handler 实例</li>
 *   <li><b>来源校验</b>：维护 {@code Map<String, Set<String>>}，
 *       记录每个 Request 类型允许从哪些来源（sdk / cluster）调用</li>
 *   <li><b>TPS 控制点注册</b>：扫描 {@code @TpsControl} 注解，
 *       自动注册限流控制点</li>
 * </ul>
 *
 * <h2>启动时机——为什么是 ContextRefreshedEvent？</h2>
 * <p>实现 {@link ApplicationListener} 而非 {@code @PostConstruct}，
 * 因为 {@code RequestHandler} 的子类分布在 config、naming、core、ai
 * 等多个模块中，它们的 Spring Bean 初始化有先后顺序。
 * {@code ContextRefreshedEvent} 保证所有模块的 Bean 都已就绪后才执行扫描。</p>
 *
 * <h2>扫描流程</h2>
 * <pre>{@code
 *   ContextRefreshedEvent 触发
 *     │
 *     └─ onApplicationEvent(event)
 *          │
 *          ├─ Step 1: event.getApplicationContext()
 *          │           .getBeansOfType(RequestHandler.class)
 *          │    → 获取 Spring 容器中所有 RequestHandler 子类 Bean
 *          │      如 InstanceRequestHandler, ConfigPublishRequestHandler, ...
 *          │
 *          ├─ Step 2: 向上遍历类继承链
 *          │   while (!clazz.getSuperclass().equals(RequestHandler.class)) {
 *          │       clazz = clazz.getSuperclass();
 *          │   }
 *          │    → 找到直接 extends RequestHandler<T,S> 的那个类
 *          │    → 跳过间接继承（如测试用的多层 Mock Handler）
 *          │
 *          ├─ Step 3: 反射获取泛型类型 T
 *          │   (Class) ((ParameterizedType) clazz.getGenericSuperclass())
 *          │       .getActualTypeArguments()[0]
 *          │    → 如 InstanceRequestHandler 得到 InstanceRequest.class
 *          │
 *          ├─ Step 4: 注册 TPS 控制点（可选）
 *          │   if (handle() 方法有 @TpsControl && TPS 功能已启用)
 *          │       → ControlManagerCenter.getTpsControlManager()
 *          │           .registerTpsPoint(pointName)
 *          │
 *          ├─ Step 5: 注册来源白名单（可选）
 *          │   if (类上有 @InvokeSource(source = {"cluster"}))
 *          │       → sourceRegistry.put("ServerReloadRequest", {"cluster"})
 *          │
 *          └─ Step 6: 建立 Handler 索引
 *              registryHandlers.putIfAbsent(tClass.getSimpleName(), handler)
 *              → key = "InstanceRequest", value = instanceRequestHandler Bean
 * }</pre>
 *
 * <h2>运行时查询——两个查询方法</h2>
 *
 * <h3>getByRequestType —— 请求路由</h3>
 * <pre>{@code
 *   gRPC 客户端发送 Payload(metadata.type="InstanceRequest", body=...)
 *     → GrpcRequestAcceptor.request() 第 117 行
 *       → requestHandlerRegistry.getByRequestType("InstanceRequest")
 *         → registryHandlers.get("InstanceRequest")
 *           → 返回 InstanceRequestHandler Bean
 *             → handler.handleRequest(instanceRequest, requestMeta)
 *               → RequestHandler.handleRequest() [模板方法]
 *                 → 执行 RequestFilters 过滤器链
 *                   → 调用子类 handle() 执行业务逻辑
 * }</pre>
 *
 * <h3>checkSourceInvokeAllowed —— 来源校验</h3>
 * <pre>{@code
 *   gRPC 客户端通过 GrpcSdkServer（source = "sdk"）发送
 *   ConfigChangeClusterSyncRequest
 *     → BaseGrpcServer.handleCommonRequest() 第 278 行
 *       → invokeSourceAllowCheck() 第 192 行
 *         → checkSourceInvokeAllowed("ConfigChangeClusterSyncRequest", "sdk")
 *           → sourceRegistry 中该类型只允许 "cluster"
 *           → 返回 false → 拒绝请求，返回 BAD_GATEWAY
 * }</pre>
 *
 * <h2>两个核心数据结构</h2>
 * <table>
 *   <tr><th>字段</th><th>类型</th><th>Key</th><th>Value</th><th>示例</th></tr>
 *   <tr>
 *     <td>registryHandlers</td>
 *     <td>Map&lt;String, RequestHandler&gt;</td>
 *     <td>泛型 T 的简单类名</td>
 *     <td>Handler Bean 实例</td>
 *     <td>"InstanceRequest" → InstanceRequestHandler</td>
 *   </tr>
 *   <tr>
 *     <td>sourceRegistry</td>
 *     <td>Map&lt;String, Set&lt;String&gt;&gt;</td>
 *     <td>泛型 T 的简单类名</td>
 *     <td>允许的来源集合</td>
 *     <td>"ServerReloadRequest" → {"cluster"}</td>
 *   </tr>
 * </table>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建</b>：Spring 扫描 {@code @Service} 自动实例化（单例）</li>
 *   <li><b>初始化</b>：Spring 容器刷新完成后触发 {@link #onApplicationEvent}，
 *       扫描所有 RequestHandler Bean 并建立索引</li>
 *   <li><b>运行时</b>：{@link #getByRequestType} 和 {@link #checkSourceInvokeAllowed}
 *       持续响应 gRPC 请求处理链路中的查询</li>
 * </ul>
 *
 * @author liuzunfei
 * @version $Id: RequestHandlerRegistry.java, v 0.1 2020年07月13日 8:24 PM liuzunfei Exp $
 */

@Service
public class RequestHandlerRegistry implements ApplicationListener<ContextRefreshedEvent> {
    
    Map<String, RequestHandler> registryHandlers = new HashMap<>();
    
    Map<String, Set<String>> sourceRegistry = new HashMap<>();
    
    /**
     * 运行时根据请求类型查找对应的 Handler —— gRPC 请求分发的核心查表操作。
     *
     * <p>key 是泛型 Request 类型的简单类名（如 {@code "InstanceRequest"}），
     * 与 {@code Payload.metadata.type} 字段一一对应。O(1) 时间复杂度。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>{@link com.alibaba.nacos.core.remote.grpc.GrpcRequestAcceptor#request}
     *       第 117 行 —— 解析 Payload 获取 type 后，
     *       查表获取 Handler 并调用 handleRequest</li>
     * </ul>
     *
     * @param requestType 请求类型的简单类名，对应 Payload.metadata.type 字段
     * @return 对应的 RequestHandler 实例，未找到返回 null
     */
    public RequestHandler getByRequestType(String requestType) {
        return registryHandlers.get(requestType);
    }
    
    /**
     * 校验指定来源是否有权限调用指定类型的请求 —— 安全隔离的核心。
     *
     * <p>默认策略：如果该类型没有在 {@code sourceRegistry} 中显式注册（即没有
     * {@code @InvokeSource} 注解），则<b>允许所有来源</b>调用。只有当显式声明了
     * 允许的来源白名单时，才会拒绝名单外的来源。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>BaseGrpcServer.invokeSourceAllowCheck() 第 192 行 —— 每个 gRPC Unary 请求被 handleCommonRequest 拦截时调用，
     *       确保集群内部专用的 Handler 不会被 SDK 客户端访问</li>
     * </ul>
     *
     * @param type   请求类型的简单类名
     * @param source 请求来源（"sdk" 或 "cluster"），
     *               由 GrpcSdkServer / GrpcClusterServer 的 getSource() 提供
     * @return true 允许调用, false 拒绝
     */
    public boolean checkSourceInvokeAllowed(String type, String source) {
        if (sourceRegistry.containsKey(type) && !sourceRegistry.get(type).contains(source)) {
            return false;
        }
        return true;
    }
    
    /**
     * Spring 容器刷新完成后的回调 —— 扫描所有 RequestHandler Bean 并建立索引。
     *
     * <h3>为什么需要遍历继承链找到直接父类？</h3>
     * <p>{@code RequestHandler<T,S>} 是抽象类，子类继承它后还可以被进一步继承。
     * 例如测试中可能有 {@code MockHandler extends InstanceRequestHandler}。
     * 只有<b>直接继承 RequestHandler</b> 的那个类的泛型参数才有意义——
     * 那才是真正要处理的 Request 类型。通过 while 循环向上查找，
     * 跳过中间的继承层，精确定位到直接子类。</p>
     *
     * <h3>关于 skip 判断</h3>
     * <p>如果遍历到了 Object.class 还没找到 RequestHandler，说明该 Bean
     * 的继承链中没有直接继承 RequestHandler（可能是多层代理或 AOP 增强导致的），
     * 跳过不注册。</p>
     *
     * @param event Spring 容器刷新完成事件
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // Step 1: 从 Spring 容器中获取所有 RequestHandler 子类的 Bean 实例
        //   跨模块扫描 —— config/naming/core/ai 模块的 Handler 都会被收集
        Map<String, RequestHandler> beansOfType =
            event.getApplicationContext().getBeansOfType(RequestHandler.class);
        Collection<RequestHandler> values = beansOfType.values();
        for (RequestHandler requestHandler : values) {
            
            // Step 2: 向上遍历继承链，找到直接 extends RequestHandler 的那个类
            //   例如 InstanceRequestHandler 的继承链：
            //   InstanceRequestHandler → RequestHandler<InstanceRequest, InstanceResponse>
            //   → 找到 InstanceRequestHandler.class
            Class<?> clazz = requestHandler.getClass();
            boolean skip = false;
            while (!clazz.getSuperclass().equals(RequestHandler.class)) {
                if (clazz.getSuperclass().equals(Object.class)) {
                    // 遍历到顶也没找到直接继承 RequestHandler 的类，标记跳过
                    skip = true;
                    break;
                }
                clazz = clazz.getSuperclass();
            }
            if (skip) {
                continue;
            }
            
            // Step 3: 注册 TPS 控制点（如果 handle() 方法标注了 @TpsControl 且 TPS 已启用）
            //   例如 HealthCheckRequestHandler.handle() 上有 @TpsControl(pointName = "HealthCheck")
            //   → 注册一个名为 "HealthCheck" 的限流点
            try {
                Method method = clazz.getMethod("handle", Request.class, RequestMeta.class);
                if (method.isAnnotationPresent(TpsControl.class)
                    && TpsControlConfig.isTpsControlEnabled()) {
                    TpsControl tpsControl = method.getAnnotation(TpsControl.class);
                    String pointName = tpsControl.pointName();
                    ControlManagerCenter.getInstance().getTpsControlManager()
                        .registerTpsPoint(pointName);
                }
            } catch (Exception e) {
                //ignore.
            }
            
            // Step 4: 从泛型参数中提取 Request 类型
            //   clazz.getGenericSuperclass() → RequestHandler<InstanceRequest, InstanceResponse>
            //   getActualTypeArguments()[0] → InstanceRequest.class
            Class tClass = (Class) ((ParameterizedType) clazz.getGenericSuperclass())
                .getActualTypeArguments()[0];
            
            // Step 5: 注册来源白名单（如果类上标注了 @InvokeSource 注解）
            //   例如 @InvokeSource(source = {"cluster"}) → 只有 cluster 来源可调用
            //   没有 @InvokeSource → 不限制来源（允许 sdk 和 cluster 都调用）
            try {
                if (clazz.isAnnotationPresent(InvokeSource.class)) {
                    InvokeSource tpsControl = clazz.getAnnotation(InvokeSource.class);
                    String[] sources = tpsControl.source();
                    if (sources != null && sources.length > 0) {
                        sourceRegistry.put(tClass.getSimpleName(), Sets.newHashSet(sources));
                    }
                }
            } catch (Exception e) {
                //ignore.
            }
            
            // Step 6: 建立 Handler 索引 —— key = Request 类型的简单类名
            //   使用 putIfAbsent 防止同类型被多次注册（如多个模块定义了同名 Handler）
            registryHandlers.putIfAbsent(tClass.getSimpleName(), requestHandler);
        }
    }
}
