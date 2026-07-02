/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.naming.remote.gprc;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.ability.constant.AbilityKey;
import com.alibaba.nacos.api.ability.constant.AbilityStatus;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.Service;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.remote.NamingRemoteConstants;
import com.alibaba.nacos.api.naming.remote.request.AbstractNamingRequest;
import com.alibaba.nacos.api.naming.remote.request.BatchInstanceRequest;
import com.alibaba.nacos.api.naming.remote.request.InstanceRequest;
import com.alibaba.nacos.api.naming.remote.request.NamingFuzzyWatchRequest;
import com.alibaba.nacos.api.naming.remote.request.PersistentInstanceRequest;
import com.alibaba.nacos.api.naming.remote.request.ServiceListRequest;
import com.alibaba.nacos.api.naming.remote.request.ServiceQueryRequest;
import com.alibaba.nacos.api.naming.remote.request.SubscribeServiceRequest;
import com.alibaba.nacos.api.naming.remote.response.BatchInstanceResponse;
import com.alibaba.nacos.api.naming.remote.response.NamingFuzzyWatchResponse;
import com.alibaba.nacos.api.naming.remote.response.QueryServiceResponse;
import com.alibaba.nacos.api.naming.remote.response.ServiceListResponse;
import com.alibaba.nacos.api.naming.remote.response.SubscribeServiceResponse;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.remote.RemoteConstants;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.api.remote.response.ResponseCode;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.api.selector.SelectorType;
import com.alibaba.nacos.client.address.ServerListChangeEvent;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.cache.NamingFuzzyWatchServiceListHolder;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.remote.AbstractNamingClientProxy;
import com.alibaba.nacos.client.naming.remote.gprc.redo.NamingGrpcRedoService;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.BatchInstanceRedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.InstanceRedoData;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.client.utils.AppNameUtils;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.RpcClient;
import com.alibaba.nacos.common.remote.client.RpcClientConfigFactory;
import com.alibaba.nacos.common.remote.client.RpcClientFactory;
import com.alibaba.nacos.common.remote.client.ServerListFactory;
import com.alibaba.nacos.common.remote.client.grpc.GrpcClientConfig;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.api.utils.json.JsonUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.nacos.api.remote.RemoteConstants.MONITOR_LABEL_NONE;
import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Nacos 2.x/3.x gRPC 命名代理 —— 通过 gRPC 双向流与 Nacos 服务端通信。
 *
 * <p>该类是 Nacos 命名服务客户端 gRPC 通道的核心实现，负责所有注册/注销/订阅/查询操作。
 * 相比 NamingHttpClientProxy（HTTP 1.x 兼容通道），gRPC 通道提供：
 * <ul>
 *   <li><b>双向流</b>：服务端可通过 ServerRequestHandler 主动推送实例变更，无需客户端轮询</li>
 *   <li><b>连接复用</b>：单一 gRPC 连接承载所有命名服务请求</li>
 *   <li><b>Redo 重做</b>：断连重连后自动重做未完成的注册/订阅操作</li>
 * </ul>
 *
 * <h2>ASCII 数据流转全景图</h2>
 * <pre>{@code
 *   NamingClientProxyDelegate
 *          │
 *          ├── registerService() ──────────────────────────────────────────┐
 *          │                                                                ▼
 *          │    ephemeral ──► cacheInstanceForRedo() ──► doRegisterService() ──► requestToServer()
 *          │    persistent ───────────────────────────► doRegisterServiceForPersistent()
 *          │
 *          ├── subscribe() ───────────────────────────────────────────────┐
 *          │                                                               ▼
 *          │    cacheSubscriberForRedo() ──► doSubscribe() ──► requestToServer()
 *          │
 *          ├── deregisterService() ───────────────────────────────────────┐
 *          │                                                               ▼
 *          │    ephemeral ──► instanceDeregister() ──► doDeregisterService() ──► requestToServer()
 *          │    persistent ──────────────────────────► doDeregisterServiceForPersistent()
 *          │
 *          ▼
 *   requestToServer() ──► 注入安全头 ──► rpcClient.request() ──► 检查 403 reLogin()
 *          ▲
 *          │
 *   ServerPush  ◄── NamingPushRequestHandler ◄── gRPC 双向流 ◄── Nacos Server
 *   FuzzyWatch  ◄── NamingFuzzyWatchNotifyRequestHandler
 *          │
 *   RedoScheduledTask ──► NamingGrpcRedoService.redo() ──► 重做未完成的注册/订阅
 * }</pre>
 *
 * <h2>核心协作者（按职责分类）</h2>
 * <table>
 *   <tr><th>协作者</th><th>职责</th><th>交互方式</th></tr>
 *   <tr><td>{@code RpcClient}</td><td>gRPC 底层连接管理（建立/维持/断线重连）</td>
 *        <td>通过 {@code RpcClientFactory.createClient()} 创建，持有引用</td></tr>
 *   <tr><td>{@code NamingGrpcRedoService}</td><td>断连重连后重做未完成的注册/订阅</td>
 *        <td>作为 {@code ConnectionEventListener} 注册到 rpcClient</td></tr>
 *   <tr><td>{@code NamingPushRequestHandler}</td><td>处理服务端主动推送的实例变更</td>
 *        <td>注册为 ServerRequestHandler，更新 ServiceInfoHolder</td></tr>
 *   <tr><td>{@code NamingFuzzyWatchNotifyRequestHandler}</td><td>处理模糊监听通知</td>
 *        <td>注册为 ServerRequestHandler，更新 FuzzyWatchServiceListHolder</td></tr>
 *   <tr><td>{@code NamingFuzzyWatchServiceListHolder}</td><td>管理模糊监听的服务列表</td>
 *        <td>构造时注册 this，start() 时启动，持有反向引用</td></tr>
 *   <tr><td>{@code SecurityProxy}</td><td>提供安全认证头（accessToken）</td>
 *        <td>通过父类 AbstractNamingClientProxy 继承</td></tr>
 * </table>
 *
 * <h2>构造器 8 步初始化流程</h2>
 * <ol>
 *   <li>缓存 namespaceId + UUID + requestTimeout</li>
 *   <li>设置 gRPC labels（source=SDK, module=naming, appName）</li>
 *   <li>注册 this 到 FuzzyWatchServiceListHolder（反向引用）</li>
 *   <li>创建 GrpcClientConfig（通过 RpcClientConfigFactory SPI）</li>
 *   <li>创建 RpcClient（通过 RpcClientFactory，ConnectionType.GRPC）</li>
 *   <li>创建 NamingGrpcRedoService（传入 this 引用）</li>
 *   <li>读取客户端指标开关</li>
 *   <li>调用 start() 完成连接初始化</li>
 * </ol>
 *
 * <h2>临时实例 vs 持久实例双路径</h2>
 * <table>
 *   <tr><th>维度</th><th>临时实例 (ephemeral)</th><th>持久实例 (persistent)</th></tr>
 *   <tr><td>注册</td><td>先缓存 redo → 发送 InstanceRequest → 标记 registered</td>
 *        <td>仅发送 PersistentInstanceRequest，无 redo</td></tr>
 *   <tr><td>注销</td><td>先标记 deregistered → 发送请求 → 移除缓存</td>
 *        <td>仅发送 PersistentInstanceRequest，无 redo</td></tr>
 *   <tr><td>Redo</td><td>支持（断连重连后自动重做）</td>
 *        <td>不支持（依赖服务端 CP 一致性）</td></tr>
 *   <tr><td>批量</td><td>BatchInstanceRequest + BatchInstanceRedoData</td>
 *        <td>不支持批量操作</td></tr>
 * </table>
 *
 * <h2>与 NamingHttpClientProxy 的对比</h2>
 * <table>
 *   <tr><th>维度</th><th>NamingGrpcClientProxy</th><th>NamingHttpClientProxy</th></tr>
 *   <tr><td>协议</td><td>gRPC 双向流</td><td>HTTP REST</td></tr>
 *   <tr><td>服务端推送</td><td>原生支持（ServerRequestHandler）</td><td>需要 UDP 推送辅助</td></tr>
 *   <tr><td>连接管理</td><td>单一长连接</td><td>连接池（NamingHttpClientManager）</td></tr>
 *   <tr><td>Redo</td><td>NamingGrpcRedoService 自动重做</td><td>无（每次请求独立）</td></tr>
 *   <tr><td>适用版本</td><td>Nacos 2.x/3.x</td><td>Nacos 1.x（兼容）</td></tr>
 *   <tr><td>路由决策</td><td colspan="2">NamingClientProxyDelegate.getExecuteClientProxy() L476-482<br>
 *       根据 instance.isEphemeral() + ServerAbility 决定走 HTTP 还是 gRPC</td></tr>
 * </table>
 *
 * <h2>403 重登机制</h2>
 * <p>每次 requestToServer() 调用前，通过父类 getSecurityHeaders() 注入身份认证信息。
 * 若服务端返回 403（NO_RIGHT），自动触发 reLogin() 刷新 accessToken，
 * 下次 login() 忽略 TTL 窗口强制刷新。</p>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建</b>：NamingClientProxyDelegate 构造器 L197-198（Step 6）</li>
 *   <li><b>运行</b>：通过 RedoScheduledTask 定时重做 + ServerRequestHandler 接收推送</li>
 *   <li><b>关闭</b>：NamingClientProxyDelegate.shutdown() L505 → redoService.shutdown()
 *                        → RpcClientFactory.destroyClient() → deregisterSubscriber()</li>
 * </ul>
 *
 * @author xiweng.yy
 * @see com.alibaba.nacos.client.naming.remote.AbstractNamingClientProxy
 * @see NamingGrpcRedoService
 * @see com.alibaba.nacos.client.naming.remote.http.NamingHttpClientProxy
 */
public class NamingGrpcClientProxy extends AbstractNamingClientProxy {
    
    /** 当前客户端操作的命名空间 ID，来自 NacosClientProperties 中的 namespace 配置。 */
    private final String namespaceId;
    
    /** 客户端唯一标识（UUID），用于标识 gRPC 连接，注册到 RpcClientFactory 时作为 key。 */
    private final String uuid;
    
    /**
     * 请求超时时间（毫秒），-1 表示使用 RpcClient 默认超时。
     * 来自属性 CommonParams.NAMING_REQUEST_TIMEOUT。
     */
    private final Long requestTimeout;
    
    /** gRPC 客户端实例，负责底层连接管理、请求发送、连接监听。 */
    private final RpcClient rpcClient;
    
    /**
     * Redo 服务，断连重连后自动重做未完成的注册/订阅操作。
     * 同时作为 ConnectionEventListener 注册到 rpcClient。
     */
    private final NamingGrpcRedoService redoService;
    
    /**
     * 是否启用客户端指标监控，来自属性 PropertyKeyConst.ENABLE_CLIENT_METRICS，
     * 默认 true。关闭可减少 MetricsMonitor 开销。
     */
    private boolean enableClientMetrics = true;
    
    /**
     * 构造 NamingGrpcClientProxy 并完成 8 步初始化。
     *
     * @param namespaceId                      命名空间 ID
     * @param securityProxy                    安全代理（客户端鉴权）
     * @param serverListFactory               服务端地址列表工厂
     * @param properties                      客户端配置属性
     * @param serviceInfoHolder               服务信息缓存持有者
     * @param namingFuzzyWatchServiceListHolder 模糊监听服务列表持有者
     * @throws NacosException 如果 gRPC 连接建立失败
     */
    public NamingGrpcClientProxy(String namespaceId, SecurityProxy securityProxy,
        ServerListFactory serverListFactory,
        NacosClientProperties properties, ServiceInfoHolder serviceInfoHolder,
        NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder)
        throws NacosException {
        // Step 1: 缓存基础配置：namespaceId + UUID + requestTimeout
        super(securityProxy);
        this.namespaceId = namespaceId;
        this.uuid = UUID.randomUUID().toString();
        this.requestTimeout =
            Long.parseLong(properties.getProperty(CommonParams.NAMING_REQUEST_TIMEOUT, "-1"));
        // Step 2: 设置 gRPC 标签（source=SDK, module=naming）+ 应用名
        Map<String, String> labels = new HashMap<>();
        labels.put(RemoteConstants.LABEL_SOURCE, RemoteConstants.LABEL_SOURCE_SDK);
        labels.put(RemoteConstants.LABEL_MODULE, RemoteConstants.LABEL_MODULE_NAMING);
        labels.put(Constants.APPNAME, AppNameUtils.getAppName());
        // Step 3: 注册反向引用到 FuzzyWatchServiceListHolder（用于模糊监听）
        namingFuzzyWatchServiceListHolder.registerNamingGrpcClientProxy(this);
        // Step 4: 创建 gRPC 客户端配置（线程数、超时等参数）
        GrpcClientConfig grpcClientConfig = RpcClientConfigFactory.getInstance()
            .createGrpcClientConfig(properties.asProperties(), labels);
        // Step 5: 通过工厂创建 RpcClient（使用 ConnectionType.GRPC）
        this.rpcClient = RpcClientFactory.createClient(uuid, ConnectionType.GRPC, grpcClientConfig);
        // Step 6: 创建 Redo 服务（持有 this 引用用于重做）
        this.redoService =
            new NamingGrpcRedoService(this, namingFuzzyWatchServiceListHolder, properties);
        // Step 7: 读取客户端指标开关
        this.enableClientMetrics = Boolean.parseBoolean(
            properties.getProperty(PropertyKeyConst.ENABLE_CLIENT_METRICS, "true"));
        NAMING_LOGGER.info("Create naming rpc client for uuid->{}", uuid);
        // Step 8: 启动 gRPC 连接并注册处理器
        start(serverListFactory, serviceInfoHolder, namingFuzzyWatchServiceListHolder);
    }
    
    /**
     * 启动 gRPC 连接，完成 6 步初始化：
     * <ol>
     *   <li>设置 ServerListFactory（服务端地址来源）</li>
     *   <li>注册 ConnectionEventListener（RedoService 监听断连/重连）</li>
     *   <li>注册 ServerRequestHandler × 2（NamingPush + NamingFuzzyWatch）</li>
     *   <li>启动 RpcClient（建立 gRPC 连接）</li>
     *   <li>启动 FuzzyWatchServiceListHolder</li>
     *   <li>注册 this 为 NotifyCenter 订阅者（监听 ServerListChangeEvent）</li>
     * </ol>
     */
    private void start(ServerListFactory serverListFactory, ServiceInfoHolder serviceInfoHolder,
        NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder)
        throws NacosException {
        // Step 1: 注入服务端地址工厂
        rpcClient.serverListFactory(serverListFactory);
        // Step 2: RedoService 同时作为 ConnectionEventListener 注册
        rpcClient.registerConnectionListener(redoService);
        // Step 3: 注册两个 ServerRequestHandler 处理服务端主动推送
        rpcClient.registerServerRequestHandler(new NamingPushRequestHandler(serviceInfoHolder));
        rpcClient.registerServerRequestHandler(
            new NamingFuzzyWatchNotifyRequestHandler(namingFuzzyWatchServiceListHolder));
        // Step 4: 启动 gRPC 连接
        rpcClient.start();
        // Step 5: 启动模糊监听
        namingFuzzyWatchServiceListHolder.start();
        // Step 6: 注册 this 为 NotifyCenter 订阅者，监听 ServerListChangeEvent
        NotifyCenter.registerSubscriber(this);
    }
    
    @Override
    public void onEvent(ServerListChangeEvent event) {
        rpcClient.onServerListChange();
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return ServerListChangeEvent.class;
    }
    
    /**
     * 注册服务实例。根据实例类型分流：
     * <ul>
     *   <li><b>临时实例</b>：先缓存到 redo → 发送 InstanceRequest → 标记 registered</li>
     *   <li><b>持久实例</b>：仅发送 PersistentInstanceRequest，无 redo（依赖服务端 CP 一致性）</li>
     * </ul>
     */
    @Override
    public void registerService(String serviceName, String groupName, Instance instance)
        throws NacosException {
        NAMING_LOGGER.info("[REGISTER-SERVICE] {} registering service {} with instance {}",
            namespaceId, serviceName,
            instance);
        if (instance.isEphemeral()) {
            // 临时实例：先缓存 redo（保障断连重连后自动重做）→ 发送注册请求
            registerServiceForEphemeral(serviceName, groupName, instance);
        } else {
            // 持久实例：仅发送注册请求（服务端 CP 协议保证一致性）
            doRegisterServiceForPersistent(serviceName, groupName, instance);
        }
    }
    
    /**
     * 临时实例注册：先缓存 redo，再发送请求。
     *
     * <p>缓存在前、请求在后，确保即使请求发送后立即断连，redo 中也有记录可重做。</p>
     */
    private void registerServiceForEphemeral(String serviceName, String groupName,
        Instance instance)
        throws NacosException {
        // Step 1: 缓存到 redo（标记为未注册状态，请求成功后再标记为 registered）
        redoService.cacheInstanceForRedo(serviceName, groupName, instance);
        // Step 2: 发送 gRPC 注册请求
        doRegisterService(serviceName, groupName, instance);
    }
    
    /**
     * 批量注册服务实例。同样采用先缓存 redo 再发请求的策略。
     */
    @Override
    public void batchRegisterService(String serviceName, String groupName, List<Instance> instances)
        throws NacosException {
        redoService.cacheInstanceForRedo(serviceName, groupName, instances);
        doBatchRegisterService(serviceName, groupName, instances);
    }
    
    /**
     * 批量注销服务实例。
     *
     * <p>采用"差集计算"策略：从已注册列表中减去待注销实例，将剩余实例重新批量注册。
     * 加锁保护 registeredInstances 的并发读写。</p>
     */
    @Override
    public void batchDeregisterService(String serviceName, String groupName,
        List<Instance> instances)
        throws NacosException {
        synchronized (redoService.getRegisteredInstances()) {
            // 计算差集：已注册列表 - 待注销列表 = 需要保留的实例
            List<Instance> retainInstance = getRetainInstance(serviceName, groupName, instances);
            // 将保留的实例重新批量注册（覆盖式更新）
            batchRegisterService(serviceName, groupName, retainInstance);
        }
    }
    
    /**
     * Get instance list that need to be Retained.
     *
     * @param serviceName         service name
     * @param groupName           group name
     * @param deRegisterInstances deregister instance list
     * @return instance list that need to be retained.
     */
    private List<Instance> getRetainInstance(String serviceName, String groupName,
        List<Instance> deRegisterInstances)
        throws NacosException {
        if (CollectionUtils.isEmpty(deRegisterInstances)) {
            throw new NacosException(NacosException.INVALID_PARAM,
                String.format(
                    "[Batch deRegistration] need deRegister instance is empty, instances: %s,",
                    deRegisterInstances));
        }
        String combinedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
        InstanceRedoData instanceRedoData =
            redoService.getRegisteredInstancesByKey(combinedServiceName);
        if (!(instanceRedoData instanceof BatchInstanceRedoData)) {
            throw new NacosException(NacosException.INVALID_PARAM, String.format(
                "[Batch deRegistration] batch deRegister is not BatchInstanceRedoData type , instances: %s,",
                deRegisterInstances));
        }
        
        BatchInstanceRedoData batchInstanceRedoData = (BatchInstanceRedoData) instanceRedoData;
        List<Instance> allRedoInstances = batchInstanceRedoData.getInstances();
        if (CollectionUtils.isEmpty(allRedoInstances)) {
            throw new NacosException(NacosException.INVALID_PARAM, String.format(
                "[Batch deRegistration] not found all registerInstance , serviceName：%s , groupName: %s",
                serviceName, groupName));
        }
        
        Map<Instance, Instance> deRegisterInstanceMap = deRegisterInstances.stream()
            .collect(Collectors.toMap(Function.identity(), Function.identity()));
        List<Instance> retainInstances = new ArrayList<>();
        for (Instance redoInstance : allRedoInstances) {
            boolean needRetained = true;
            Iterator<Map.Entry<Instance, Instance>> it =
                deRegisterInstanceMap.entrySet().iterator();
            while (it.hasNext()) {
                Instance deRegisterInstance = it.next().getKey();
                // only compare Ip & Port because redoInstance's instanceId or serviceName might be null but deRegisterInstance's might not be null.
                if (compareIpAndPort(deRegisterInstance, redoInstance)) {
                    needRetained = false;
                    // clear current entry to speed up next redoInstance comparing.
                    it.remove();
                    break;
                }
            }
            if (needRetained) {
                retainInstances.add(redoInstance);
            }
        }
        return retainInstances;
    }
    
    private boolean compareIpAndPort(Instance deRegisterInstance, Instance redoInstance) {
        return ((deRegisterInstance.getIp().equals(redoInstance.getIp()))
            && (deRegisterInstance.getPort() == redoInstance.getPort()));
    }
    
    /**
     * Execute batch register operation.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param instances   instances
     * @throws NacosException NacosException
     */
    public void doBatchRegisterService(String serviceName, String groupName,
        List<Instance> instances)
        throws NacosException {
        BatchInstanceRequest request = new BatchInstanceRequest(namespaceId, serviceName, groupName,
            NamingRemoteConstants.BATCH_REGISTER_INSTANCE, instances);
        requestToServer(request, BatchInstanceResponse.class);
        redoService.instanceRegistered(serviceName, groupName);
    }
    
    /**
     * Execute register operation.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param instance    instance to register
     * @throws NacosException nacos exception
     */
    public void doRegisterService(String serviceName, String groupName, Instance instance)
        throws NacosException {
        InstanceRequest request = new InstanceRequest(namespaceId, serviceName, groupName,
            NamingRemoteConstants.REGISTER_INSTANCE, instance);
        requestToServer(request, Response.class);
        redoService.instanceRegistered(serviceName, groupName);
    }
    
    /**
     * Execute register operation for persistent instance.
     *
     * @param serviceName name of service
     * @param groupName   group of service
     * @param instance    instance to register
     * @throws NacosException nacos exception
     */
    public void doRegisterServiceForPersistent(String serviceName, String groupName,
        Instance instance)
        throws NacosException {
        PersistentInstanceRequest request =
            new PersistentInstanceRequest(namespaceId, serviceName, groupName,
                NamingRemoteConstants.REGISTER_INSTANCE, instance);
        requestToServer(request, Response.class);
    }
    
    /**
     * 注销服务实例。根据实例类型分流：
     * <ul>
     *   <li><b>临时实例</b>：从 redo 中移除 → 发送 InstanceRequest → 标记 deregistered</li>
     *   <li><b>持久实例</b>：仅发送 PersistentInstanceRequest，无 redo</li>
     * </ul>
     */
    @Override
    public void deregisterService(String serviceName, String groupName, Instance instance)
        throws NacosException {
        NAMING_LOGGER.info("[DEREGISTER-SERVICE] {} deregistering service {} with instance: {}",
            namespaceId,
            serviceName, instance);
        if (instance.isEphemeral()) {
            deregisterServiceForEphemeral(serviceName, groupName, instance);
        } else {
            doDeregisterServiceForPersistent(serviceName, groupName, instance);
        }
    }
    
    /**
     * 临时实例注销。
     *
     * <p>特殊处理：如果当前服务采用批量注册模式（BatchInstanceRedoData），
     * 则走批量注销流程（差集计算后重新批量注册）；否则走单实例注销流程。</p>
     */
    private void deregisterServiceForEphemeral(String serviceName, String groupName,
        Instance instance)
        throws NacosException {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        InstanceRedoData instanceRedoData = redoService.getRegisteredInstancesByKey(key);
        if (instanceRedoData instanceof BatchInstanceRedoData) {
            // 批量注册模式 → 走批量注销（差集计算后重新批量注册）
            List<Instance> instances = new ArrayList<>();
            if (null != instance) {
                instances.add(instance);
            }
            batchDeregisterService(serviceName, groupName, instances);
        } else {
            // 单实例注册模式 → 标记 deregistered → 发送注销请求
            redoService.instanceDeregister(serviceName, groupName);
            doDeregisterService(serviceName, groupName, instance);
        }
    }
    
    /**
     * Execute deregister operation.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param instance    instance
     * @throws NacosException nacos exception
     */
    public void doDeregisterService(String serviceName, String groupName, Instance instance)
        throws NacosException {
        InstanceRequest request = new InstanceRequest(namespaceId, serviceName, groupName,
            NamingRemoteConstants.DE_REGISTER_INSTANCE, instance);
        requestToServer(request, Response.class);
        redoService.instanceDeregistered(serviceName, groupName);
    }
    
    /**
     * Execute deregister operation for persistent instance.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param instance    instance
     * @throws NacosException nacos exception
     */
    public void doDeregisterServiceForPersistent(String serviceName, String groupName,
        Instance instance)
        throws NacosException {
        PersistentInstanceRequest request =
            new PersistentInstanceRequest(namespaceId, serviceName, groupName,
                NamingRemoteConstants.DE_REGISTER_INSTANCE, instance);
        requestToServer(request, Response.class);
    }
    
    @Override
    public void updateInstance(String serviceName, String groupName, Instance instance)
        throws NacosException {
    }
    
    @Override
    public ServiceInfo queryInstancesOfService(String serviceName, String groupName,
        String clusters,
        boolean healthyOnly) throws NacosException {
        ServiceQueryRequest request = new ServiceQueryRequest(namespaceId, serviceName, groupName);
        request.setCluster(clusters);
        request.setHealthyOnly(healthyOnly);
        QueryServiceResponse response = requestToServer(request, QueryServiceResponse.class);
        return response.getServiceInfo();
    }
    
    @Override
    public Service queryService(String serviceName, String groupName) throws NacosException {
        return null;
    }
    
    @Override
    public void createService(Service service, AbstractSelector selector) throws NacosException {
    }
    
    @Override
    public boolean deleteService(String serviceName, String groupName) throws NacosException {
        return false;
    }
    
    @Override
    public void updateService(Service service, AbstractSelector selector) throws NacosException {
    }
    
    @Override
    public ListView<String> getServiceList(int pageNo, int pageSize, String groupName,
        AbstractSelector selector)
        throws NacosException {
        ServiceListRequest request =
            new ServiceListRequest(namespaceId, groupName, pageNo, pageSize);
        if (selector != null) {
            if (SelectorType.valueOf(selector.getType()) == SelectorType.label) {
                request.setSelector(JsonUtils.toJson(selector));
            }
        }
        ServiceListResponse response = requestToServer(request, ServiceListResponse.class);
        ListView<String> result = new ListView<>();
        result.setCount(response.getCount());
        result.setData(response.getServiceNames());
        return result;
    }
    
    /**
     * 订阅服务。先缓存 redo，再发送订阅请求。
     *
     * <p>缓存在前确保断连重连后自动重做订阅操作。
     * 返回值是服务端当前的服务信息（包含所有实例列表），
     * 客户端将其缓存到 ServiceInfoHolder 中。</p>
     *
     * @return 当前服务信息（包含所有实例列表）
     */
    @Override
    public ServiceInfo subscribe(String serviceName, String groupName, String clusters)
        throws NacosException {
        NAMING_LOGGER.info("[GRPC-SUBSCRIBE] service:{}, group:{}, cluster:{} ", serviceName,
            groupName, clusters);
        // Step 1: 缓存订阅到 redo（标记为未订阅状态，请求成功后再标记为 subscribed）
        redoService.cacheSubscriberForRedo(serviceName, groupName, clusters);
        // Step 2: 发送 gRPC 订阅请求
        return doSubscribe(serviceName, groupName, clusters);
    }
    
    /**
     * Execute subscribe operation.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters, current only support subscribe all clusters, maybe deprecated
     * @return current service info of subscribe service
     * @throws NacosException nacos exception
     */
    public ServiceInfo doSubscribe(String serviceName, String groupName, String clusters)
        throws NacosException {
        SubscribeServiceRequest request =
            new SubscribeServiceRequest(namespaceId, groupName, serviceName, clusters,
                true);
        SubscribeServiceResponse response =
            requestToServer(request, SubscribeServiceResponse.class);
        redoService.subscriberRegistered(serviceName, groupName, clusters);
        return response.getServiceInfo();
    }
    
    /**
     * 取消订阅。先从 redo 中标记 deregister，再发送取消订阅请求。
     */
    @Override
    public void unsubscribe(String serviceName, String groupName, String clusters)
        throws NacosException {
        NAMING_LOGGER.info("[GRPC-UNSUBSCRIBE] service:{}, group:{}, cluster:{} ", serviceName,
            groupName, clusters);
        // Step 1: 从 redo 中标记为未订阅
        redoService.subscriberDeregister(serviceName, groupName, clusters);
        // Step 2: 发送 gRPC 取消订阅请求
        doUnsubscribe(serviceName, groupName, clusters);
    }
    
    @Override
    public boolean isSubscribed(String serviceName, String groupName, String clusters)
        throws NacosException {
        return redoService.isSubscriberRegistered(serviceName, groupName, clusters);
    }
    
    /**
     * Execute unsubscribe operation.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters, current only support subscribe all clusters, maybe deprecated
     * @throws NacosException nacos exception
     */
    public void doUnsubscribe(String serviceName, String groupName, String clusters)
        throws NacosException {
        SubscribeServiceRequest request =
            new SubscribeServiceRequest(namespaceId, groupName, serviceName, clusters,
                false);
        requestToServer(request, SubscribeServiceResponse.class);
        redoService.removeSubscriberForRedo(serviceName, groupName, clusters);
    }
    
    @Override
    public boolean serverHealthy() {
        return rpcClient.isRunning();
    }
    
    /**
     * Determine whether nacos-server supports the capability.
     *
     * @param abilityKey ability key
     * @return true if supported, otherwise false
     */
    public boolean isAbilitySupportedByServer(AbilityKey abilityKey) {
        return rpcClient.getConnectionAbility(abilityKey) == AbilityStatus.SUPPORTED;
    }
    
    /**
     * Execute unsubscribe operation.
     *
     * @param namingFuzzyWatchRequest namingFuzzyWatchRequest
     * @throws NacosException nacos exception
     */
    public NamingFuzzyWatchResponse fuzzyWatchRequest(
        NamingFuzzyWatchRequest namingFuzzyWatchRequest)
        throws NacosException {
        return requestToServer(namingFuzzyWatchRequest, NamingFuzzyWatchResponse.class);
    }
    
    /**
     * 向 Nacos 服务端发送 gRPC 请求的核心方法。
     *
     * <h3>执行流程（5 步）</h3>
     * <ol>
     *   <li><b>注入安全头</b>：根据请求类型调用 getSecurityHeaders() 注入 accessToken
     *       <ul>
     *         <li>AbstractNamingRequest → 传入 namespace + groupName + serviceName</li>
     *         <li>NamingFuzzyWatchRequest → 仅传入 namespace</li>
     *       </ul>
     *   </li>
     *   <li><b>发送请求</b>：通过 rpcClient.request() 发送 gRPC 请求；
     *       若 requestTimeout >= 0 则使用指定超时，否则使用 RpcClient 默认超时</li>
     *   <li><b>检查响应码</b>：
     *       <ul>
     *         <li>403（NO_RIGHT）→ 触发 reLogin() 刷新 accessToken，然后抛出 NacosException</li>
     *         <li>其他非 SUCCESS 码 → 直接抛出 NacosException</li>
     *       </ul>
     *   </li>
     *   <li><b>类型校验</b>：校验响应类型与期望类型一致，不一致则抛出 SERVER_ERROR</li>
     *   <li><b>指标记录</b>：异常时记录失败指标到 MetricsMonitor</li>
     * </ol>
     *
     * <h3>403 重登机制</h3>
     * <p>当 accessToken 过期或被撤销时，服务端返回 403（NO_RIGHT）。
     * 该方法调用父类 reLogin() 触发 SecurityProxy 强制刷新 token，
     * 下次 login() 忽略 TTL 窗口重新向服务端 POST /v3/auth/user/login 获取新 token。</p>
     *
     * <h3>两种安全头注入路径</h3>
     * <p>gRPC 协议的服务发现请求主要有两类：
     * <ul>
     *   <li>AbstractNamingRequest（注册/注销/订阅/查询）—— 需要 namespace + groupName + serviceName</li>
     *   <li>NamingFuzzyWatchRequest（模糊监听）—— 仅需要 namespace</li>
     * </ul>
     * 通过 instanceof 判断请求类型，调用不同的安全头注入方式。</p>
     *
     * @param <T>           期望的响应类型
     * @param request       gRPC 请求对象
     * @param responseClass 期望的响应 Class
     * @return 服务端响应
     * @throws NacosException 请求失败、403 无权限、响应类型不匹配时抛出
     */
    private <T extends Response> T requestToServer(Request request, Class<T> responseClass)
        throws NacosException {
        Response response = null;
        try {
            // Step 1: 注入安全认证头（accessToken）
            if (request instanceof AbstractNamingRequest) {
                request.putAllHeader(
                    getSecurityHeaders(((AbstractNamingRequest) request).getNamespace(),
                        ((AbstractNamingRequest) request).getGroupName(),
                        ((AbstractNamingRequest) request).getServiceName()));
            } else if (request instanceof NamingFuzzyWatchRequest) {
                request.putAllHeader(
                    getSecurityHeaders(((NamingFuzzyWatchRequest) request).getNamespace(), null,
                        null));
            } else {
                throw new NacosException(400, "unknown naming request type");
            }
            
            // Step 2: 发送 gRPC 请求（使用指定超时或默认超时）
            response = requestTimeout < 0 ? rpcClient.request(request)
                : rpcClient.request(request, requestTimeout);
            // Step 3: 检查响应码
            if (ResponseCode.SUCCESS.getCode() != response.getResultCode()) {
                // Step 3a: 403 无权限 → 重新登录刷新 accessToken
                if (NacosException.NO_RIGHT == response.getErrorCode()) {
                    reLogin();
                }
                // Step 3b: 抛出异常（调用方自行处理重试逻辑）
                throw new NacosException(response.getErrorCode(), response.getMessage());
            }
            // Step 4: 校验响应类型是否与期望一致
            if (responseClass.isAssignableFrom(response.getClass())) {
                return (T) response;
            }
            NAMING_LOGGER.error(
                "Server return unexpected response '{}', expected response should be '{}'",
                response.getClass().getName(), responseClass.getName());
            throw new NacosException(NacosException.SERVER_ERROR, "Server return invalid response");
        } catch (NacosException e) {
            // 记录失败指标 → 原样抛出（调用方可能基于 errorCode 做重试判断）
            recordRequestFailedMetrics(request, e, response);
            throw e;
        } catch (Exception e) {
            // 记录失败指标 → 包装为 NacosException 抛出
            recordRequestFailedMetrics(request, e, response);
            throw new NacosException(NacosException.SERVER_ERROR, "Request nacos server failed: ",
                e);
        }
    }
    
    /**
     * Records registration metrics for a service instance.
     *
     * @param request   The registration request object.
     * @param exception The Exception encountered during the registration process, or null if registration was
     *                  successful.
     * @param response  The response object containing registration result information, or null if registration failed.
     */
    private void recordRequestFailedMetrics(Request request, Exception exception,
        Response response) {
        if (!enableClientMetrics) {
            return;
        }
        
        try {
            if (Objects.isNull(response)) {
                MetricsMonitor.getNamingRequestFailedMonitor(request.getClass().getSimpleName(),
                    MONITOR_LABEL_NONE,
                    MONITOR_LABEL_NONE, exception.getClass().getSimpleName()).inc();
            } else {
                MetricsMonitor.getNamingRequestFailedMonitor(request.getClass().getSimpleName(),
                    String.valueOf(response.getResultCode()),
                    String.valueOf(response.getErrorCode()),
                    MONITOR_LABEL_NONE).inc();
            }
        } catch (Throwable t) {
            NAMING_LOGGER.warn("Fail to record metrics for request {}",
                request.getClass().getSimpleName(), t);
        }
    }
    
    /**
     * 关闭 gRPC 命名代理。关闭顺序：
     * <ol>
     *   <li>关闭 RedoService（停止定时重做任务 + 清理缓存）</li>
     *   <li>销毁 RpcClient（关闭 gRPC 连接，从 RpcClientFactory 移除）</li>
     *   <li>从 NotifyCenter 注销订阅者</li>
     * </ol>
     */
    @Override
    public void shutdown() throws NacosException {
        NAMING_LOGGER.info("Shutdown naming grpc client proxy for  uuid->{}", uuid);
        // Step 1: 关闭 Redo 服务（停止定时重做 + 清理缓存）
        redoService.shutdown();
        // Step 2: 销毁 RpcClient（关闭 gRPC 连接）
        shutDownAndRemove(uuid);
        // Step 3: 从 NotifyCenter 注销 this
        NotifyCenter.deregisterSubscriber(this);
    }
    
    private void shutDownAndRemove(String uuid) {
        synchronized (RpcClientFactory.getAllClientEntries()) {
            try {
                RpcClientFactory.destroyClient(uuid);
                NAMING_LOGGER.info("shutdown and remove naming rpc client  for uuid ->{}", uuid);
            } catch (NacosException e) {
                NAMING_LOGGER.warn("Fail to shutdown naming rpc client  for uuid ->{}", uuid);
            }
        }
    }
    
    public boolean isEnable() {
        return rpcClient.isRunning();
    }
    
    public String getNamespaceId() {
        return namespaceId;
    }
}
