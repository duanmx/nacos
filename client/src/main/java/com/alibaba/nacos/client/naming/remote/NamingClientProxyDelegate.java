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

package com.alibaba.nacos.client.naming.remote;

import com.alibaba.nacos.api.ability.constant.AbilityKey;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.Service;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.cache.NamingFuzzyWatchServiceListHolder;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.core.NamingServerListManager;
import com.alibaba.nacos.client.naming.core.ServiceInfoUpdateService;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientManager;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientProxy;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.constant.Constants.Security.SECURITY_INFO_REFRESH_INTERVAL_MILLS;
import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Naming 客户端代理委托者 —— NacosNamingService 与服务端通信的"前台总调度"。
 *
 * <p>委托模式（Delegate Pattern）：自身不直接处理通信逻辑，而是根据实例类型和服务端能力，
 * 将操作委托给内部持有的 {@link NamingGrpcClientProxy}（gRPC 通信）或
 * {@link NamingHttpClientProxy}（HTTP 通信）。</p>
 *
 * <h2>数据流转全景</h2>
 * <pre>{@code
 *   NacosNamingService（用户 API 层）
 *       │
 *       │  clientProxy.xxx() 调用
 *       ▼
 *   NamingClientProxyDelegate（本类，路由层）
 *       │
 *       ├───── registerService / deregisterService
 *       │      → getExecuteClientProxy(instance) 路由决策
 *       │         ├─ 临时实例 → grpcClientProxy（gRPC）
 *       │         └─ 持久实例 → 看服务端能力 → gRPC 或 HTTP
 *       │
 *       ├───── subscribe / unsubscribe / isSubscribed
 *       │      → 始终走 grpcClientProxy（gRPC）
 *       │      → subscribe 还会联动 serviceInfoHolder + serviceInfoUpdateService
 *       │
 *       ├───── batchRegister / batchDeregister / queryInstances / getServiceList
 *       │      → 始终走 grpcClientProxy（gRPC，这些操作不支持 HTTP）
 *       │
 *       └───── serverHealthy
 *              → grpcClientProxy.serverHealthy() || httpClientProxy.serverHealthy()
 * }</pre>
 *
 * <h2>路由决策逻辑</h2>
 * <p>仅 registerService 和 deregisterService 需要路由决策（见
 * {@link #getExecuteClientProxy(Instance)}）：</p>
 * <ul>
 *   <li>临时实例（ephemeral=true）→ 始终走 gRPC</li>
 *   <li>持久实例（ephemeral=false）→ 看服务端是否支持 gRPC 持久化，
 *       支持走 gRPC，不支持走 HTTP</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：NacosNamingService.init() L133-135</li>
 *   <li><b>调用方</b>：NacosNamingService 中所有 clientProxy.xxx() 调用
 *       （registerService L183, deregisterService L242, subscribe L418/L427/L546,
 *       unsubscribe L596, queryInstancesOfService L407, getServiceList L693,
 *       isSubscribed L421/L739, serverHealthy L703）</li>
 *   <li><b>间接调用方</b>：ServiceInfoUpdateService（构造器接收 this 作为参数，
 *       用于定时拉取实例时调用 queryInstancesOfService / isSubscribed / serverHealthy）</li>
 *   <li><b>关闭者</b>：NacosNamingService.shutDown() L709</li>
 * </ul>
 *
 * @author xiweng.yy
 */
public class NamingClientProxyDelegate implements NamingClientProxy {
    
    /**
     * 服务端地址管理器 —— 维护 Nacos Server 地址列表，支持轮询和故障转移。
     */
    private final NamingServerListManager serverListManager;
    
    /**
     * 定时更新服务 —— 订阅后周期性向服务端拉取实例列表，作为 gRPC 推送的兜底。
     * <p>构造器中将 this 传入，ServiceInfoUpdateService 通过它调用
     * queryInstancesOfService / isSubscribed / serverHealthy。</p>
     */
    private final ServiceInfoUpdateService serviceInfoUpdateService;
    
    /**
     * 本地缓存持有者 —— 存储已拉取的实例列表，subscribe 时写入。
     */
    private final ServiceInfoHolder serviceInfoHolder;
    
    /**
     * HTTP 通信代理 —— 兼容 Nacos 1.x 旧版服务端，仅用于持久实例注册/注销。
     */
    private final NamingHttpClientProxy httpClientProxy;
    
    /**
     * gRPC 通信代理 —— 主力通信通道，支持注册/注销/订阅/推送。
     */
    private final NamingGrpcClientProxy grpcClientProxy;
    
    /**
     * 安全代理 —— 处理客户端身份认证（登录 + token 刷新）。
     */
    private final SecurityProxy securityProxy;
    
    /**
     * 安全认证定时刷新线程池 —— 周期性调用 securityProxy.login() 刷新 token。
     */
    private ScheduledExecutorService executorService;
    
    /**
     * 构造 NamingClientProxyDelegate，初始化所有通信组件。
     *
     * <p>调用方：NacosNamingService.init() L133-135。</p>
     *
     * <h3>初始化顺序（6 步）</h3>
     * <ol>
     *   <li>ServiceInfoUpdateService —— 定时拉取兜底，接收 this 用于回调</li>
     *   <li>NamingServerListManager —— 服务端地址管理，start() 加载地址</li>
     *   <li>ServiceInfoHolder —— 外部传入的本地缓存</li>
     *   <li>SecurityProxy —— 安全认证，立即登录 + 定时刷新</li>
     *   <li>NamingHttpClientProxy —— HTTP 兼容代理（旧版服务端）</li>
     *   <li>NamingGrpcClientProxy —— gRPC 主力代理，建连 + 注册推送处理器</li>
     * </ol>
     *
     * @param namespace                       命名空间
     * @param serviceInfoHolder               本地缓存持有者（NacosNamingService 创建）
     * @param properties                      Nacos 客户端配置
     * @param changeNotifier                  实例变更通知器（用于 ServiceInfoUpdateService）
     * @param namingFuzzyWatchServiceListHolder 模糊监听持有者（传给 gRPC 代理）
     * @throws NacosException 初始化失败时抛出
     */
    public NamingClientProxyDelegate(String namespace, ServiceInfoHolder serviceInfoHolder,
        NacosClientProperties properties, InstancesChangeNotifier changeNotifier,
        NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder)
        throws NacosException {

        // 1. 定时拉取兜底 —— 构造器接收 this，通过 clientProxy 回调
        //    queryInstancesOfService / isSubscribed / serverHealthy；
        //    订阅后周期性向服务端拉取实例列表，作为 gRPC 推送的兜底机制
        this.serviceInfoUpdateService =
            new ServiceInfoUpdateService(properties, serviceInfoHolder, this,
                changeNotifier);

        // 2. 服务端地址管理 —— 继承 AbstractServerListManager，实现 ServerListFactory；
        //    start() → 父类 SPI 加载 ServerListProvider → getServerList() 返回地址列表；
        //    genNextServer() 原子递增索引取模，供 RpcClient 建连时轮询选 server
        this.serverListManager = new NamingServerListManager(properties, namespace);
        this.serverListManager.start();

        // 3. 本地实例缓存中心 —— NacosNamingService.init() 中创建，外部传入；
        //    所有实例数据来源（gRPC 推送 / 定时拉取 / 主动订阅）
        //    最终都汇聚到 processServiceInfo() 统一处理
        this.serviceInfoHolder = serviceInfoHolder;

        // 4. 安全代理 —— 客户端鉴权的核心入口，统一管理所有 ClientAuthService 插件：
        //    ① SPI 加载：构造时通过 ClientAuthPluginManager 加载所有 AbstractClientAuthService
        //       实现（NacosClientAuthServiceImpl / OidcClientAuthServiceImpl / LdapClientAuthServiceImpl），
        //       注入 serverList 和 nacosRestTemplate；
        //    ② 周期性登录：initSecurityProxy() 立即 login() + 定时刷新，向服务端
        //       POST /v3/auth/user/login 获取 accessToken，保存在 LoginIdentityContext；
        //    ③ 请求时注入：getIdentityContext(resource) 合并所有插件的 context，
        //       将 accessToken 塞入 HTTP Header / gRPC Metadata；
        //    ④ 403 重登：服务端返回 403 时触发 reLogin()，标记 RELOGINFLAG，
        //       下次 login() 忽略 TTL 窗口强制刷新；监听 ServerListChangeEvent 更新地址
        this.securityProxy = new SecurityProxy(this.serverListManager,
            NamingHttpClientManager.getInstance().getNacosRestTemplate());
        initSecurityProxy(properties);  // 立即登录 + 定时刷新 token

        // 5. HTTP 通信代理 —— 兼容 Nacos 1.x 旧版服务端
        //    仅用于持久实例的注册/注销（旧版服务端不支持 gRPC 持久化）
        this.httpClientProxy =
            new NamingHttpClientProxy(namespace, securityProxy, serverListManager, properties);

        // 6. gRPC 通信代理 —— 主力通信通道（Nacos 2.x+）
        //    支持：实例注册/注销、订阅/取消订阅、服务端主动推送
        this.grpcClientProxy =
            new NamingGrpcClientProxy(namespace, securityProxy, serverListManager, properties,
                serviceInfoHolder, namingFuzzyWatchServiceListHolder);
    }
    
    /**
     * 初始化安全代理 —— 立即登录 + 定时刷新 token。
     * <p>使用 ScheduledThreadPoolExecutor 周期性调用 securityProxy.login()，
     * 刷新间隔由 SECURITY_INFO_REFRESH_INTERVAL_MILLS 控制。</p>
     */
    private void initSecurityProxy(NacosClientProperties properties) {
        this.executorService = new ScheduledThreadPoolExecutor(1,
            new NameThreadFactory("com.alibaba.nacos.client.naming.security"));
        final Properties nacosClientPropertiesView = properties.asProperties();
        this.securityProxy.login(nacosClientPropertiesView);
        this.executorService.scheduleWithFixedDelay(
            () -> securityProxy.login(nacosClientPropertiesView), 0,
            SECURITY_INFO_REFRESH_INTERVAL_MILLS, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 注册实例 —— 根据实例类型路由到 gRPC 或 HTTP。
     * <p>调用方：NacosNamingService.registerInstance() L183。</p>
     */
    @Override
    public void registerService(String serviceName, String groupName, Instance instance)
        throws NacosException {
        getExecuteClientProxy(instance).registerService(serviceName, groupName, instance);
    }
    
    /**
     * 批量注册实例 —— 始终走 gRPC（HTTP 不支持批量操作）。
     * <p>调用方：NacosNamingService.batchRegisterInstance() L192。</p>
     */
    @Override
    public void batchRegisterService(String serviceName, String groupName, List<Instance> instances)
        throws NacosException {
        NAMING_LOGGER.info("batchRegisterInstance instances: {} ,serviceName: {} begin.", instances,
            serviceName);
        if (CollectionUtils.isEmpty(instances)) {
            NAMING_LOGGER.warn("batchRegisterInstance instances is Empty:{}", instances);
        }
        grpcClientProxy.batchRegisterService(serviceName, groupName, instances);
        NAMING_LOGGER.info("batchRegisterInstance instances: {} ,serviceName: {} finish.",
            instances, serviceName);
    }
    
    /**
     * 批量注销实例 —— 始终走 gRPC（HTTP 不支持批量操作）。
     * <p>调用方：NacosNamingService.batchDeregisterInstance() L201。</p>
     */
    @Override
    public void batchDeregisterService(String serviceName, String groupName,
        List<Instance> instances)
        throws NacosException {
        NAMING_LOGGER.info("batch DeregisterInstance instances: {} ,serviceName: {} begin.",
            instances, serviceName);
        if (CollectionUtils.isEmpty(instances)) {
            NAMING_LOGGER.warn("batch DeregisterInstance instances is Empty:{}", instances);
        }
        grpcClientProxy.batchDeregisterService(serviceName, groupName, instances);
        NAMING_LOGGER.info("batch DeregisterInstance instances: {} ,serviceName: {} finish.",
            instances, serviceName);
    }
    
    /**
     * 注销实例 —— 根据实例类型路由到 gRPC 或 HTTP。
     * <p>调用方：NacosNamingService.deregisterInstance() L242。</p>
     */
    @Override
    public void deregisterService(String serviceName, String groupName, Instance instance)
        throws NacosException {
        getExecuteClientProxy(instance).deregisterService(serviceName, groupName, instance);
    }
    
    /**
     * 更新实例 —— 当前未实现（空方法）。
     * <p>接口 NamingClientProxy 定义了此方法，但 NamingClientProxyDelegate 和
     * NamingGrpcClientProxy 均未实现。如需更新实例，应先注销再注册。</p>
     */
    @Override
    public void updateInstance(String serviceName, String groupName, Instance instance)
        throws NacosException {
        
    }
    
    /**
     * 查询服务实例列表 —— 始终走 gRPC。
     * <p>调用方：NacosNamingService.getServiceInfoBySubscribe() L407
     * （subscribe=false 时直接查询服务端）；
     * ServiceInfoUpdateService 定时拉取时也调用此方法。</p>
     *
     * @param serviceName 服务名
     * @param groupName   组名
     * @param clusters    集群名（逗号分隔）
     * @param healthyOnly 是否只查询健康实例
     * @return 服务实例信息
     */
    @Override
    public ServiceInfo queryInstancesOfService(String serviceName, String groupName,
        String clusters,
        boolean healthyOnly) throws NacosException {
        return grpcClientProxy.queryInstancesOfService(serviceName, groupName, clusters,
            healthyOnly);
    }
    
    /**
     * 查询服务信息 —— 当前未实现，返回 null。
     */
    @Override
    public Service queryService(String serviceName, String groupName) throws NacosException {
        return null;
    }
    
    /**
     * 创建服务 —— 当前未实现（空方法）。
     */
    @Override
    public void createService(Service service, AbstractSelector selector) throws NacosException {
        
    }
    
    /**
     * 删除服务 —— 当前未实现，返回 false。
     */
    @Override
    public boolean deleteService(String serviceName, String groupName) throws NacosException {
        return false;
    }
    
    /**
     * 更新服务 —— 当前未实现（空方法）。
     */
    @Override
    public void updateService(Service service, AbstractSelector selector) throws NacosException {
        
    }
    
    /**
     * 分页查询服务列表 —— 始终走 gRPC。
     * <p>调用方：NacosNamingService.getServicesOfServer() L693。</p>
     *
     * @param pageNo   页码（从 1 开始）
     * @param pageSize 每页大小
     * @param groupName 组名
     * @param selector  选择器（仅 label 类型有效）
     * @return 服务名列表 + 总数
     */
    @Override
    public ListView<String> getServiceList(int pageNo, int pageSize, String groupName,
        AbstractSelector selector)
        throws NacosException {
        return grpcClientProxy.getServiceList(pageNo, pageSize, groupName, selector);
    }
    
    /**
     * 订阅服务 —— 本类最复杂的方法，联动 3 个组件。
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>NacosNamingService.tryToSubscribe() L418/L427 —— 首次订阅或缓存未命中时</li>
     *   <li>NacosNamingService.doSubscribe() L546 —— 用户调用 subscribe() 时</li>
     * </ul>
     *
     * <h3>处理流程（4 步）</h3>
     * <ol>
     *   <li>启动定时更新任务（scheduleUpdateIfAbsent）—— 订阅后周期性拉取，兜底 gRPC 推送</li>
     *   <li>查本地缓存（serviceInfoHolder.getServiceInfoMap）—— 有缓存就不用请求服务端</li>
     *   <li>缓存未命中或未订阅 → 向服务端发起 gRPC 订阅（grpcClientProxy.subscribe）</li>
     *   <li>处理结果（serviceInfoHolder.processServiceInfo）—— 写入缓存 + 计算 diff + 发布事件</li>
     * </ol>
     *
     * @param serviceName 服务名
     * @param groupName   组名
     * @param clusters    集群名（当前仅支持订阅全部集群）
     * @return 当前服务实例信息
     */
    @Override
    public ServiceInfo subscribe(String serviceName, String groupName, String clusters)
        throws NacosException {
        NAMING_LOGGER.info("[SUBSCRIBE-SERVICE] service:{}, group:{}, clusters:{} ", serviceName,
            groupName, clusters);
        // Step 1: 启动定时更新任务（如果尚未启动）
        serviceInfoUpdateService.scheduleUpdateIfAbsent(serviceName, groupName, clusters);
        // Step 2: 先查本地缓存，避免不必要的网络请求
        String serviceNameWithGroup = NamingUtils.getGroupedName(serviceName, groupName);
        String serviceKey = ServiceInfo.getKey(serviceNameWithGroup, clusters);
        ServiceInfo result = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
        // Step 3: 缓存未命中 或 尚未订阅 → 向服务端发起 gRPC 订阅
        if (null == result || !isSubscribed(serviceName, groupName, clusters)) {
            result = grpcClientProxy.subscribe(serviceName, groupName, clusters);
        }
        // Step 4: 写入缓存 + 计算 diff + 发布 InstancesChangeEvent + 异步刷盘
        serviceInfoHolder.processServiceInfo(result);
        return result;
    }
    
    /**
     * 取消订阅服务 —— 停止定时更新 + 向服务端发送取消订阅。
     * <p>调用方：NacosNamingService.doUnsubscribe() L596。</p>
     *
     * @param serviceName 服务名
     * @param groupName   组名
     * @param clusters    集群名
     */
    @Override
    public void unsubscribe(String serviceName, String groupName, String clusters)
        throws NacosException {
        NAMING_LOGGER.debug("[UNSUBSCRIBE-SERVICE] service:{}, group:{}, cluster:{} ", serviceName,
            groupName,
            clusters);
        // Step 1: 停止定时更新任务
        serviceInfoUpdateService.stopUpdateIfContain(serviceName, groupName, clusters);
        // Step 2: 向服务端发送取消订阅（gRPC）
        grpcClientProxy.unsubscribe(serviceName, groupName, clusters);
    }
    
    /**
     * 判断是否已订阅指定服务 —— 始终走 gRPC。
     * <p>调用方：NacosNamingService.tryToSubscribe() L421（判断是否已订阅）；
     * NacosNamingService.notifyIfSubscribed() L739（重复订阅检查）；
     * subscribe() 方法内部 L216（决定是否需要请求服务端）。</p>
     *
     * @return true 表示已向服务端发起过订阅请求
     */
    @Override
    public boolean isSubscribed(String serviceName, String groupName, String clusters)
        throws NacosException {
        return grpcClientProxy.isSubscribed(serviceName, groupName, clusters);
    }
    
    /**
     * 检查服务端健康状态 —— gRPC 和 HTTP 任一可用即认为健康。
     * <p>调用方：NacosNamingService.getServerStatus() L703；
     * ServiceInfoUpdateService 定时拉取前检查。</p>
     *
     * @return true 表示 gRPC 或 HTTP 至少有一个连接正常
     */
    @Override
    public boolean serverHealthy() {
        return grpcClientProxy.serverHealthy() || httpClientProxy.serverHealthy();
    }
    
    /**
     * 路由决策 —— 根据实例类型和服务端能力，选择 gRPC 还是 HTTP 通信。
     *
     * <p>仅 registerService 和 deregisterService 调用此方法。
     * 其他方法（subscribe/batch/query 等）始终走 gRPC。</p>
     *
     * <h3>决策条件</h3>
     * <ul>
     *   <li>条件1：instance.isEphemeral()
     *     <p>临时实例（ephemeral=true）靠心跳维持存活，下线自动摘除。
     *     临时实例始终走 gRPC，不需要判断服务端能力。</p></li>
     *   <li>条件2：grpcClientProxy.isAbilitySupportedByServer(SERVER_PERSISTENT_INSTANCE_BY_GRPC)
     *     <p>持久实例（ephemeral=false）需要看服务端是否支持 gRPC 持久化。
     *     客户端建连时通过 ConnectionSetupRequest 与服务端协商能力表，
     *     能力表存在 Connection.abilityTable 中。</p>
     *     <ul>
     *       <li>true  → 新版服务端，支持 gRPC 持久化 → 走 gRPC</li>
     *       <li>false → 旧版服务端，不支持 → 走 HTTP</li>
     *     </ul></li>
     * </ul>
     *
     * @param instance 待注册/注销的实例
     * @return gRPC 或 HTTP 代理
     */
    private NamingClientProxy getExecuteClientProxy(Instance instance) {
        if (instance.isEphemeral() || grpcClientProxy.isAbilitySupportedByServer(
            AbilityKey.SERVER_PERSISTENT_INSTANCE_BY_GRPC)) {
            return grpcClientProxy;
        }
        return httpClientProxy;
    }
    
    /**
     * 关闭所有组件，释放资源。
     * <p>调用方：NacosNamingService.shutDown() L709。</p>
     *
     * <h3>关闭顺序（6 步）</h3>
     * <ol>
     *   <li>serviceInfoUpdateService —— 停止定时拉取线程</li>
     *   <li>serverListManager —— 停止地址管理</li>
     *   <li>httpClientProxy —— 关闭 HTTP 代理</li>
     *   <li>grpcClientProxy —— 关闭 gRPC 连接（最关键，释放连接池）</li>
     *   <li>securityProxy —— 关闭安全代理</li>
     *   <li>executorService —— 关闭 token 刷新线程池</li>
     * </ol>
     */
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        serviceInfoUpdateService.shutdown();
        serverListManager.shutdown();
        httpClientProxy.shutdown();
        grpcClientProxy.shutdown();
        securityProxy.shutdown();
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
