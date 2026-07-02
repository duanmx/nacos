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

package com.alibaba.nacos.client.naming.remote.http;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.Service;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.selector.AbstractSelector;
import com.alibaba.nacos.api.selector.ExpressionSelector;
import com.alibaba.nacos.api.selector.SelectorType;
import com.alibaba.nacos.api.utils.json.JsonUtils;
import com.alibaba.nacos.api.utils.json.NacosTypeReference;
import com.alibaba.nacos.client.address.ServerListChangeEvent;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.core.NamingServerListManager;
import com.alibaba.nacos.client.naming.remote.AbstractNamingClientProxy;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.common.http.HttpRestResult;
import com.alibaba.nacos.common.http.HttpUtils;
import com.alibaba.nacos.common.http.client.NacosRestTemplate;
import com.alibaba.nacos.common.http.param.Header;
import com.alibaba.nacos.common.http.param.Query;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.HttpMethod;
import com.alibaba.nacos.common.utils.InternetAddressUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import org.apache.hc.core5.http.HttpStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;
import static com.alibaba.nacos.common.constant.RequestUrlConstants.HTTPS_PREFIX;
import static com.alibaba.nacos.common.constant.RequestUrlConstants.HTTP_PREFIX;

/**
 * Nacos 1.x HTTP 命名代理 —— 通过 HTTP REST 与旧版 Nacos 服务端通信，兼容存量部署。
 *
 * <p><b>整体定位</b>：已废弃（{@code @Deprecated}），官方计划在 naming 客户端完全迁移到
 * gRPC 后删除。当前仅作为 <b>持久实例</b>（persistent instance）向不支持 gRPC 持久化
 * 的旧版服务端注册/注销时的兜底通道。</p>
 *
 * <h2>与 gRPC 代理的路由决策</h2>
 * <p>调用方 NamingClientProxyDelegate.getExecuteClientProxy() L476-482 决定走 HTTP 还是 gRPC：</p>
 * <pre>{@code
 *   if (instance.isEphemeral() || grpcClientProxy.isAbilitySupportedByServer(
 *       AbilityKey.SERVER_PERSISTENT_INSTANCE_BY_GRPC)) {
 *       return grpcClientProxy;    // 临时实例 or 服务端支持 gRPC 持久化 → gRPC
 *   }
 *   return httpClientProxy;        // 持久实例 & 旧版服务端 → HTTP（本类）
 * }</pre>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   业务层（NamingClientProxyDelegate / NacosNamingMaintainService）
 *     │ registerService / deregisterService / createService / ...
 *     ↓
 *   组装 HTTP 参数（ip, port, weight, ephemeral, metadata...）
 *     ↓
 *   reqApi(api, params, method)
 *     ├─ 注入 namespaceId
 *     ├─ 从 serverListManager 获取 server 列表
 *     └─ 分流两条路径：
 *         ├─ isDomain() = true  → 域名模式：对同一域名重试 maxRetry 次
 *         └─ isDomain() = false → 多 server 模式：随机起点轮询所有 server
 *     ↓
 *   callServer(api, params, body, server, method)
 *     ├─ getSecurityHeaders() → 通过父类调用 securityProxy.getIdentityContext() 获取 accessToken
 *     ├─ 拼接 URL → nacosRestTemplate.exchangeForm() → HTTP 请求
 *     ├─ 200 → 返回结果
 *     ├─ 304 → 返回空字符串
 *     └─ 403 → reLogin() 触发 token 刷新 → 抛异常给上层重试
 * }</pre>
 *
 * <h2>域名模式 vs 多 server 模式（重试策略差异）</h2>
 * <table>
 *   <tr><th>条件</th><th>模式</th><th>重试策略</th></tr>
 *   <tr><td>PropertiesListProvider + 单地址</td><td>域名模式</td><td>对同一域名重试 {@code maxRetry} 次</td></tr>
 *   <tr><td>多地址 or Endpoint 拉取</td><td>多 server 模式</td><td>随机起点轮询所有 server，每个 server 重试 1 次</td></tr>
 * </table>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>NamingServerListManager</b> —— 提供 server 地址列表（getServerList）、
 *       域名模式判断（isDomain/getNacosDomain）</li>
 *   <li><b>SecurityProxy</b> —— 通过父类 {@link AbstractNamingClientProxy#getSecurityHeaders
 *       getSecurityHeaders()} 获取 accessToken；403 时通过 {@link #reLogin()} 触发强制刷新</li>
 *   <li><b>NamingHttpClientManager</b>（单例）—— 共享的 NacosRestTemplate，统一 HTTP 连接池</li>
 *   <li><b>MetricsMonitor</b> —— 记录 HTTP 请求耗时和响应码（client metrics）</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：
 *     <ul>
 *       <li>NamingClientProxyDelegate 构造器 L134-135 —— Naming 主模块（通过 getExecuteClientProxy 路由）</li>
 *       <li>NacosNamingMaintainService.init() L89-90 —— 旧的 Naming 运维服务（已废弃）</li>
 *     </ul>
 *   </li>
 *   <li><b>关闭者</b>：
 *     <ul>
 *       <li>NamingClientProxyDelegate.shutdown() L504 —— 第 3 步关闭</li>
 *       <li>NacosNamingMaintainService.shutDown() L209</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * @author nkorange
 */
@Deprecated
public class NamingHttpClientProxy extends AbstractNamingClientProxy {
    
    /**
     * HTTP 客户端模板 —— 由 NamingHttpClientManager 单例提供，统一连接池管理。
     * <p>超时配置：连接超时 3s（CON_TIME_OUT_MILLIS）、读取超时 50s（READ_TIME_OUT_MILLIS）。</p>
     */
    private final NacosRestTemplate nacosRestTemplate =
        NamingHttpClientManager.getInstance().getNacosRestTemplate();
    
    /** 默认服务端端口 8848。 */
    private static final int DEFAULT_SERVER_PORT = 8848;
    
    /** HTTP 请求头中的模块名，固定为 "Naming"。 */
    private static final String MODULE_NAME = "Naming";
    
    private static final String IP_PARAM = "ip";
    
    private static final String PORT_PARAM = "port";
    
    private static final String WEIGHT_PARAM = "weight";
    
    private static final String ENABLE_PARAM = "enabled";
    
    private static final String EPHEMERAL_PARAM = "ephemeral";
    
    private static final String META_PARAM = "metadata";
    
    private static final String SELECTOR_PARAM = "selector";
    
    private static final String HEALTHY_PARAM = "healthy";
    
    private static final String PROTECT_THRESHOLD_PARAM = "protectThreshold";
    
    private static final String REGISTER_ENABLE_PARAM = "enable";
    
    /** 命名空间 ID。 */
    private final String namespaceId;
    
    /** 服务端地址管理器 —— 提供 server 列表 + 域名模式判断。 */
    private final NamingServerListManager serverListManager;
    
    /** 域名模式下最大重试次数（默认 3，见 UtilAndComs.REQUEST_DOMAIN_RETRY_COUNT）。 */
    private final int maxRetry;
    
    /** 服务端端口，默认 8848，可通过 -Dcom.alibaba.nacos.client.naming.server.port 覆盖。 */
    private int serverPort = DEFAULT_SERVER_PORT;
    
    /** 是否上报客户端指标（监控 HTTP 请求耗时），默认 true。 */
    private boolean enableClientMetrics = true;
    
    /**
     * 构造 Naming HTTP 代理。
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>NamingClientProxyDelegate 构造器 L134-135</li>
     *   <li>NacosNamingMaintainService.init() L89-90</li>
     * </ul>
     *
     * @param namespaceId       命名空间 ID
     * @param securityProxy     安全代理（通过父类持有，用于获取 accessToken 和 403 重登）
     * @param serverListManager 服务端地址管理器（用于获取 server 列表和域名判断）
     * @param properties        配置属性（用于读取 maxRetry 和 client metrics 开关）
     */
    // TODO: Remove this deprecated HTTP naming proxy after naming client fully relies on gRPC.
    public NamingHttpClientProxy(String namespaceId, SecurityProxy securityProxy,
        NamingServerListManager serverListManager,
        NacosClientProperties properties) {
        super(securityProxy);
        this.serverListManager = serverListManager;
        this.setServerPort(DEFAULT_SERVER_PORT);
        this.namespaceId = namespaceId;
        this.maxRetry = ConvertUtils
            .toInt(properties.getProperty(PropertyKeyConst.NAMING_REQUEST_DOMAIN_RETRY_COUNT,
                String.valueOf(UtilAndComs.REQUEST_DOMAIN_RETRY_COUNT)));
        this.enableClientMetrics = Boolean.parseBoolean(
            properties.getProperty(PropertyKeyConst.ENABLE_CLIENT_METRICS, "true"));
    }
    
    @Override
    public void onEvent(ServerListChangeEvent event) {
        // do nothing in http client
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return ServerListChangeEvent.class;
    }
    
    /**
     * 注册持久实例 —— 通过 HTTP POST 向服务端注册。
     *
     * <p>调用方：NamingClientProxyDelegate.getExecuteClientProxy() L476-482，
     * 仅当实例是持久实例且服务端不支持 gRPC 持久化时走此路径。</p>
     *
     * <p>临时实例（ephemeral=true）调用此处会直接抛出 UnsupportedOperationException，
     * 因为临时实例必须走 gRPC（有心跳保活需求）。</p>
     *
     * @param serviceName 服务名
     * @param groupName   组名
     * @param instance    实例（必须 isEphemeral()=false，否则抛异常）
     * @throws UnsupportedOperationException 临时实例调用时
     */
    @Override
    public void registerService(String serviceName, String groupName, Instance instance)
        throws NacosException {
        NAMING_LOGGER.info("[REGISTER-SERVICE] {} registering service {} with instance: {}",
            namespaceId, serviceName,
            instance);
        String groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
        if (instance.isEphemeral()) {
            throw new UnsupportedOperationException(
                "Do not support register ephemeral instances by HTTP, please use gRPC replaced.");
        }
        final Map<String, String> params = new HashMap<>(32);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, groupedServiceName);
        params.put(CommonParams.GROUP_NAME, groupName);
        params.put(CommonParams.CLUSTER_NAME, instance.getClusterName());
        params.put(IP_PARAM, instance.getIp());
        params.put(PORT_PARAM, String.valueOf(instance.getPort()));
        params.put(WEIGHT_PARAM, String.valueOf(instance.getWeight()));
        params.put(REGISTER_ENABLE_PARAM, String.valueOf(instance.isEnabled()));
        params.put(HEALTHY_PARAM, String.valueOf(instance.isHealthy()));
        params.put(EPHEMERAL_PARAM, String.valueOf(instance.isEphemeral()));
        params.put(META_PARAM, JsonUtils.toJson(instance.getMetadata()));
        reqApi(UtilAndComs.nacosUrlInstance, params, HttpMethod.POST);
    }
    
    @Override
    public void batchRegisterService(String serviceName, String groupName,
        List<Instance> instances) {
        throw new UnsupportedOperationException(
            "Do not support persistent instances to perform batch registration methods.");
    }
    
    @Override
    public void batchDeregisterService(String serviceName, String groupName,
        List<Instance> instances) {
        throw new UnsupportedOperationException(
            "Do not support persistent instances to perform batch de registration methods.");
    }
    
    /**
     * 注销持久实例 —— 通过 HTTP DELETE 向服务端注销。
     *
     * <p>调用方：NamingClientProxyDelegate.getExecuteClientProxy() L476-482。
     * 临时实例（isEphemeral()=true）调用时直接 return，不发请求。这是 <b>静默忽略</b>
     * 而非抛异常，与 registerService 的处理方式不同。</p>
     *
     * @param serviceName 服务名
     * @param groupName   组名
     * @param instance    实例（临时实例时静默返回不发送请求）
     */
    @Override
    public void deregisterService(String serviceName, String groupName, Instance instance)
        throws NacosException {
        NAMING_LOGGER.info("[DEREGISTER-SERVICE] {} deregistering service {} with instance: {}",
            namespaceId,
            serviceName, instance);
        if (instance.isEphemeral()) {
            return;
        }
        final Map<String, String> params = new HashMap<>(16);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, NamingUtils.getGroupedName(serviceName, groupName));
        params.put(CommonParams.CLUSTER_NAME, instance.getClusterName());
        params.put(IP_PARAM, instance.getIp());
        params.put(PORT_PARAM, String.valueOf(instance.getPort()));
        params.put(EPHEMERAL_PARAM, String.valueOf(instance.isEphemeral()));
        
        reqApi(UtilAndComs.nacosUrlInstance, params, HttpMethod.DELETE);
    }
    
    @Override
    public void updateInstance(String serviceName, String groupName, Instance instance)
        throws NacosException {
        NAMING_LOGGER.info("[UPDATE-SERVICE] {} update service {} with instance: {}", namespaceId,
            serviceName,
            instance);
        
        final Map<String, String> params = new HashMap<>(32);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put(CommonParams.GROUP_NAME, groupName);
        params.put(CommonParams.CLUSTER_NAME, instance.getClusterName());
        params.put(IP_PARAM, instance.getIp());
        params.put(PORT_PARAM, String.valueOf(instance.getPort()));
        params.put(WEIGHT_PARAM, String.valueOf(instance.getWeight()));
        params.put(ENABLE_PARAM, String.valueOf(instance.isEnabled()));
        params.put(EPHEMERAL_PARAM, String.valueOf(instance.isEphemeral()));
        params.put(META_PARAM, JsonUtils.toJson(instance.getMetadata()));
        
        reqApi(UtilAndComs.nacosUrlInstance, params, HttpMethod.PUT);
    }
    
    @Override
    public ServiceInfo queryInstancesOfService(String serviceName, String groupName,
        String clusters,
        boolean healthyOnly) {
        throw new UnsupportedOperationException(
            "Do not support query instance by http client,please use gRPC replaced.");
    }
    
    @Override
    public Service queryService(String serviceName, String groupName) throws NacosException {
        NAMING_LOGGER.info("[QUERY-SERVICE] {} query service : {}, {}", namespaceId, serviceName,
            groupName);
        
        final Map<String, String> params = new HashMap<>(16);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put(CommonParams.GROUP_NAME, groupName);
        
        String result = reqApi(UtilAndComs.nacosUrlService, params, HttpMethod.GET);
        return JsonUtils.toObj(result, Service.class);
    }
    
    @Override
    public void createService(Service service, AbstractSelector selector) throws NacosException {
        
        NAMING_LOGGER.info("[CREATE-SERVICE] {} creating service : {}", namespaceId, service);
        
        final Map<String, String> params = new HashMap<>(16);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, service.getName());
        params.put(CommonParams.GROUP_NAME, service.getGroupName());
        params.put(PROTECT_THRESHOLD_PARAM, String.valueOf(service.getProtectThreshold()));
        params.put(META_PARAM, JsonUtils.toJson(service.getMetadata()));
        params.put(SELECTOR_PARAM, JsonUtils.toJson(selector));
        
        reqApi(UtilAndComs.nacosUrlService, params, HttpMethod.POST);
        
    }
    
    @Override
    public boolean deleteService(String serviceName, String groupName) throws NacosException {
        NAMING_LOGGER.info("[DELETE-SERVICE] {} deleting service : {} with groupName : {}",
            namespaceId, serviceName,
            groupName);
        
        final Map<String, String> params = new HashMap<>(16);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, serviceName);
        params.put(CommonParams.GROUP_NAME, groupName);
        
        String result = reqApi(UtilAndComs.nacosUrlService, params, HttpMethod.DELETE);
        return "ok".equals(result);
    }
    
    @Override
    public void updateService(Service service, AbstractSelector selector) throws NacosException {
        NAMING_LOGGER.info("[UPDATE-SERVICE] {} updating service : {}", namespaceId, service);
        
        final Map<String, String> params = new HashMap<>(16);
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.SERVICE_NAME, service.getName());
        params.put(CommonParams.GROUP_NAME, service.getGroupName());
        params.put(PROTECT_THRESHOLD_PARAM, String.valueOf(service.getProtectThreshold()));
        params.put(META_PARAM, JsonUtils.toJson(service.getMetadata()));
        params.put(SELECTOR_PARAM, JsonUtils.toJson(selector));
        
        reqApi(UtilAndComs.nacosUrlService, params, HttpMethod.PUT);
    }
    
    /**
     * 检查服务端健康状态 —— 请求 {@code /v3/admin/core/state/liveness} 端点。
     *
     * <p>调用方：ServiceInfoUpdateService，用于判断服务端是否可用，
     * 不可用时不触发定时拉取。</p>
     *
     * @return true 表示服务端正常（返回 code=0），false 表示不可达或异常
     */
    @Override
    public boolean serverHealthy() {
        try {
            String result = reqApi(UtilAndComs.webContext + "/v3/admin/core/state/liveness",
                new HashMap<>(8), HttpMethod.GET);
            Map<String, Object> json =
                JsonUtils.toObj(result, new NacosTypeReference<Map<String, Object>>() {
                });
            Object statusCode = json.get("code");
            return statusCode instanceof Number && 0 == ((Number) statusCode).intValue();
        } catch (Exception e) {
            return false;
        }
    }
    
    @Override
    public ListView<String> getServiceList(int pageNo, int pageSize, String groupName,
        AbstractSelector selector)
        throws NacosException {
        
        Map<String, String> params = new HashMap<>(16);
        params.put("pageNo", String.valueOf(pageNo));
        params.put("pageSize", String.valueOf(pageSize));
        params.put(CommonParams.NAMESPACE_ID, namespaceId);
        params.put(CommonParams.GROUP_NAME, groupName);
        
        if (selector != null) {
            switch (SelectorType.valueOf(selector.getType())) {
                case none:
                    break;
                case label:
                    ExpressionSelector expressionSelector = (ExpressionSelector) selector;
                    params.put(SELECTOR_PARAM, JsonUtils.toJson(expressionSelector));
                    break;
                default:
                    break;
            }
        }
        
        String result = reqApi(UtilAndComs.nacosUrlBase + "/service/list", params, HttpMethod.GET);
        
        Map<String, Object> json =
            JsonUtils.toObj(result, new NacosTypeReference<Map<String, Object>>() {
            });
        ListView<String> listView = new ListView<>();
        Object count = json.get("count");
        listView.setCount(count instanceof Number ? ((Number) count).intValue() : 0);
        listView.setData(JsonUtils.toObj(JsonUtils.toJson(json.get("doms")),
            new NacosTypeReference<List<String>>() {
            }));
        
        return listView;
    }
    
    @Override
    public ServiceInfo subscribe(String serviceName, String groupName, String clusters)
        throws NacosException {
        throw new UnsupportedOperationException(
            "Do not support subscribe service by UDP, please use gRPC replaced.");
    }
    
    @Override
    public void unsubscribe(String serviceName, String groupName, String clusters)
        throws NacosException {
    }
    
    @Override
    public boolean isSubscribed(String serviceName, String groupName, String clusters)
        throws NacosException {
        return true;
    }
    
    public String reqApi(String api, Map<String, String> params, String method)
        throws NacosException {
        return reqApi(api, params, Collections.EMPTY_MAP, method);
    }
    
    public String reqApi(String api, Map<String, String> params, Map<String, String> body,
        String method)
        throws NacosException {
        return reqApi(api, params, body, serverListManager.getServerList(), method);
    }
    
    /**
     * HTTP 请求核心方法 —— 根据域名模式/多 server 模式选择不同重试策略。
     *
     * <h3>两种重试策略</h3>
     * <table>
     *   <tr><th>模式</th><th>条件</th><th>重试方式</th><th>重试次数</th></tr>
     *   <tr><td><b>域名模式</b></td><td>serverListManager.isDomain()=true</td>
     *        <td>对同一域名重试</td><td>{@code maxRetry} 次</td></tr>
     *   <tr><td><b>多 server 模式</b></td><td>serverListManager.isDomain()=false</td>
     *        <td>随机起点轮询所有 server</td><td>{@code servers.size()} 次</td></tr>
     * </table>
     *
     * <p>调用方：所有 registerService/deregisterService/createService/deleteService 等方法，
     * 最终都汇聚到此处。</p>
     *
     * @param api     API 路径（如 /nacos/v1/ns/instance）
     * @param params  查询参数（自动注入 namespaceId）
     * @param body    请求体（POST/PUT 时使用）
     * @param servers 服务端地址列表（来自 serverListManager.getServerList()）
     * @param method  HTTP 方法（GET/POST/PUT/DELETE）
     * @return HTTP 响应体字符串
     * @throws NacosException 所有 server 都失败时抛出
     */
    public String reqApi(String api, Map<String, String> params, Map<String, String> body,
        List<String> servers,
        String method) throws NacosException {
        
        params.put(CommonParams.NAMESPACE_ID, getNamespaceId());
        
        if (CollectionUtils.isEmpty(servers) && !serverListManager.isDomain()) {
            throw new NacosException(NacosException.INVALID_PARAM, "no server available");
        }
        
        NacosException exception = new NacosException();
        if (serverListManager.isDomain()) {
            String nacosDomain = serverListManager.getNacosDomain();
            for (int i = 0; i < maxRetry; i++) {
                try {
                    return callServer(api, params, body, nacosDomain, method);
                } catch (NacosException e) {
                    exception = e;
                    if (NAMING_LOGGER.isDebugEnabled()) {
                        NAMING_LOGGER.debug("request {} failed.", nacosDomain, e);
                    }
                }
            }
        } else {
            int index = ThreadLocalRandom.current().nextInt(servers.size());
            
            for (int i = 0; i < servers.size(); i++) {
                String server = servers.get(index);
                try {
                    return callServer(api, params, body, server, method);
                } catch (NacosException e) {
                    exception = e;
                    if (NAMING_LOGGER.isDebugEnabled()) {
                        NAMING_LOGGER.debug("request {} failed.", server, e);
                    }
                }
                index = (index + 1) % servers.size();
            }
        }
        
        NAMING_LOGGER.error("request: {} failed, servers: {}, code: {}, msg: {}", api, servers,
            exception.getErrCode(),
            exception.getErrMsg());
        
        throw new NacosException(exception.getErrCode(),
            "failed to req API:" + api + " after all servers(" + servers + ") tried: "
                + exception.getMessage());
        
    }
    
    /**
     * 向单个 server 发送 HTTP 请求 —— 认证注入 + 指标上报 + 403 重登。
     *
     * <h3>执行流程（5 步）</h3>
     * <ol>
     *   <li><b>认证注入</b>：通过父类 {@link AbstractNamingClientProxy#getSecurityHeaders
     *       getSecurityHeaders()} 获取 accessToken 等鉴权 header，合并到请求参数中</li>
     *   <li><b>URL 拼接</b>：处理 http(s) 前缀、自动补端口（默认 8848）、拼接 API 路径</li>
     *   <li><b>HTTP 请求</b>：nacosRestTemplate.exchangeForm() 发送，记录耗时</li>
     *   <li><b>响应处理</b>：
     *     <ul>
     *       <li>200 → 返回响应体</li>
     *       <li>304 → 返回空字符串（Not Modified，数据未变更）</li>
     *       <li>403 → 调用 reLogin() 触发 token 刷新 <b>（关键：同时抛异常，由上层 reqApi 捕获并重试）</b></li>
     *       <li>其他 → 抛 NacosException</li>
     *     </ul>
     *   </li>
     *   <li><b>指标上报</b>：通过 MetricsMonitor 记录请求耗时和 HTTP 状态码</li>
     * </ol>
     *
     * @param api       API 路径
     * @param params    查询参数（已注入 namespaceId 和鉴权 header）
     * @param body      请求体
     * @param curServer 目标 server（可能是域名/IP:端口/完整 URL）
     * @param method    HTTP 方法
     * @return 响应体字符串
     * @throws NacosException 非 200/304 响应时抛出（403 时已触发 reLogin）
     */
    public String callServer(String api, Map<String, String> params, Map<String, String> body,
        String curServer,
        String method) throws NacosException {
        long start = System.currentTimeMillis();
        long end = 0;
        String namespace = params.get(CommonParams.NAMESPACE_ID);
        String group = params.get(CommonParams.GROUP_NAME);
        String serviceName = params.get(CommonParams.SERVICE_NAME);
        // Step 1: 注入鉴权 header（accessToken 等）
        params.putAll(getSecurityHeaders(namespace, group, serviceName));
        Header header = HttpUtils.builderHeader(MODULE_NAME);
        
        // Step 2: 拼接 URL（处理 http(s) 前缀、自动补端口）
        String url;
        if (curServer.startsWith(HTTPS_PREFIX) || curServer.startsWith(HTTP_PREFIX)) {
            url = curServer + api;
        } else {
            if (!InternetAddressUtil.containsPort(curServer)) {
                curServer = curServer + InternetAddressUtil.IP_PORT_SPLITER + serverPort;
            }
            url = NamingHttpClientManager.getInstance().getPrefix() + curServer + api;
        }
        try {
            // Step 3: 发送 HTTP 请求
            HttpRestResult<String> restResult = nacosRestTemplate.exchangeForm(url, header,
                Query.newInstance().initParams(params), body, method, String.class);
            end = System.currentTimeMillis();
            
            // Step 5: 指标上报
            if (enableClientMetrics) {
                try {
                    MetricsMonitor
                        .getNamingRequestMonitor(method, url,
                            String.valueOf(restResult.getCode()))
                        .observe(end - start);
                } catch (Throwable t) {
                    NAMING_LOGGER.error(
                        "Failed to record metrics. Method: {}, URL: {}, HTTP Status Code: {}",
                        method, url, restResult.getCode(), t);
                }
            }
            
            // Step 4: 响应处理
            if (restResult.ok()) {
                return restResult.getData();
            }
            if (HttpStatus.SC_NOT_MODIFIED == restResult.getCode()) {
                return StringUtils.EMPTY;
            }
            
            // 403 → 触发 token 刷新，同时抛异常给上层 reqApi 重试
            if (HttpStatus.SC_FORBIDDEN == restResult.getCode()) {
                reLogin();
            }
            
            throw new NacosException(restResult.getCode(), restResult.getMessage());
        } catch (NacosException e) {
            NAMING_LOGGER.error("[NA] failed to request", e);
            throw e;
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to request", e);
            throw new NacosException(NacosException.SERVER_ERROR, e);
        }
    }
    
    public String getNamespaceId() {
        return namespaceId;
    }
    
    /**
     * 设置服务端端口，并检查系统属性 {@code com.alibaba.nacos.client.naming.server.port}
     * 是否覆盖。系统属性优先于代码设置。
     *
     * @param serverPort 默认端口（代码传入 8848）
     */
    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
        
        String sp = NacosClientProperties.PROTOTYPE
            .getProperty(SystemPropertyKeyConst.NAMING_SERVER_PORT);
        if (StringUtils.isNotBlank(sp)) {
            this.serverPort = Integer.parseInt(sp);
        }
    }
    
    /**
     * 关闭 HTTP 代理 —— 销毁 NamingHttpClientManager 单例的 NacosRestTemplate。
     *
     * <p>调用方：NamingClientProxyDelegate.shutdown() L504（第 3 步关闭）。</p>
     */
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        NamingHttpClientManager.getInstance().shutdown();
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
