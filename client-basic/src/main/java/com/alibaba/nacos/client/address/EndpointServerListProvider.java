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

package com.alibaba.nacos.client.address;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.constant.Constants.Address;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.utils.ClientBasicParamUtil;
import com.alibaba.nacos.client.utils.ContextPathUtil;
import com.alibaba.nacos.client.utils.TemplateUtils;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.http.HttpRestResult;
import com.alibaba.nacos.common.http.HttpUtils;
import com.alibaba.nacos.common.http.client.NacosRestTemplate;
import com.alibaba.nacos.common.http.param.Query;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.InternetAddressUtil;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Endpoint（地址服务器）的 ServerListProvider —— 通过 HTTP 从远程端点周期性拉取
 * Nacos Server 地址列表，并在列表变化时发布 {@link ServerListChangeEvent}。
 *
 * <h2>核心职责</h2>
 * <p>实现 {@link ServerListProvider} SPI 接口，为客户端提供动态的 server 地址列表。
 * 与 PropertiesListProvider（配置文件静态地址）不同，本类从外部 HTTP Endpoint
 * 动态获取地址，适用于大规模部署场景下的集中化地址管理。</p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   AbstractServerListManager.start()                          ← 调用方触发生命周期
 *     → NacosServiceLoader.load(ServerListProvider.class)      ← SPI 加载所有实现
 *       → 按 getOrder() 降序排序，遍历调用 match(properties)    ← 匹配激活条件
 *         → match() 检查 endpoint 参数是否存在                  ← 匹配：properties 中有 endpoint
 *           → init(properties, nacosRestTemplate)               ← 初始化 7 个参数 + 启动定时任务
 *             ├─ initEndpoint/Port/ContextPath/ServerListName   ← 从 properties 读参数
 *             ├─ initAddressServerUrl                           ← 拼接 HTTP URL
 *             └─ startRefreshServerListTask                     ← 启动定时拉取
 *                 ├─ 初始化阶段：重试 5 次（100ms 指数退避）      ← 确保首次能拿到列表
 *                 └─ 运行阶段：定时 refreshServerListIfNeed()
 *                     ├─ 防抖检查（30 秒内不重复刷新）            ← refreshServerListInternal
 *                     ├─ HTTP GET → addressServerUrl            ← 获取原始地址文本
 *                     ├─ 解析每行为 ip:port                     ← 缺端口时补 8848
 *                     ├─ 排序 + 与当前列表比较                   ← 判断是否变更
 *                     └─ 变更时 → serversFromEndpoint = 新列表
 *                                → NotifyCenter.publishEvent(ServerListChangeEvent)
 *                                   ├─ SecurityProxy.onEvent     ← 刷新认证插件地址
 *                                   ├─ NamingGrpcClientProxy.onEvent ← RpcClient.onServerListChange()
 *                                   └─ NamingHttpClientProxy.onEvent ← 空实现（仅注册存在）
 * }</pre>
 *
 * <h2>SPI 匹配与加载</h2>
 * <p>启动时，AbstractServerListManager.start() 通过 NacosServiceLoader 加载所有
 * ServerListProvider SPI 实现，按 getOrder() 降序排序后逐一调用 match()。
 * match() 返回 true 后，立即调用 init() 完成初始化，不再尝试其余 provider。</p>
 *
 * <p>内置 provider 优先级（order 越大越优先）：</p>
 * <ul>
 *   <li>PropertiesListProvider — match: serverAddr 已配置</li>
 *   <li>EndpointServerListProvider — match: endpoint 已配置</li>
 * </ul>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>AbstractServerListProvider</b>（父类）—— 提供 contextPath/namespace 初始化，
 *       声明 ServerListProvider 接口但大部分方法留 abstract</li>
 *   <li><b>AbstractServerListManager</b> —— SPI 加载、调用 init()/match()/shutdown()、
 *       通过 getServerList() 向上层暴露地址列表</li>
 *   <li><b>NacosRestTemplate</b> —— 用于 HTTP GET 请求 endpoint 获取地址文本</li>
 *   <li><b>ServerListChangeEvent</b> —— 列表变更时通过 NotifyCenter 发布，
 *       下游有 SecurityProxy（刷新认证）、NamingGrpcClientProxy（gRPC 重连）等监听</li>
 *   <li><b>NotifyCenter</b> —— 发布/订阅事件的中枢</li>
 * </ul>
 *
 * <h2>关键参数</h2>
 * <table>
 *   <tr><th>参数名</th><th>说明</th><th>默认值</th></tr>
 *   <tr><td>endpoint</td><td>地址服务器的 IP 或域名</td><td>无（必填，否则不匹配）</td></tr>
 *   <tr><td>endpointPort</td><td>地址服务器端口</td><td>8080</td></tr>
 *   <tr><td>endpointContextPath</td><td>地址服务器上下文路径</td><td>继承 contextPath</td></tr>
 *   <tr><td>endpointClusterName</td><td>地址服务器上的集群名（URI 最后一段）</td><td>serverlist</td></tr>
 *   <tr><td>refreshInterval</td><td>定时刷新间隔（秒）</td><td>30</td></tr>
 *   <tr><td>namespace</td><td>命名空间（作为 query param）</td><td>空</td></tr>
 * </table>
 *
 * <p>最终请求 URL 格式：
 * {@code http://{endpoint}:{endpointPort}{contextPath}/{serverListName}?namespace={namespace}}</p>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：AbstractServerListManager.start() L95 —— SPI 实例化
 *     （测试代码中直接 new EndpointServerListProvider()）</li>
 *   <li><b>初始化者</b>：AbstractServerListManager.start() L106 ——
 *     调用 serverListProvider.init(properties, getNacosRestTemplate())</li>
 *   <li><b>调用方</b>（通过 AbstractServerListManager.getServerList() 间接调用）：
 *     NamingHttpClientProxy.reqApi() L364、SecurityProxy L60/L65、RpcClient 等</li>
 *   <li><b>关闭者</b>：AbstractServerListManager.shutdown() L73 ——
 *     调用 serverListProvider.shutdown() → 停止定时刷新线程池</li>
 * </ul>
 *
 * @author totalo
 */
public class EndpointServerListProvider extends AbstractServerListProvider {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(EndpointServerListProvider.class);
    
    /**
     * Endpoint 解析规则的默认值 —— 默认 true，表示启用 endpoint 地址解析规则。
     * <p>解析规则由 ClientBasicParamUtil.parsingEndpointRule() 提供，用于从复杂的
     * endpoint 字符串中提取纯地址。</p>
     */
    private static final boolean USE_ENDPOINT_PARSING_RULE_DEFAULT_VALUE = true;
    
    /**
     * HTTP 客户端 —— 由父类 AbstractServerListManager 传入，用于向 endpoint 发起 HTTP GET 请求。
     * <p>Naming 模块使用 NamingHttpClientManager 提供的 NacosRestTemplate 实例。</p>
     */
    private NacosRestTemplate nacosRestTemplate;
    
    private static final String CUSTOM_NAME = "custom";
    
    /**
     * 地址列表两次刷新之间的最小间隔（30 秒）。
     * <p>目的：防止频繁 HTTP 请求对 endpoint 造成压力。即使定时任务触发频率更高，
     * 也会在此间隔内被跳过。</p>
     */
    private final long refreshServerListInternal = TimeUnit.SECONDS.toMillis(30);
    
    /**
     * 初始化阶段获取 server list 的最大重试次数（5 次）。
     * <p>每次重试间的等待时间按 (retryCount + 1) * 100ms 指数增长。</p>
     */
    private final int initServerListRetryTimes = 5;
    
    /**
     * 上一次成功刷新的时间戳 —— 用于 refreshServerListIfNeed() 中的 30 秒防抖判断。
     * <p>初始值 0 表示首次刷新一定可以通过防抖检查。</p>
     */
    private long lastServerListRefreshTime = 0L;
    
    /**
     * 定时刷新线程池 —— 单线程 ScheduledThreadPoolExecutor，
     * 周期性执行 refreshServerListIfNeed()。
     */
    private ScheduledExecutorService refreshServerListExecutor;
    
    /**
     * 地址服务器的主机名或 IP —— 从 properties 的 endpoint（或系统变量 endpoint 解析）获取。
     * <p>注意：match() 要求此值非空，否则不会匹配到此 provider。</p>
     */
    private String endpoint;
    
    /**
     * 地址服务器的端口号 —— 默认 8080，可通过 endpointPort 参数覆盖。
     */
    private int endpointPort = 8080;
    
    /**
     * 地址服务器的上下文路径 —— 优先 endpointContextPath，否则继承 contextPath。
     * <p>在 initAddressServerUrl() 中参与拼接最终的 HTTP URL。</p>
     */
    private String endpointContextPath;
    
    /**
     * 地址服务器上的节点列表名称 —— 作为 HTTP URL 的最后一段路径。
     * <p>默认值来自 ClientBasicParamUtil.getDefaultNodesPath()（通常是 "serverlist"），
     * 可通过 endpointClusterName（或旧版 clusterName）覆盖。</p>
     */
    private String serverListName = ClientBasicParamUtil.getDefaultNodesPath();
    
    /**
     * 从 endpoint 获取到的当前 server 地址列表 —— volatile 保证多线程可见性。
     * <p>getServerList() 直接返回此列表的引用，不做防御性拷贝，
     * 因此调用方应只读使用。</p>
     */
    private volatile List<String> serversFromEndpoint = new ArrayList<>();
    
    /**
     * 最终拼接的 endpoint HTTP 请求 URL。
     * <p>格式：{@code http://{endpoint}:{port}{contextPath}/{serverListName}?namespace=xxx}</p>
     */
    private String addressServerUrl;
    
    /**
     * 模块名 —— 从 CLIENT_MODULE_TYPE 属性获取（由 AbstractServerListManager 构造器设置）。
     * <p>用于 HttpUtils.builderHeader(moduleName)，区分不同模块的 HTTP 请求。
     * 默认值 "default" 在 initModuleName() 中被覆盖为 "Naming"/"Config" 等。</p>
     */
    private String moduleName = "default";
    
    /**
     * 初始化所有参数并启动定时刷新任务 —— 被 AbstractServerListManager.start() 调用。
     *
     * <p>调用方：AbstractServerListManager.start() L106 —— SPI 匹配成功后立即调用。</p>
     *
     * <h3>初始化顺序（7 步）</h3>
     * <ol>
     *   <li>父类 init → 读 contextPath + namespace</li>
     *   <li>读 endpoint（与 match() 逻辑一致）</li>
     *   <li>读 endpointPort（默认 8080）</li>
     *   <li>读 endpointContextPath（优先专用参数，否则继承 contextPath）</li>
     *   <li>读 serverListName（endpointClusterName 或 clusterName 或默认 "serverlist"）</li>
     *   <li>拼接 addressServerUrl</li>
     *   <li>读 moduleName（覆盖默认值 "default"）</li>
     *   <li>启动定时刷新任务 → 先同步重试 5 次获取列表，再启动定时线程池</li>
     * </ol>
     *
     * @param properties        Nacos 客户端配置
     * @param nacosRestTemplate HTTP 客户端（由各模块 getNacosRestTemplate() 提供）
     * @throws NacosException 初始化重试 5 次后仍未获取到 server list 时抛出
     */
    @Override
    public void init(final NacosClientProperties properties,
        final NacosRestTemplate nacosRestTemplate)
        throws NacosException {
        // Step 1: 父类初始化 → contextPath + namespace
        super.init(properties, nacosRestTemplate);
        this.nacosRestTemplate = nacosRestTemplate;
        // Steps 2-6: 逐步初始化 endpoint 相关参数
        initEndpoint(properties);
        initEndpointPort(properties);
        initEndpointContextPath(properties);
        initServerListName(properties);
        initAddressServerUrl(properties);
        initModuleName(properties);
        // Step 7: 同步获取初始列表 + 启动定时刷新
        startRefreshServerListTask(properties);
    }
    
    /**
     * 获取当前缓存的 server 地址列表 —— 直接返回 volatile 引用的列表。
     *
     * <p>调用方（间接）：通过 AbstractServerListManager.getServerList() L64-66 被广泛调用：</p>
     * <ul>
     *   <li>NamingHttpClientProxy.reqApi() L364 —— 获取全量地址用于 HTTP 请求轮询</li>
     *   <li>SecurityProxy 构造器 L60 + ServerListChangeEvent listener L65 ——
     *       初始化认证插件 + 服务端地址变更时刷新</li>
     *   <li>RpcClient（通过 ServerListFactory.getServerList()）—— gRPC 连接管理</li>
     *   <li>NamingServerListManager（继承的 getServerList()）—— 所有地址管理操作</li>
     * </ul>
     *
     * <p>注意：返回的是内部列表引用，调用方应只读使用，不应修改。</p>
     *
     * @return 从 endpoint 拉取并解析的 server 地址列表（不可变视图）
     */
    @Override
    public List<String> getServerList() {
        return serversFromEndpoint;
    }
    
    /**
     * 生成唯一的 server 名称 —— 用于标识当前 provider 实例。
     *
     * <p>调用方：AbstractServerListManager.getServerName() L109-111 ——
     * 拼接为 "{moduleName}-{serverName}" 用于日志和诊断。</p>
     *
     * <p>格式：{@code custom-{endpoint}_{port}_{contextPath}_{serverListName}_{namespace}}，
     * 各部分以下划线连接，namespace 为空时不拼接。</p>
     *
     * @return 唯一标识的 server 名称字符串
     */
    @Override
    public String getServerName() {
        String contextPathTmp =
            StringUtils.isNotBlank(this.endpointContextPath) ? this.endpointContextPath
                : this.contextPath;
        return CUSTOM_NAME + "-"
            + String.join("_", endpoint, String.valueOf(endpointPort), contextPathTmp,
                serverListName)
            + (StringUtils.isNotBlank(namespace) ? ("_" + StringUtils.trim(namespace)) : "");
    }
    
    /**
     * 获取 SPI 优先级 —— 值越大越优先被匹配。
     *
     * <p>由 AbstractServerListManager.start() L88 按降序排序后遍历调用 match()。
     * 当前实现中，PropertiesListProvider 的 order 更高（优先匹配 serverAddr），
     * EndpointServerListProvider 次之。</p>
     *
     * @return 优先级值，来自 Address.ENDPOINT_SERVER_LIST_PROVIDER_ORDER
     */
    @Override
    public int getOrder() {
        return Address.ENDPOINT_SERVER_LIST_PROVIDER_ORDER;
    }
    
    /**
     * 匹配激活条件 —— 检查 endpoint 参数是否已配置。
     *
     * <p>调用方：AbstractServerListManager.start() L90 —— SPI 遍历中逐一调用，
     * 第一个返回 true 的 provider 被选中并调用 init()。</p>
     *
     * <p>匹配逻辑：</p>
     * <ol>
     *   <li>从 properties 读取 endpoint 参数</li>
     *   <li>如果启用 endpoint 解析规则（默认 true），调用 ClientBasicParamUtil.parsingEndpointRule()
     *       对 endpoint 进行规则解析</li>
     *   <li>返回 endpoint 是否非空白字符串</li>
     * </ol>
     *
     * <p>endpoint 参数来源（优先级从高到低）：</p>
     * <ul>
     *   <li>系统属性 ALIBABA_ALIWARE_ENDPOINT_URL（环境变量）</li>
     *   <li>properties 中的 endpoint 配置项</li>
     * </ul>
     *
     * @param properties Nacos 客户端配置
     * @return true 表示配置了 endpoint，应使用本 provider
     */
    @Override
    public boolean match(final NacosClientProperties properties) {
        String endpointTmp = getEndPointTmp(properties);
        return StringUtils.isNotBlank(endpointTmp);
    }
    
    /**
     * 获取地址来源 URL —— 用于诊断和监控。
     *
     * <p>调用方：AbstractServerListManager.getAddressSource() L121-123；
     * ClientWorker（Config 模块）L611 将其写入 metrics。</p>
     *
     * @return 当前 endpoint 请求的完整 URL
     */
    @Override
    public String getAddressSource() {
        return this.addressServerUrl;
    }
    
    /**
     * 获取解析后的 endpoint 地址 —— 供 match() 和 init() 使用。
     *
     * <p>处理流程：</p>
     * <ol>
     *   <li>从 properties 读取 endpoint 参数（支持系统属性和 properties 双来源）</li>
     *   <li>读取 IS_USE_ENDPOINT_PARSING_RULE 标志（默认 true）</li>
     *   <li>如果启用规则解析，调用 ClientBasicParamUtil.parsingEndpointRule()
     *       从复杂 endpoint 字符串中提取纯地址</li>
     * </ol>
     *
     * <p>解析规则示例：若 endpoint 配置为 "http://nacos.example.com:8080/xx"，
     * parsingEndpointRule() 会提取为 "nacos.example.com"。</p>
     *
     * @param properties Nacos 客户端配置
     * @return 解析后的纯 endpoint 地址
     */
    private String getEndPointTmp(NacosClientProperties properties) {
        String endpointTmp = properties.getProperty(PropertyKeyConst.ENDPOINT);
        String isUseEndpointRuleParsing =
            properties.getProperty(PropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                properties.getProperty(SystemPropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                    String.valueOf(USE_ENDPOINT_PARSING_RULE_DEFAULT_VALUE)));
        if (Boolean.parseBoolean(isUseEndpointRuleParsing)) {
            endpointTmp = ClientBasicParamUtil.parsingEndpointRule(endpointTmp);
        }
        return endpointTmp;
    }
    
    /**
     * 启动定时刷新任务 —— 先同步获取初始列表，再启动后台定时线程。
     *
     * <p>调用方：init() L97 —— 初始化流程的最后一步。</p>
     *
     * <h3>执行流程（两阶段）</h3>
     * <ol>
     *   <li><b>初始化阶段</b>：最多重试 5 次（initServerListRetryTimes），每次调用
     *       refreshServerListIfNeed()，如果仍未获取到列表，等待 (i+1)*100ms 后重试。
     *       <p>5 次都失败后抛出 NacosException，阻止客户端继续启动（因为没有地址无法通信）。</p></li>
     *   <li><b>运行阶段</b>：创建单线程 ScheduledThreadPoolExecutor，
     *       以 scheduleWithFixedDelay 方式周期性执行 refreshServerListIfNeed()。
     *       刷新间隔由 ENDPOINT_REFRESH_INTERVAL_SECONDS 参数控制（默认 30 秒）。</li>
     * </ol>
     *
     * <p>注意：初始化阶段使用 {@code this.wait()} 进行同步等待（非异步），
     * 确保 start() 返回时 server list 已经可用。</p>
     *
     * @param properties Nacos 客户端配置
     * @throws NacosException 初始化重试 5 次后仍未获取到 server list 时抛出
     */
    public void startRefreshServerListTask(NacosClientProperties properties) throws NacosException {
        // ---- Phase 1: 同步初始化 ----
        // 重试最多 5 次，每次间隔 100ms * (i+1)，确保首次启动能拿到地址列表
        for (int i = 0; i < initServerListRetryTimes && getServerList().isEmpty(); ++i) {
            refreshServerListIfNeed();
            if (!serversFromEndpoint.isEmpty()) {
                break;
            }
            try {
                this.wait((i + 1) * 100L);
            } catch (Exception e) {
                LOGGER.warn("get serverlist fail,url: {}", addressServerUrl);
            }
        }
        
        // 5 次重试后仍为空 → 客户端无法继续运行
        if (serversFromEndpoint.isEmpty()) {
            LOGGER.error("[init-serverlist] fail to get NACOS-server serverlist! url: {}",
                addressServerUrl);
            throw new NacosException(NacosException.SERVER_ERROR,
                "fail to get NACOS-server serverlist! not connnect url:" + addressServerUrl);
        }
        
        // ---- Phase 2: 启动定时刷新 ----
        refreshServerListExecutor = new ScheduledThreadPoolExecutor(1,
            new NameThreadFactory(
                "com.alibaba.nacos.client.address.EndpointServerListProvider.refreshServerList"));
        long refreshInterval = Long.parseLong(
            properties.getProperty(PropertyKeyConst.ENDPOINT_REFRESH_INTERVAL_SECONDS, "30"));
        refreshServerListExecutor.scheduleWithFixedDelay(this::refreshServerListIfNeed, 0L,
            refreshInterval,
            TimeUnit.SECONDS);
    }
    
    /**
     * 条件刷新 server 列表 —— 带 30 秒防抖，仅在列表确实变化时发布事件。
     *
     * <p>由定时线程周期性调用，同时也可被初始化阶段同步调用。</p>
     *
     * <h3>执行流程（5 步）</h3>
     * <ol>
     *   <li><b>防抖检查</b>：距离上次成功刷新不足 30 秒 → 直接返回</li>
     *   <li><b>HTTP 拉取</b>：GET addressServerUrl 获取原始地址文本</li>
     *   <li><b>排序</b>：新列表排序（保证比较的一致性）</li>
     *   <li><b>差异比较</b>：CollectionUtils.isEqualCollection() 忽略顺序比较</li>
     *   <li><b>原子更新 + 发布事件</b>：列表不同 → 替换引用 + 更新时间戳 +
     *       NotifyCenter.publishEvent(new ServerListChangeEvent())</li>
     * </ol>
     *
     * <p>注意：整个方法由 try-catch(Throwable) 包裹，任何异常都不会向上抛出，
     * 保证定时任务不会因单次失败而终止。</p>
     */
    private void refreshServerListIfNeed() {
        try {
            // Step 1: 30 秒防抖 —— 距离上次成功刷新不足 refreshServerListInternal 则跳过
            if (System.currentTimeMillis()
                - lastServerListRefreshTime < refreshServerListInternal) {
                return;
            }
            // Step 2: HTTP GET → 获取原始地址文本
            List<String> list = getServerListFromEndpoint();
            if (CollectionUtils.isEmpty(list)) {
                throw new Exception("Can not acquire Nacos list");
            }
            // Step 3: 排序以保证比较的一致性
            list.sort(String::compareTo);
            // Step 4: 忽略顺序比较集合内容
            if (!CollectionUtils.isEqualCollection(list, serversFromEndpoint)) {
                LOGGER.info("[SERVER-LIST] server list is updated: {}", list);
                // Step 5: 原子替换 + 更新时间戳 + 发布变更事件
                serversFromEndpoint = list;
                lastServerListRefreshTime = System.currentTimeMillis();
                NotifyCenter.publishEvent(new ServerListChangeEvent());
            }
        } catch (Throwable e) {
            LOGGER.warn("failed to update server list", e);
        }
    }
    
    /**
     * 从 endpoint 拉取并解析 server 地址列表 —— 核心网络请求方法。
     *
     * <h3>执行流程</h3>
     * <ol>
     *   <li>HTTP GET → addressServerUrl，获取原始文本（每行一个 server 地址）</li>
     *   <li>逐行解析 ip:port，缺端口时补充默认端口 8848</li>
     *   <li>返回解析后的地址列表</li>
     * </ol>
     *
     * <p>示例响应（纯文本格式）：</p>
     * <pre>
     * 192.168.1.1:8848
     * 192.168.1.2:8848
     * 192.168.1.3
     * </pre>
     * <p>解析结果：["192.168.1.1:8848", "192.168.1.2:8848", "192.168.1.3:8848"]</p>
     *
     * @return 解析后的 server 地址列表，网络异常或响应码非 200 时返回 null
     */
    private List<String> getServerListFromEndpoint() {
        try {
            // Step 1: HTTP GET 请求 endpoint
            HttpRestResult<String> httpResult = nacosRestTemplate.get(addressServerUrl,
                HttpUtils.builderHeader(moduleName), Query.EMPTY, String.class);
            
            if (!httpResult.ok()) {
                LOGGER.error("[check-serverlist] error. addressServerUrl: {}, code: {}",
                    addressServerUrl,
                    httpResult.getCode());
                return null;
            }
            // Step 2: 逐行解析 ip:port
            List<String> lines = IoUtils.readLines(new StringReader(httpResult.getData()));
            List<String> result = new ArrayList<>(lines.size());
            for (String serverAddr : lines) {
                String[] ipPort = InternetAddressUtil.splitIpPortStr(serverAddr);
                String ip = ipPort[0].trim();
                if (ipPort.length == 1) {
                    // 只有 IP 没有端口 → 补充默认端口 8848
                    result.add(ip + InternetAddressUtil.IP_PORT_SPLITER
                        + ClientBasicParamUtil.getDefaultServerPort());
                } else {
                    result.add(serverAddr);
                }
            }
            return result;
        } catch (Exception e) {
            LOGGER.error("[check-serverlist] exception. url: {}", addressServerUrl, e);
            return null;
        }
    }
    
    /**
     * 从 properties 读取 endpoint 地址并设置到成员字段。
     * <p>endpoint 不能为 null 或空——因为 match() 已经确保不为空才会调用 init()。</p>
     */
    private void initEndpoint(NacosClientProperties properties) {
        this.endpoint = getEndPointTmp(properties);
    }
    
    /**
     * 从 properties 读取 endpoint 端口号 —— 默认 8080。
     *
     * <p>读取优先级：</p>
     * <ol>
     *   <li>系统属性 ALIBABA_ALIWARE_ENDPOINT_PORT（环境变量）</li>
     *   <li>properties 中的 endpointPort 配置项</li>
     *   <li>默认值 8080</li>
     * </ol>
     */
    private void initEndpointPort(NacosClientProperties properties) {
        String endpointPortTmp = TemplateUtils.stringEmptyAndThenExecute(
            properties.getProperty(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_ENDPOINT_PORT),
            () -> properties.getProperty(PropertyKeyConst.ENDPOINT_PORT));
        if (StringUtils.isNotBlank(endpointPortTmp)) {
            this.endpointPort = Integer.parseInt(endpointPortTmp);
        }
    }
    
    /**
     * 从 properties 读取 endpoint 上下文路径 —— 如果未配置则继承 contextPath。
     *
     * <p>读取优先级：</p>
     * <ol>
     *   <li>系统属性 ALIBABA_ALIWARE_ENDPOINT_CONTEXT_PATH</li>
     *   <li>properties 中的 endpointContextPath 配置项</li>
     *   <li>不设置时 → initAddressServerUrl() 中使用父类的 contextPath</li>
     * </ol>
     */
    private void initEndpointContextPath(NacosClientProperties properties) {
        String endpointContextPathTmp = TemplateUtils.stringEmptyAndThenExecute(
            properties.getProperty(
                PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_ENDPOINT_CONTEXT_PATH),
            () -> properties.getProperty(PropertyKeyConst.ENDPOINT_CONTEXT_PATH));
        if (StringUtils.isNotBlank(endpointContextPathTmp)) {
            this.endpointContextPath = endpointContextPathTmp;
        }
    }
    
    /**
     * 从 properties 读取 server 列表在 endpoint 上的名称（URI 路径最后一段）。
     *
     * <p>读取优先级：</p>
     * <ol>
     *   <li>endpointClusterName 参数</li>
     *   <li>如果启用 IS_ADAPT_CLUSTER_NAME_USAGE 且 endpointClusterName 为空 →
     *       使用 clusterName 参数（兼容旧版配置）</li>
     *   <li>默认值：ClientBasicParamUtil.getDefaultNodesPath() = "serverlist"</li>
     * </ol>
     */
    private void initServerListName(NacosClientProperties properties) {
        String serverListNameTmp = properties.getProperty(PropertyKeyConst.ENDPOINT_CLUSTER_NAME);
        boolean isUseClusterName = Boolean.parseBoolean(
            properties.getProperty(PropertyKeyConst.IS_ADAPT_CLUSTER_NAME_USAGE));
        if (StringUtils.isBlank(serverListNameTmp) && isUseClusterName) {
            serverListNameTmp = properties.getProperty(PropertyKeyConst.CLUSTER_NAME);
        }
        if (!StringUtils.isBlank(serverListNameTmp)) {
            this.serverListName = serverListNameTmp;
        }
    }
    
    /**
     * 拼接最终的 endpoint HTTP 请求 URL。
     *
     * <p>URL 格式：
     * {@code http://{endpoint}:{endpointPort}{contextPath}/{serverListName}?namespace={namespace}}</p>
     *
     * <p>contextPath 优先级：endpointContextPath > 父类 contextPath。
     * 如果设置了 namespace，追加为 query parameter。
     * 如果配置了 ENDPOINT_QUERY_PARAMS，追加额外的 query parameters。</p>
     */
    private void initAddressServerUrl(NacosClientProperties properties) {
        String contextPathTmp = StringUtils.isNotBlank(this.endpointContextPath)
            ? ContextPathUtil.normalizeContextPath(
                this.endpointContextPath)
            : ContextPathUtil.normalizeContextPath(this.contextPath);
        StringBuilder addressServerUrlTem = new StringBuilder(
            String.format("http://%s:%d%s/%s", this.endpoint, this.endpointPort, contextPathTmp,
                this.serverListName));
        boolean hasQueryString = false;
        if (StringUtils.isNotBlank(namespace)) {
            addressServerUrlTem.append("?namespace=").append(namespace);
            hasQueryString = true;
        }
        if (properties.containsKey(PropertyKeyConst.ENDPOINT_QUERY_PARAMS)) {
            addressServerUrlTem.append(hasQueryString ? "&" : "?");
            addressServerUrlTem
                .append(properties.getProperty(PropertyKeyConst.ENDPOINT_QUERY_PARAMS));
        }
        this.addressServerUrl = addressServerUrlTem.toString();
        LOGGER.info("address server url = {}", this.addressServerUrl);
    }
    
    /**
     * 从 properties 读取模块名（CLIENT_MODULE_TYPE）—— 用于 HTTP 请求的 Header。
     * <p>该属性由 AbstractServerListManager 构造器在 SPI 加载之前写入，
     * 值由 getModuleName() 返回（如 "Naming"、"Config" 等）。
     * 如果某种原因未设置，保留默认值 "default"。</p>
     */
    private void initModuleName(NacosClientProperties properties) {
        String moduleNameTmp = properties.getProperty(Constants.CLIENT_MODULE_TYPE);
        if (StringUtils.isNotBlank(moduleNameTmp)) {
            this.moduleName = moduleNameTmp;
        }
    }
    
    /**
     * 关闭定时刷新线程池 —— 停止周期性地址拉取。
     *
     * <p>调用方：AbstractServerListManager.shutdown() L73 ——
     * 在 AbstractServerListManager 关闭时调用。</p>
     *
     * <p>注意：仅关闭线程池，不主动触发最后一次刷新。
     * 关闭后 serversFromEndpoint 仍可被读取（最后一次缓存的值）。</p>
     */
    @Override
    public void shutdown() throws NacosException {
        if (null != refreshServerListExecutor) {
            refreshServerListExecutor.shutdown();
        }
    }
}
