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

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosLoadException;
import com.alibaba.nacos.client.address.AbstractServerListManager;
import com.alibaba.nacos.client.address.PropertiesListProvider;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientManager;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.http.client.NacosRestTemplate;
import org.slf4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Naming 模块的服务端地址管理器 —— 管理 Nacos Server 地址列表，支持轮询和故障转移。
 *
 * <h2>核心职责</h2>
 * <p>继承 AbstractServerListManager，为其指定模块名（"Naming"）和 HTTP 客户端。
 * 作为 {@link com.alibaba.nacos.common.remote.client.ServerListFactory} 实现，
 * 提供 gRPC 连接层所需的轮询取地址能力（genNextServer / getCurrentServer）。</p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   配置来源（properties / endpoint）→ AbstractServerListManager.start()
 *     → SPI 加载 ServerListProvider（PropertiesListProvider 或 EndpointServerListProvider）
 *       → getServerList() 返回地址列表
 *         ├─→ genNextServer()  → RpcClient 轮询取下一台服务器建连
 *         ├─→ getCurrentServer() → RpcClient 获取当前连接的服务器
 *         ├─→ getServerList() → NamingHttpClientProxy 获取全量地址
 *         ├─→ isDomain() / getNacosDomain() → NamingHttpClientProxy 域名直连模式
 *         └─→ getContextPath() / getAddressSource() / isFixed()
 *              → SecurityProxy / ClientWorker 获取运行时信息
 * }</pre>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>AbstractServerListManager</b>（父类）—— 提供 SPI 加载、地址列表、关闭等通用能力，
 *       同时实现 ServerListFactory + Closeable 接口</li>
 *   <li><b>ServerListProvider</b>（SPI）—— 实际获取地址列表的提供者，
 *       如 PropertiesListProvider（配置文件）、EndpointServerListProvider（Endpoint HTTP 获取）</li>
 *   <li><b>RpcClient</b>（gRPC 底层）—— 通过 ServerListFactory 接口调用 genNextServer/getCurrentServer，
 *       用于 gRPC 连接建立与切换</li>
 *   <li><b>NamingHttpClientProxy</b> —— 通过 getServerList()/isDomain()/getNacosDomain()
 *       获取地址信息，用于 HTTP 请求的重试和路由</li>
 *   <li><b>SecurityProxy</b> —— 构造器接收 AbstractServerListManager，
 *       用 getServerList() 初始化认证插件，并监听 ServerListChangeEvent 刷新 server list</li>
 *   <li><b>NamingGrpcClientProxy</b> —— 构造器接收 ServerListFactory（即本类），
 *       L133 传入 RpcClient.serverListFactory()，后续 gRPC 建连时自动轮询</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：
 *     <ul>
 *       <li>NamingClientProxyDelegate 构造器 L178-179（NacosNamingService.init() 间接触发）</li>
 *       <li>NacosNamingMaintainService.init() L84-85</li>
 *       <li>AiHttpClientProxy 构造器 L103-104</li>
 *       <li>AiGrpcClient 构造器 L130</li>
 *     </ul>
 *   </li>
 *   <li><b>关闭者</b>：
 *     <ul>
 *       <li>NamingClientProxyDelegate.shutdown() L491</li>
 *       <li>NacosNamingMaintainService.shutDown() L208</li>
 *       <li>AiHttpClientProxy.shutdown() L522</li>
 *       <li>AiGrpcClient.shutdown() L752</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * @author xiweng.yy
 */
public class NamingServerListManager extends AbstractServerListManager {
    
    private static final Logger LOGGER = LogUtils.logger(NamingServerListManager.class);
    
    /**
     * 当前轮询索引 —— 原子整数，保证多线程下的 server 选择安全。
     * <p>genNextServer() 对该值递增并取模，实现 Round-Robin 轮询；
     * getCurrentServer() 只读不增。</p>
     */
    private final AtomicInteger currentIndex = new AtomicInteger();
    
    /**
     * Nacos 域名 —— 当 serverListProvider 是 PropertiesListProvider 且仅有一个地址时，
     * 该地址被识别为"域名"而非 IP列表，存储在此字段中。
     * <p>域名模式下，NamingHttpClientProxy.reqApi() 会对同一域名进行多次 retry，而不是轮询多个 server。</p>
     */
    private String nacosDomain;
    
    /**
     * 是否为域名模式 —— true 表示地址是域名而非 IP 列表。
     * <p>NamingHttpClientProxy 根据此标志选择不同的请求重试策略（域名重试 vs 多 server 轮询重试）。</p>
     */
    private boolean isDomain;
    
    /**
     * 测试用构造器 —— 仅接收 Properties，自动包装为 NacosClientProperties。
     * <p>使用方：NamingServerListManagerTest 中的各种测试方法。</p>
     *
     * @param properties JDK Properties 格式的客户端配置
     */
    @JustForTest
    public NamingServerListManager(Properties properties) {
        this(NacosClientProperties.PROTOTYPE.derive(properties), "");
    }
    
    /**
     * 正式构造器 —— 接收 NacosClientProperties 和 namespace，委托父类初始化。
     *
     * <p>父类 AbstractServerListManager 的构造逻辑：</p>
     * <ol>
     *   <li>derive 一份新的 NacosClientProperties（避免影响原始配置）</li>
     *   <li>设置 namespace（如果有）</li>
     *   <li>设置 CLIENT_MODULE_TYPE = getModuleName() = "Naming"</li>
     * </ol>
     *
     * <p>注意：构造器只做配置准备，真正的 SPI 加载在 start() 中完成。</p>
     *
     * @param properties Nacos 客户端配置
     * @param namespace  命名空间（可为空字符串）
     */
    public NamingServerListManager(NacosClientProperties properties, String namespace) {
        super(properties, namespace);
    }
    
    /**
     * 启动地址管理器 —— 调用父类 SPI 加载 + 初始化轮询索引 + 识别域名模式。
     *
     * <h3>执行流程（3 步）</h3>
     * <ol>
     *   <li><b>父类 start()</b>：SPI 加载 ServerListProvider，匹配并初始化。
     *     <p>例如：properties 中有 serverAddr → 匹配 PropertiesListProvider；
     *     有 endpoint → 匹配 EndpointServerListProvider。</p></li>
     *   <li><b>初始化轮询索引</b>：currentIndex 设为 [0, serverList.size()) 内的随机值，
     *     避免所有客户端首次请求都打在同一台 server 上（负载均衡）。</li>
     *   <li><b>识别域名模式</b>：如果 SPI 匹配到的是 PropertiesListProvider（配置直连模式）
     *     且地址列表只有一条，将其标记为"域名"模式。
     *     <p>域名模式下，NamingHttpClientProxy.reqApi() 走域名 retry 策略而非多 server 轮询。</p></li>
     * </ol>
     *
     * <p>调用方（4 个）：</p>
     * <ul>
     *   <li>NamingClientProxyDelegate 构造器 L179 —— Naming 主模块初始化</li>
     *   <li>NacosNamingMaintainService.init() L85 —— Naming 运维模式初始化</li>
     *   <li>AiHttpClientProxy 构造器 L104 —— AI HTTP 代理初始化</li>
     *   <li>AiGrpcClient.start() L155 —— AI gRPC 客户端启动</li>
     * </ul>
     *
     * @throws NacosException 当地址列表为空时抛出 NacosLoadException
     */
    @Override
    public void start() throws NacosException {
        // Step 1: 父类 start() —— SPI 加载 ServerListProvider
        super.start();
        List<String> serverList = getServerList();
        if (serverList.isEmpty()) {
            throw new NacosLoadException("serverList is empty,please check configuration");
        } else {
            // Step 2: 随机初始化轮询索引，避免所有客户端从同一台 server 开始
            currentIndex.set(ThreadLocalRandom.current().nextInt(serverList.size()));
        }
        // Step 3: 域名模式识别（仅 PropertiesListProvider 且单地址时触发）
        if (serverListProvider instanceof PropertiesListProvider) {
            if (serverList.size() == 1) {
                isDomain = true;
                nacosDomain = serverList.get(0);
            }
        }
    }
    
    /**
     * 获取 Nacos 域名 —— 仅在 isDomain()=true 时有值。
     *
     * <p>调用方：NamingHttpClientProxy.reqApi() L390，
     * 域名模式下对同一域名进行最多 maxRetry 次重试。</p>
     *
     * @return Nacos 域名，可能为 null（非域名模式）
     */
    public String getNacosDomain() {
        return nacosDomain;
    }
    
    /**
     * 判断是否为域名直连模式 —— 单地址 + PropertiesListProvider 时返回 true。
     *
     * <p>调用方：NamingHttpClientProxy.reqApi() L384、L389，
     * 根据此标志选择不同的重试策略：</p>
     * <ul>
     *   <li>true（域名模式）→ 对同一域名重试 maxRetry 次</li>
     *   <li>false（多 server 模式）→ 随机起点轮询所有 server</li>
     * </ul>
     *
     * @return true 表示是域名模式，false 表示是多 server IP 列表模式
     */
    public boolean isDomain() {
        return isDomain;
    }
    
    /**
     * 返回模块名 —— 固定返回 "Naming"。
     * <p>由父类 AbstractServerListManager 构造器调用，设置 CLIENT_MODULE_TYPE 属性。
     * 用于区分不同模块（Naming / Config / AI 等）的 ServerListManager 实例。</p>
     *
     * @return 模块名 "Naming"
     */
    @Override
    protected String getModuleName() {
        return "Naming";
    }
    
    /**
     * 获取 HTTP 客户端 —— 返回 Naming 模块专用的 NacosRestTemplate。
     * <p>由父类 AbstractServerListManager.start() 调用，传给 ServerListProvider.init()
     * 用于 Endpoint 方式下通过 HTTP 获取地址列表。</p>
     *
     * @return NamingHttpClientManager 管理的单例 NacosRestTemplate
     */
    @Override
    protected NacosRestTemplate getNacosRestTemplate() {
        return NamingHttpClientManager.getInstance().getNacosRestTemplate();
    }
    
    /**
     * 轮询获取下一个 server 地址 —— 原子递增索引并取模，实现 Round-Robin 轮询。
     *
     * <p>实现 ServerListFactory 接口，供 RpcClient 调用。
     * 每次调用 currentIndex 原子自增 1，然后模 serverList.size()，
     * 保证多线程环境下的正确性和公平性。</p>
     *
     * <p>调用方（间接）：RpcClient 通过 ServerListFactory 接口调用，
     * 用于 gRPC 连接建立和故障转移时选择下一台 server。
     * 具体链路：RpcClient → ServerListFactory.genNextServer() → NamingServerListManager.genNextServer()。</p>
     *
     * @return 下一台 server 的地址（格式如 "127.0.0.1:8848" 或 "nacos.example.com"）
     */
    @Override
    public String genNextServer() {
        int index = currentIndex.incrementAndGet() % getServerList().size();
        return getServerList().get(index);
    }
    
    /**
     * 获取当前连接中的 server 地址 —— 读取 currentIndex 但不自增。
     *
     * <p>实现 ServerListFactory 接口，供 RpcClient 调用。
     * 与 genNextServer() 的区别在于不会递增索引，仅返回当前索引对应的 server。</p>
     *
     * <p>调用方（间接）：RpcClient 通过 ServerListFactory 接口调用，
     * 用于获取当前正在使用的 server 地址。</p>
     *
     * @return 当前索引对应的 server 地址
     */
    @Override
    public String getCurrentServer() {
        return getServerList().get(currentIndex.get() % getServerList().size());
    }
}
