/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.security;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.address.AbstractServerListManager;
import com.alibaba.nacos.client.address.ServerListChangeEvent;
import com.alibaba.nacos.client.auth.impl.NacosAuthLoginConstant;
import com.alibaba.nacos.common.http.client.NacosRestTemplate;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.plugin.auth.api.LoginIdentityContext;
import com.alibaba.nacos.plugin.auth.api.RequestResource;
import com.alibaba.nacos.plugin.auth.spi.client.ClientAuthPluginManager;
import com.alibaba.nacos.plugin.auth.spi.client.ClientAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Nacos 客户端安全代理（鉴权门面） —— 基于 SPI 插件化机制，统一管理客户端与
 * Nacos 服务端之间的认证鉴权交互。
 *
 * <h2>核心职责</h2>
 * <p>对上层所有通信组件（Naming、Config、Lock）提供统一的鉴权能力：
 * <ul>
 *   <li>驱动 {@link ClientAuthPluginManager} 加载所有 SPI 鉴权插件</li>
 *   <li>提供 {@link #login(Properties)} 和 {@link #reLogin()} 管理登录态</li>
 *   <li>通过 {@link #getIdentityContext(RequestResource)} 为每次 RPC/HTTP
 *       请求注入鉴权 Header（如 accessToken）</li>
 *   <li>监听服务端地址变更事件，自动刷新鉴权插件的服务端列表</li>
 * </ul>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   用户 Properties（用户名/密码/AccessKey）
 *     → SecurityProxy.login()
 *       → ClientAuthPluginManager → 遍历所有 ClientAuthService SPI 实现
 *         → 各插件完成登录，持有 LoginIdentityContext（含 accessToken）
 *
 *   每次 RPC/HTTP 调用：
 *     → AbstractNamingClientProxy.getSecurityHeaders() / AbstractLockClient.getSecurityHeaders()
 *       → SecurityProxy.getIdentityContext(resource)
 *         → 从各插件的 LoginIdentityContext 中提取 Header（如 accessToken、app）
 *           → 注入到 gRPC Metadata 或 HTTP Header
 * }</pre>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>ClientAuthPluginManager</b> — SPI 插件管理器，负责加载和生命周期管理</li>
 *   <li><b>ClientAuthService（SPI）</b> — 各鉴权插件实现（如默认用户名密码、RAM、OIDC）</li>
 *   <li><b>AbstractServerListManager</b> — 服务端地址列表管理器</li>
 *   <li><b>NotifyCenter</b> — 订阅 ServerListChangeEvent，服务端地址变更时自动刷新</li>
 * </ul>
 *
 * <h2>调用方（上层通信组件均依赖此类）</h2>
 * <ul>
 *   <li>NacosConfigService 构造器 — 通过 SecurityProxy 管理 Config 鉴权</li>
 *   <li>AbstractNamingClientProxy.getSecurityHeaders() — 为 Naming gRPC/HTTP 请求注入鉴权</li>
 *   <li>NacosLockService 构造器 — 为 Lock gRPC 请求注入鉴权</li>
 *   <li>AiGrpcClient/AbstractLockClient.getSecurityHeaders() — AI 和 Lock 模块鉴权</li>
 * </ul>
 *
 * @author nkorange
 * @since 1.2.0
 */
public class SecurityProxy implements Closeable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SecurityProxy.class);
    
    private ClientAuthPluginManager clientAuthPluginManager;
    
    /**
     * 构造安全代理，初始化鉴权插件管理器并注册服务端地址变更监听。
     *
     * <p>构造完成后会自动注册一个 {@link ServerListChangeEvent} 订阅者，
     * 当服务端地址列表变更时，通知所有鉴权插件刷新连接。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>NacosConfigService 构造器 — 创建 Config 模块的安全代理</li>
     *   <li>NacosLockService 构造器 — 创建 Lock 模块的安全代理</li>
     *   <li>NamingClientProxyDelegate 构造器 — 创建 Naming 模块的安全代理</li>
     * </ul>
     *
     * @param serverListManager 服务端地址列表管理器，客户端请求的目标服务端
     * @param nacosRestTemplate HTTP 请求模板，用于鉴权插件的 HTTP 通信（如登录接口）
     */
    public SecurityProxy(AbstractServerListManager serverListManager,
        NacosRestTemplate nacosRestTemplate) {
        clientAuthPluginManager = new ClientAuthPluginManager();
        clientAuthPluginManager.init(serverListManager.getServerList(), nacosRestTemplate);
        NotifyCenter.registerSubscriber(new Subscriber<ServerListChangeEvent>() {
            
            @Override
            public void onEvent(ServerListChangeEvent event) {
                clientAuthPluginManager.refreshServerList(serverListManager.getServerList());
            }
            
            @Override
            public Class<? extends Event> subscribeType() {
                return ServerListChangeEvent.class;
            }
        });
    }
    
    /**
     * 驱动所有已加载的鉴权插件执行登录操作。
     *
     * <p>此方法幂等：如果没有任何 SPI 鉴权插件（如未配置用户名密码），直接返回。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>NacosConfigService.loginWithProperties() — 首次登录</li>
     *   <li>NacosLockService.initSecurityProxy() — Lock 模块定时刷新登录态</li>
     *   <li>NamingClientProxyDelegate 构造器 — Naming 模块初始登录</li>
     * </ul>
     *
     * @param properties 包含登录凭证的配置（用户名/密码/AccessKey 等）
     */
    public void login(Properties properties) {
        if (clientAuthPluginManager.getAuthServiceSpiImplSet().isEmpty()) {
            return;
        }
        for (ClientAuthService clientAuthService : clientAuthPluginManager
            .getAuthServiceSpiImplSet()) {
            clientAuthService.login(properties);
        }
    }
    
    /**
     * 获取所有鉴权插件提供的身份上下文 Header 集合。
     *
     * <p>遍历所有 SPI 鉴权插件，从各插件的 {@link LoginIdentityContext} 中提取
     * 所有 key-value 对（如 accessToken、ak/sk 签名等），合并为一个 Map 返回。
     * 调用方将此 Map 注入到 gRPC Metadata 或 HTTP Header 中。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>AbstractNamingClientProxy.getSecurityHeaders() — Naming 模块每次 RPC/HTTP 调用</li>
     *   <li>AbstractLockClient.getSecurityHeaders() — Lock 模块每次 RPC 调用</li>
     *   <li>AiGrpcClient — AI 模块每次 RPC 调用</li>
     * </ul>
     *
     * @param resource 请求资源描述（用于 RAM 鉴权等需要区分资源的场景）
     * @return 合并后的鉴权 Header Map，至少包含一个 key-value 对
     */
    public Map<String, String> getIdentityContext(RequestResource resource) {
        Map<String, String> header = new HashMap<>(1);
        for (ClientAuthService clientAuthService : clientAuthPluginManager
            .getAuthServiceSpiImplSet()) {
            LoginIdentityContext loginIdentityContext =
                clientAuthService.getLoginIdentityContext(resource);
            for (String key : loginIdentityContext.getAllKey()) {
                header.put(key, loginIdentityContext.getParameter(key));
            }
        }
        return header;
    }
    
    @Override
    public void shutdown() throws NacosException {
        clientAuthPluginManager.shutdown();
    }
    
    /**
     * 重新登录 —— 标记当前 accessToken 即将过期，触发插件刷新登录态。
     *
     * <p>与 {@link #login(Properties)} 不同，此方法不重新传递 Properties，
     * 而是在当前 {@link LoginIdentityContext} 上设置 RELOGINFLAG 标记，
     * 告知插件使用已有凭证重新获取 accessToken。</p>
     *
     * <p>典型触发场景：服务端返回 403 禁止访问时，调用方判断是 accessToken
     * 过期，先调用此方法刷新 Token，然后重试原请求。参见
     * NamingHttpClientProxy.reqApi() 中的 403 处理逻辑。</p>
     */
    public void reLogin() {
        if (clientAuthPluginManager.getAuthServiceSpiImplSet().isEmpty()) {
            return;
        }
        for (ClientAuthService clientAuthService : clientAuthPluginManager
            .getAuthServiceSpiImplSet()) {
            try {
                LoginIdentityContext loginIdentityContext =
                    clientAuthService.getLoginIdentityContext(new RequestResource());
                if (loginIdentityContext != null) {
                    loginIdentityContext.setParameter(NacosAuthLoginConstant.RELOGINFLAG, "true");
                }
            } catch (Exception e) {
                LOGGER.error("[SecurityProxy] set reLoginFlag failed.", e);
            }
        }
    }
}
