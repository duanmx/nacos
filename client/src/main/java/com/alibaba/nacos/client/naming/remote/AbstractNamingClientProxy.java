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

package com.alibaba.nacos.client.naming.remote;

import com.alibaba.nacos.plugin.auth.api.RequestResource;
import com.alibaba.nacos.client.address.ServerListChangeEvent;
import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.client.utils.AppNameUtils;
import com.alibaba.nacos.common.notify.listener.Subscriber;

import java.util.HashMap;
import java.util.Map;

/**
 * Naming 客户端代理抽象基类 —— 为 HTTP 和 gRPC 代理提供鉴权和 403 重登的共用逻辑。
 *
 * <p>相当于所有命名代理的"安全底座"，封装了客户端身份认证（获取 accessToken、注入 app 标识）
 * 以及 token 过期时的强制重登录（reLogin）。</p>
 *
 * <h2>继承关系</h2>
 * <pre>{@code
 *   Subscriber<ServerListChangeEvent>  ← 监听服务端地址变更事件
 *        ↑
 *   AbstractNamingClientProxy          ← 本类（鉴权抽象）
 *        ↑                    ↑
 *   NamingGrpcClientProxy   NamingHttpClientProxy
 *        (gRPC 代理)          (HTTP 代理，已废弃)
 * }</pre>
 *
 * <h2>两个子类的差异化处理</h2>
 * <ul>
 *   <li><b>NamingGrpcClientProxy</b>：鉴权失败（403）时回调
 *       {@code requestToServer()} 中的 403 处理逻辑 → 触发
 *       {@link SecurityProxy#reLogin()} 重新登录 → 重置 gRPC 连接</li>
 *   <li><b>NamingHttpClientProxy</b>：鉴权失败（403）时回调
 *       {@link com.alibaba.nacos.client.naming.remote.http.NamingHttpClientProxy#callServer
 *       callServer()} L646 → 触发
 *       {@link #reLogin()} → HTTP 协议层面不重置连接，仅刷新 token</li>
 * </ul>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>SecurityProxy</b> —— 客户端安全凭证管理器，持有 loginService 引用，
 *       负责实际登录、token 刷新</li>
 *   <li><b>AppNameUtils</b> —— 获取应用名（app 标识），用于服务端区分不同应用</li>
 *   <li><b>ServerListChangeEvent</b> —— 父类 Subscriber 订阅的事件，
 *       服务端地址变更时回调 {@code onEvent()}</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：NamingGrpcClientProxy / NamingHttpClientProxy 构造器（通过 super 调用）</li>
 *   <li><b>关闭者</b>：随子类 shutdown() 一起销毁</li>
 * </ul>
 *
 * @author xiweng.yy
 */
public abstract class AbstractNamingClientProxy extends Subscriber<ServerListChangeEvent>
    implements NamingClientProxy {
    
    /** HTTP header 中应用名的 key，固定为 "app"。 */
    private static final String APP_FILED = "app";
    
    /**
     * 客户端安全凭证管理器 —— 负责登录（login）、获取 identity context（accessToken）、
     * 以及在鉴权失败时强制重新登录（reLogin）。
     */
    private final SecurityProxy securityProxy;
    
    /**
     * 构造抽象命名代理并持有安全凭证管理器。
     *
     * @param securityProxy 客户端安全凭证管理器（由 NamingClientProxyDelegate 构造器传入）
     */
    protected AbstractNamingClientProxy(SecurityProxy securityProxy) {
        this.securityProxy = securityProxy;
    }
    
    /**
     * 获取服务端鉴权请求头 —— 包含 accessToken 等鉴权信息 + app 标识。
     *
     * <p>调用方（通过 NamingHttpClientProxy/callServer 间接调用）：</p>
     * <ul>
     *   <li>NamingHttpClientProxy.callServer() L604 —— 每次 HTTP 请求前注入鉴权参数</li>
     *   <li>NamingHttpClientProxy.callServer() 通过
     *       {@link com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy NamingGrpcClientProxy}
     *       获取 gRPC metadata</li>
     * </ul>
     *
     * <h3>流程（2 步）</h3>
     * <ol>
     *   <li>构建 RequestResource（namespace + group + serviceName）
     *       → 调用 securityProxy.getIdentityContext() 获取 accessToken 等 header</li>
     *   <li>追加 app 标识头（通过 getAppHeaders()）</li>
     * </ol>
     *
     * @param namespace   命名空间
     * @param group       分组
     * @param serviceName 服务名
     * @return 鉴权 header Map（含 accessToken + app）
     */
    protected Map<String, String> getSecurityHeaders(String namespace, String group,
        String serviceName) {
        // Step 1: 通过 SecurityProxy 获取 accessToken 等鉴权信息
        RequestResource resource =
            RequestResource.namingBuilder().setNamespace(namespace).setGroup(group)
                .setResource(serviceName).build();
        Map<String, String> result = this.securityProxy.getIdentityContext(resource);
        // Step 2: 追加 app 标识
        result.putAll(getAppHeaders());
        return result;
    }
    
    /**
     * 获取应用名请求头 —— 服务端通过 app 标识区分不同应用的请求。
     *
     * @return 包含 "app" → 应用名的 Map（大小为 1）
     */
    protected Map<String, String> getAppHeaders() {
        Map<String, String> result = new HashMap<>(1);
        result.put(APP_FILED, AppNameUtils.getAppName());
        return result;
    }
    
    /**
     * 强制重新登录 —— token 过期或 403 时调用。
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>NamingGrpcClientProxy.requestToServer() —— gRPC 请求收到 403 响应时</li>
     *   <li>NamingHttpClientProxy.callServer() L647 —— HTTP 请求收到 403 响应时</li>
     * </ul>
     *
     * <p>SecurityProxy.reLogin() 会将登录状态标记为过期，下次请求时自动触发重新登录。
     * 注意：这只是<b>标记</b>登录态过期，实际登录发生在下次请求时。</p>
     */
    protected void reLogin() {
        securityProxy.reLogin();
    }
}
