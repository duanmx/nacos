/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.auth;

import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.config.NacosAuthConfig;
import com.alibaba.nacos.auth.serveridentity.ServerIdentityResult;
import com.alibaba.nacos.plugin.auth.api.AuthResult;
import com.alibaba.nacos.plugin.auth.api.IdentityContext;
import com.alibaba.nacos.plugin.auth.api.Permission;
import com.alibaba.nacos.plugin.auth.api.Resource;
import com.alibaba.nacos.plugin.auth.exception.AccessException;

/**
 * 协议无关的鉴权服务接口（protocol-agnostic auth service）。
 *
 * <h2>核心职责</h2>
 * <p>定义从请求中解析身份/资源并校验的契约。HTTP 和 gRPC 两种协议共用同一套身份校验
 * （{@link #validateIdentity}）和权限校验（{@link #validateAuthority}）后端，
 * 只有从请求中提取信息的方式不同——通过泛型参数 {@code <R>}
 * （HTTP 用 {@code HttpServletRequest}，gRPC 用 {@code Request}）实现协议适配。</p>
 *
 * <h2>在鉴权管道中的位置</h2>
 * <pre>{@code
 *   Filter 层                           本接口
 *   ┌──────────────────────┐        ┌──────────────────────┐
 *   │ AbstractWebAuthFilter │──委托→│ ProtocolAuthService  │
 *   │ RemoteRequestAuthFilter│──委托→│   ├─ parseResource()  │  从请求中提取 Resource
 *   └──────────────────────┘        │   ├─ parseIdentity()  │  从请求中提取 Identity
 *                                    │   ├─ validateIdentity()│  验证用户身份
 *                                    │   ├─ validateAuthority()│  验证用户权限
 *                                    │   └─ checkServerIdentity()│  验证节点身份
 *                                    └──────────┬───────────┘
 *                                               │ 委托
 *                                               ▼
 *                                    ┌──────────────────────┐
 *                                    │ AuthPluginService    │  SPI 插件
 *                                    │ (nacos / ldap / oidc)│
 *                                    └──────────────────────┘
 * }</pre>
 *
 * <h2>类继承体系</h2>
 * <pre>{@code
 *   ProtocolAuthService<R>                    接口（本类）
 *         ↑
 *   AbstractProtocolAuthService<R>            模板方法基类
 *     ├─ 封装 AuthPluginManager SPI 委托
 *     ├─ 封装 ServerIdentityChecker 节点身份校验
 *     └─ 提供 parseSpecifiedResource() / useSpecifiedParserToParse() 工具方法
 *         ↑                        ↑
 *   HttpProtocolAuthService    GrpcProtocolAuthService
 *     (R = HttpServletRequest)    (R = gRPC Request)
 *     parseResource: Naming/      parseResource: 从 gRPC metadata 解析
 *       Config/Ai parser map
 *     parseIdentity: HTTP         parseIdentity: 从 gRPC metadata 解析
 *       header → IdentityContext
 * }</pre>
 *
 * <h2>调用方</h2>
 * <ul>
 *   <li>{@code AbstractWebAuthFilter.doFilter()} —— HTTP 鉴权管道的 Step 6-9，逐一调用本接口的 5 个方法</li>
 *   <li>{@code RemoteRequestAuthFilter.filter()} —— gRPC 鉴权管道，同样调用本接口的 5 个方法</li>
 * </ul>
 *
 * <h2>设计意图</h2>
 * <p>HTTP 和 gRPC 的区别仅在于"如何从请求里提取信息"
 * （HTTP 从 Header/URL 中读，gRPC 从 Metadata 中读），
 * 一旦提取出 IdentityContext 和 Resource，后续的身份校验和权限校验完全一致。
 * 本接口用泛型 {@code <R>} 抹平了这个差异——上层 Filter 不需要知道底层是 HTTP 还是 gRPC。</p>
 *
 * @param <R> 协议请求类型（HTTP: {@code HttpServletRequest}，gRPC: {@code Request}）
 * @author xiweng.yy
 * @see AbstractProtocolAuthService
 * @see HttpProtocolAuthService
 * @see com.alibaba.nacos.auth.GrpcProtocolAuthService
 */
public interface ProtocolAuthService<R> {
    
    /**
     * 初始化鉴权服务（在 Filter 构造时调用一次）。
     *
     * <p>典型工作：初始化 {@code ServerIdentityChecker}（加载节点身份配置），
     * 注册 ResourceParser（如 Naming/Config/Ai 的 HTTP 资源解析器）。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>{@code AbstractWebAuthFilter} 构造函数 L66-L67 —— 创建后立即初始化</li>
     *   <li>{@code RemoteRequestAuthFilter} 构造函数 —— 同上</li>
     * </ul>
     */
    void initialize();
    
    /**
     * 判断是否需要对当前 API 启用鉴权（插件级别的二次开关）。
     *
     * <p>注意：这个方法和 {@code NacosAuthConfig#isAuthEnabled()} 的作用不同：</p>
     * <ul>
     *   <li>{@code isAuthEnabled()} —— 总开关（全局 on/off），在 Step 5 检查</li>
     *   <li>{@code enableAuth(secured)} —— 插件开关，在 Step 6 之后检查。
     *       插件可以在此基础上做细粒度控制，如"只对 WRITE 操作鉴权"或"只对 NAMING 类型鉴权"</li>
     * </ul>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>{@code AbstractWebAuthFilter.doFilter()} —— HTTP 鉴权管道 Step 6 之后</li>
     * </ul>
     *
     * <p>实现：{@code AbstractProtocolAuthService} 委托给 {@code AuthPluginService.enableAuth()}</p>
     *
     * @param secured 方法上的 @Secured 注解（含 action、signType 等信息）
     * @return {@code true} 如果插件决定需要鉴权
     */
    boolean enableAuth(Secured secured);
    
    /**
     * 从协议请求和 @Secured 注解中解析出 Resource（资源标识）。
     *
     * <p>Resource 是鉴权的"对象"——如命名空间下的某个服务，或某个配置的 dataId。
     * 不同协议解析方式不同：</p>
     * <ul>
     *   <li>HTTP: 从 URL 路径 + @Secured.signType 找到对应的 ResourceParser
     *       （NamingHttpResourceParser / ConfigHttpResourceParser / AiHttpResourceParser）</li>
     *   <li>gRPC: 从 gRPC Metadata 中提取 namespace、group、dataId</li>
     * </ul>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>{@code AbstractWebAuthFilter.doFilter()} Step 7 —— 获取资源标识后传给
     *       validateIdentity/validateAuthority</li>
     *   <li>{@code RemoteRequestAuthFilter.filter()} —— 同上</li>
     * </ul>
     *
     * @param request 协议请求（HTTP: HttpServletRequest，gRPC: Request）
     * @param secured 方法上的 @Secured 注解
     * @return Resource（含 namespaceId、group、resourceName、signType）
     */
    Resource parseResource(R request, Secured secured);
    
    /**
     * 从协议请求中解析出 IdentityContext（用户身份上下文）。
     *
     * <p>不同协议从不同位置提取身份信息：</p>
     * <ul>
     *   <li>HTTP: 委托给 {@code HttpIdentityContextBuilder}，从 Authorization header、
     *       Bearer token、Basic Auth 等提取用户名/密码/token</li>
     *   <li>gRPC: 从 gRPC Metadata 中提取身份信息</li>
     * </ul>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>{@code AbstractWebAuthFilter.doFilter()} Step 7</li>
     *   <li>{@code RemoteRequestAuthFilter.filter()}</li>
     * </ul>
     *
     * @param request 协议请求
     * @return IdentityContext（含 username、token 等身份信息）
     */
    IdentityContext parseIdentity(R request);
    
    /**
     * 验证用户身份是否合法（你是谁？）。
     *
     * <p>这是鉴权管道 Step 7 的核心——验证用户名密码或 token 是否正确。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>{@code AbstractWebAuthFilter.doFilter()} L119 —— 失败则抛 AccessException（403）</li>
     *   <li>{@code RemoteRequestAuthFilter.filter()} L52-L58 —— 失败则返回 gRPC error response</li>
     * </ul>
     *
     * <p>实现：{@code AbstractProtocolAuthService} 委托给
     * {@code AuthPluginManager → AuthPluginService.validateIdentity()}</p>
     *
     * @param identityContext 从 parseIdentity() 获取的身份上下文
     * @param resource        从 parseResource() 获取的资源标识
     * @return AuthResult（成功/失败，失败时含错误码和错误信息）
     * @throws AccessException 鉴权过程中发生的异常（如 SPI 插件抛出的异常）
     */
    AuthResult validateIdentity(IdentityContext identityContext, Resource resource)
        throws AccessException;
    
    /**
     * 验证用户是否对指定资源有指定操作的权限（你能做什么？）。
     *
     * <p>这是鉴权管道 Step 9 —— 在身份校验通过后，检查该用户是否有权对
     * 这个 Resource 执行这个 Action（READ/WRITE）。</p>
     *
     * <p>注意：如果 @Secured 标记了 {@code tags = "IDENTITY_ONLY"}，
     * 则 Step 8 会跳过此方法（只验身份，不验权限）。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>{@code AbstractWebAuthFilter.doFilter()} L137-L138 —— 失败则抛 AccessException</li>
     *   <li>{@code RemoteRequestAuthFilter.filter()} L60-L64</li>
     * </ul>
     *
     * <p>实现：{@code AbstractProtocolAuthService} 委托给
     * {@code AuthPluginManager → AuthPluginService.validateAuthority()}</p>
     *
     * @param identityContext 身份上下文
     * @param permission      权限对象（含 resource + action，如"对 namespace/group/dataId 的 READ 权限"）
     * @return AuthResult
     * @throws AccessException 鉴权过程中的异常
     */
    AuthResult validateAuthority(IdentityContext identityContext, Permission permission)
        throws AccessException;
    
    /**
     * 检查请求是否来自另一个 Nacos 节点（而非外部用户）。
     *
     * <p>这是鉴权管道 Step 6 —— 级别最高：如果请求来自合法的集群节点，
     * 直接放行（MATCHED），跳过后续的用户身份和权限校验。
     * 如果身份不匹配则直接拒绝（FAIL）。</p>
     *
     * <p>识别方式：从请求中提取 serverIdentityKey header/metadata，
     * 与配置的 serverIdentityValue 对比。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>{@code AbstractWebAuthFilter.doFilter()} Step 6 —— 根据结果决定
     *       直接放行（MATCHED）、拒绝（FAIL）还是继续用户鉴权（NONE）</li>
     * </ul>
     *
     * <p>实现：{@code AbstractProtocolAuthService} 委托给
     * {@code ServerIdentityChecker.check()}</p>
     *
     * @param request 协议请求
     * @param secured 方法上的 @Secured 注解
     * @return FAIL（身份不匹配拒绝）/ MATCHED（节点身份匹配放行）/ NONE（非节点请求，继续用户鉴权）
     */
    ServerIdentityResult checkServerIdentity(R request, Secured secured);
}
