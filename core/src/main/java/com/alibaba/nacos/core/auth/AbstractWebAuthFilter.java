/*
 * Copyright 1999-2023 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.core.auth;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.api.NacosApiException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.api.model.v2.ErrorCode;
import com.alibaba.nacos.api.model.v2.Result;
import com.alibaba.nacos.auth.HttpProtocolAuthService;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.config.NacosAuthConfig;
import com.alibaba.nacos.auth.serveridentity.ServerIdentityResult;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.core.code.ControllerMethodsCache;
import com.alibaba.nacos.core.context.RequestContext;
import com.alibaba.nacos.core.context.RequestContextHolder;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.plugin.auth.api.AuthResult;
import com.alibaba.nacos.plugin.auth.api.IdentityContext;
import com.alibaba.nacos.plugin.auth.api.Permission;
import com.alibaba.nacos.plugin.auth.api.Resource;
import com.alibaba.nacos.plugin.auth.constant.Constants;
import com.alibaba.nacos.plugin.auth.exception.AccessException;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * HTTP 鉴权 Filter 的抽象基类，实现 Servlet {@link Filter} 接口。
 *
 * <h2>核心职责</h2>
 * <p>定义了一个 9 步鉴权管道（模板方法），所有 HTTP 鉴权 Filter
 * （{@link AuthFilter} 和 {@link AuthAdminFilter}）共享同一套执行流程，
 * 只通过 3 个钩子方法注入差异化行为。</p>
 *
 * <h2>9 步鉴权管道</h2>
 * <pre>{@code
 *   请求进入
 *     ├─ 1. methodsCache.getMethod(req) → 查不到？→ 放行（不是 API）
 *     ├─ 2. @Secured 注解不存在？       → 放行（公开接口）
 *     ├─ 3. 记录 ApiType 到 RequestContext
 *     ├─ 4. isMatchFilter()=false？     → 放行（不是我该管的 Filter）
 *     ├─ 5. isAuthEnabled()=false？     → 放行（鉴权总开关关闭）
 *     ├─ 6. checkServerIdentity()       → 节点间身份校验
 *     │      ├─ FAIL    → 403
 *     │      └─ MATCHED → 直接放行（节点间请求跳过用户鉴权）
 *     ├─ 7. validateIdentity()          → 用户身份校验
 *     │      └─ 失败 → 403
 *     ├─ 8. isIdentityOnlyApi()？       → 是 → 放行（仅需身份，不需权限）
 *     └─ 9. validateAuthority()         → 权限校验
 *            └─ 失败 → 403
 * }</pre>
 *
 * <h2>子类差异化：3 个钩子方法</h2>
 * <table>
 *   <tr><th>钩子方法</th><th>AuthFilter</th><th>AuthAdminFilter</th></tr>
 *   <tr><td>isMatchFilter()</td><td>非 ADMIN_API → true</td><td>ADMIN_API → true</td></tr>
 *   <tr><td>isAuthEnabled()</td><td>默认 false（OPEN_API 不鉴权）</td><td>默认 true（ADMIN_API 要鉴权）</td></tr>
 *   <tr><td>checkServerIdentity()</td><td>INNER_API + 升级未完成 → 跳过</td><td>不覆盖（继承默认）</td></tr>
 * </table>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>ControllerMethodsCache</b> —— URL→Method 映射，运行反射获取 @Secured</li>
 *   <li><b>HttpProtocolAuthService</b> —— 门面，封装 parseResource/parseIdentity/validateIdentity/validateAuthority</li>
 *   <li><b>RequestContext</b> —— ThreadLocal 存储当前请求的鉴权上下文</li>
 * </ul>
 *
 * <h2>错误处理</h2>
 * <p>所有鉴权失败都通过 try-catch 捕获，按 5 种异常类型分类处理：
 * AccessException → 403、IllegalArgumentException → 400、
 * NacosApiException/NacosException/NacosRuntimeException → 对应错误码、
 * IOException/ServletException/RuntimeException → 原样抛出。</p>
 *
 * @author xiweng.yy
 * @see AuthFilter
 * @see AuthAdminFilter
 */
public abstract class AbstractWebAuthFilter implements Filter {
    
    private final ControllerMethodsCache methodsCache;
    
    private final HttpProtocolAuthService protocolAuthService;
    
    protected AbstractWebAuthFilter(NacosAuthConfig authConfig,
        ControllerMethodsCache methodsCache) {
        this.methodsCache = methodsCache;
        this.protocolAuthService = new HttpProtocolAuthService(authConfig);
        this.protocolAuthService.initialize();
    }
    
    /**
     * 执行 9 步鉴权管道（模板方法）。
     *
     * <p>前 5 步是快速放行（查不到方法、无 @Secured、不匹配 Filter、鉴权关闭、无身份校验需求），
     * 后 4 步是真正的鉴权（节点身份 → 用户身份 → 权限）。
     * 任何一步不放行都会终止请求。</p>
     *
     * <p>调用方：Tomcat 的 {@link FilterChain}（Servlet 容器自动调用）。</p>
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        
        // Step 1: 通过 URL 查找对应的 Controller 方法
        Method method = methodsCache.getMethod(req);
        if (method == null) {
            chain.doFilter(request, response);
            return;
        }
        
        // Step 2: 没有 @Secured 注解 → 公开接口，无需鉴权
        if (!method.isAnnotationPresent(Secured.class)) {
            chain.doFilter(request, response);
            return;
        }
        
        try {
            Secured secured = method.getAnnotation(Secured.class);
            
            // Step 3: 将 ApiType 写入 RequestContext，供后续处理使用
            RequestContext requestContext = RequestContextHolder.getContext();
            requestContext.getAuthContext().setApiType(secured.apiType().name());
            
            // Step 4: 钩子 —— 这个 Filter 该处理这种 ApiType 吗？
            //   AuthFilter: 非 ADMIN_API → true
            //   AuthAdminFilter: ADMIN_API → true
            if (!isMatchFilter(secured)) {
                chain.doFilter(request, response);
                return;
            }
            
            // Step 5: 钩子 —— 鉴权总开关是否开启？
            //   AuthFilter: OPEN_API 默认 false（不鉴权）
            //   AuthAdminFilter: ADMIN_API 默认 true（要鉴权）
            if (!isAuthEnabled()) {
                chain.doFilter(request, response);
                return;
            }
            
            if (Loggers.AUTH.isDebugEnabled()) {
                Loggers.AUTH.debug("auth start, request: {} {}", req.getMethod(),
                    req.getRequestURI());
            }
            
            // Step 6: 节点间身份校验 —— 来自其他 Nacos 节点的请求
            //   FAIL    → 403 拒绝
            //   MATCHED → 直接放行（节点间请求不走用户身份/权限校验）
            //   NONE    → 普通用户请求，继续 Step 7
            ServerIdentityResult serverIdentityResult = checkServerIdentity(req, secured);
            switch (serverIdentityResult.getStatus()) {
                case FAIL:
                    writeResultResponse(resp, HttpServletResponse.SC_FORBIDDEN,
                        Result.failure(ErrorCode.ACCESS_DENIED, serverIdentityResult.getMessage()));
                    return;
                case MATCHED:
                    chain.doFilter(request, response);
                    return;
                default:
                    break;
            }
            
            // 检查插件的 enableAuth 覆盖（某些场景下插件可以禁用鉴权）
            if (!protocolAuthService.enableAuth(secured)) {
                chain.doFilter(request, response);
                return;
            }
            
            // Step 7: 用户身份校验
            //   parseResource: 从 URL + @Secured 解析出 Resource（如 namespace+dataId）
            //   parseIdentity: 从 Authorization header 解析出 IdentityContext（用户名/token）
            //   validateIdentity: 调用 AuthPluginManager → 具体插件验证用户名密码
            Resource resource = protocolAuthService.parseResource(req, secured);
            IdentityContext identityContext = protocolAuthService.parseIdentity(req);
            AuthResult result = protocolAuthService.validateIdentity(identityContext, resource);
            
            // 将鉴权结果写入 ThreadLocal，业务代码可以通过 RequestContext 读取
            requestContext.getAuthContext().setIdentityContext(identityContext);
            requestContext.getAuthContext().setResource(resource);
            requestContext.getAuthContext().setAuthResult(result);
            
            if (!result.isSuccess()) {
                throw new AccessException(result.format());
            }
            
            // Step 8: 仅需身份校验的 API？
            //   条件：@Secured(tags = "IDENTITY_ONLY") — 知道你是谁就行，不需要查权限
            if (isIdentityOnlyApi(secured)) {
                if (Loggers.AUTH.isDebugEnabled()) {
                    Loggers.AUTH.debug(
                        "API is identity only, skip validate authority, request: {} {}",
                        req.getMethod(),
                        req.getRequestURI());
                }
                chain.doFilter(request, response);
                return;
            }
            
            // Step 9: 权限校验
            //   检查用户对这个资源是否有 READ/WRITE 权限
            String action = secured.action().toString();
            result = protocolAuthService.validateAuthority(identityContext,
                new Permission(resource, action));
            if (!result.isSuccess()) {
                throw new AccessException(result.format());
            }
            
            // 全部通过 → 进入业务 Controller
            chain.doFilter(request, response);
        } catch (Exception e) {
            // 统一异常处理：按 5 种异常类型分类转换为 HTTP 响应
            handleFilterException(req, resp, e);
        }
    }
    
    private void handleFilterException(HttpServletRequest req, HttpServletResponse resp,
        Exception e)
        throws IOException, ServletException {
        if (e instanceof AccessException accessException) {
            if (Loggers.AUTH.isDebugEnabled()) {
                Loggers.AUTH.debug("access denied, request: {} {}, reason: {}", req.getMethod(),
                    req.getRequestURI(),
                    accessException.getErrMsg());
            }
            writeResultResponse(resp, HttpServletResponse.SC_FORBIDDEN,
                Result.failure(ErrorCode.ACCESS_DENIED, accessException.getErrMsg()));
            return;
        }
        if (e instanceof IllegalArgumentException) {
            writeResultResponse(resp, HttpServletResponse.SC_BAD_REQUEST,
                Result.failure(ErrorCode.PARAMETER_VALIDATE_ERROR,
                    ExceptionUtil.getAllExceptionMsg(e)));
            return;
        }
        if (e instanceof NacosApiException nacosApiException) {
            writeResultResponse(resp, nacosApiException.getErrCode(),
                new Result<>(nacosApiException.getDetailErrCode(),
                    nacosApiException.getErrAbstract(),
                    nacosApiException.getErrMsg()));
            return;
        }
        if (e instanceof NacosException nacosException) {
            writeResultResponse(resp, nacosException.getErrCode(),
                Result.failure(ErrorCode.SERVER_ERROR, nacosException.getErrMsg()));
            return;
        }
        if (e instanceof NacosRuntimeException nacosRuntimeException) {
            writeResultResponse(resp, nacosRuntimeException.getErrCode(),
                Result.failure(ErrorCode.SERVER_ERROR, nacosRuntimeException.getMessage()));
            return;
        }
        handleUnexpectedException(e);
    }
    
    private void handleUnexpectedException(Exception e) throws IOException, ServletException {
        Loggers.AUTH.warn("[AUTH-FILTER] Server failed: ", e);
        if (e instanceof IOException) {
            throw (IOException) e;
        }
        if (e instanceof ServletException) {
            throw (ServletException) e;
        }
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        throw new ServletException(e);
    }
    
    private void writeResultResponse(HttpServletResponse response, int status, Result<?> result)
        throws IOException {
        WebUtils.response(response, JacksonUtils.toJson(result), status);
    }
    
    private boolean isIdentityOnlyApi(Secured secured) {
        for (String tag : secured.tags()) {
            if (Constants.Tag.ONLY_IDENTITY.equals(tag)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 判断这个 Filter 是否应该处理当前 API。
     *
     * <p>这是子类差异化最关键的方法 —— 决定 Filter 的"路由规则"。</p>
     *
     * <p>调用方：{@link #doFilter(ServletRequest, ServletResponse, FilterChain)} Step 4。</p>
     *
     * <p>子类覆盖：</p>
     * <ul>
     *   <li>AuthFilter: 非 ADMIN_API → true（处理 OPEN/CONSOLE/INNER_API）</li>
     *   <li>AuthAdminFilter: ADMIN_API → true（只处理 ADMIN_API）</li>
     * </ul>
     *
     * @param secured 方法上的 @Secured 注解
     * @return {@code true} 如果这个 Filter 应该处理此请求
     */
    protected boolean isMatchFilter(Secured secured) {
        return true;
    }
    
    /**
     * 检查请求是否来自另一个 Nacos 节点（而非外部用户）。
     *
     * <p>调用方：{@link #doFilter(ServletRequest, ServletResponse, FilterChain)} Step 6。</p>
     *
     * <p>子类覆盖：</p>
     * <ul>
     *   <li>AuthFilter: INNER_API + 升级未完成 → 跳过校验（兼容 2.x 老节点）</li>
     *   <li>AuthAdminFilter: 不覆盖（继承默认，不处理节点间请求）</li>
     * </ul>
     *
     * <p>默认委托给 HttpProtocolAuthService.checkServerIdentity()。</p>
     *
     * @param request HTTP 请求
     * @param secured 方法上的 @Secured 注解
     * @return FAIL/MATCHED/NONE 三种结果
     */
    protected ServerIdentityResult checkServerIdentity(HttpServletRequest request,
        Secured secured) {
        return protocolAuthService.checkServerIdentity(request, secured);
    }
    
    /**
     * 鉴权总开关是否开启（抽象方法，必须由子类实现）。
     *
     * <p>调用方：{@link #doFilter(ServletRequest, ServletResponse, FilterChain)} Step 5。</p>
     *
     * <p>子类实现：</p>
     * <ul>
     *   <li>AuthFilter: 读取 NacosServerAuthConfig.isAuthEnabled()（默认 false）</li>
     *   <li>AuthAdminFilter: 读取 NacosServerAdminAuthConfig.isAuthEnabled()（默认 true）</li>
     * </ul>
     *
     * @return get value from {@link NacosAuthConfig#isAuthEnabled()}
     */
    protected abstract boolean isAuthEnabled();
}
