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

package com.alibaba.nacos.core.auth;

import com.alibaba.nacos.auth.config.NacosAuthConfigHolder;
import com.alibaba.nacos.core.code.ControllerMethodsCache;
import com.alibaba.nacos.core.web.NacosWebBean;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Nacos 鉴权 Filter 的 Spring 配置工厂（装配器）。
 *
 * <h2>核心职责</h2>
 * <p>这个类只做一件事：创建 {@link AuthFilter} 和 {@link AuthAdminFilter} 两个 Bean，
 * 并注册为 Servlet Filter（拦截所有 URL 路径 {@code /*}，优先级 6）。</p>
 *
 * <h2>为什么有两个 Filter 都拦截 {@code /*}？</h2>
 * <p>两个 Filter 通过 {@code isMatchFilter()} 互斥分工：</p>
 * <ul>
 *   <li>{@link AuthFilter} —— 处理非 ADMIN_API（OPEN_API、CONSOLE_API、INNER_API）</li>
 *   <li>{@link AuthAdminFilter} —— 只处理 ADMIN_API（管理后台 API）</li>
 * </ul>
 * <p>两者都注册 {@code /*} 是因为注册阶段无法预知 URL 对应的 ApiType，
 * 这是运行时通过 {@link ControllerMethodsCache} 获取的信息。
 * 实际执行时，同一请求只会被其中一个真正处理（被另一个跳过）。</p>
 *
 * <h2>为什么从 {@code NacosAuthConfigHolder} 获取配置而非 {@code @Autowired}？</h2>
 * <p>{@code NacosAuthConfigHolder} 通过 Java SPI（{@code NacosServiceLoader}）加载
 * 而非 Spring 容器加载。配置类在构造函数中做 fail-fast 校验，
 * 必须在 Spring Bean 初始化之前完成——如果配置非法，Nacos 启动即失败。</p>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>AuthFilter</b> —— 非 ADMIN 请求的鉴权 Filter，需要额外注入 {@code InnerApiAuthEnabled}
 *       用于 2.x→3.x 升级期间 INNER_API 的身份校验兼容</li>
 *   <li><b>AuthAdminFilter</b> —— ADMIN 请求的鉴权 Filter，只处理管理后台 API</li>
 *   <li><b>NacosAuthConfigHolder</b> —— SPI 配置注册表（单例），按 scope 查找对应的配置</li>
 *   <li><b>ControllerMethodsCache</b> —— URL→Method 映射缓存，Filter 用它反射获取
 *       {@code @Secured} 注解</li>
 *   <li><b>NacosServerAuthConfig</b> —— OPEN_API 的鉴权配置（scope=OPEN_API，
 *       默认不启用鉴权）</li>
 *   <li><b>NacosServerAdminAuthConfig</b> —— ADMIN_API 的鉴权配置（scope=ADMIN_API，
 *       默认启用鉴权）</li>
 * </ul>
 *
 * <h2>加载时机</h2>
 * <p>标注了 {@code @NacosWebBean}，意味着只在 Web 上下文（Tomcat 启动后的子容器）
 * 中加载，不会在 Core 上下文中加载。这是因为 Filter 依赖 Servlet 容器。</p>
 *
 * <h2>Filter 链中 AuthFilter 的位置</h2>
 * <pre>{@code
 *   order=1: TrafficReviseFilter         (naming 模块)
 *   order=5: FormSizeFilter              (core-web)
 *   order=6: AuthFilter / AuthAdminFilter ← 本类
 *   order=7: DistroFilter                (naming 模块)
 *   order=8: ParamCheckerFilter          (core)
 * }</pre>
 *
 * @author mai.jh
 * @see AbstractWebAuthFilter
 * @see com.alibaba.nacos.core.web.NacosWebBean
 */
@Configuration
@NacosWebBean
public class AuthConfig {
    
    /**
     * 注册 AuthFilter 到 Servlet Filter 链。
     *
     * <p>拦截所有 URL（{@code /*}），优先级 6。
     * AuthFilter 在运行时通过 {@code isMatchFilter()} 只处理非 ADMIN_API 的请求。</p>
     *
     * <p>被 Spring 容器自动调用（@Bean 方法）。</p>
     *
     * @param authFilter 由本类的 {@link #authFilter(ControllerMethodsCache, InnerApiAuthEnabled)}
     *                   方法创建的 AuthFilter Bean
     * @return FilterRegistrationBean，Spring Boot 会自动注册为 Servlet Filter
     */
    @Bean
    public FilterRegistrationBean<AuthFilter> authFilterRegistration(AuthFilter authFilter) {
        FilterRegistrationBean<AuthFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(authFilter);
        registration.addUrlPatterns("/*");
        registration.setName("authFilter");
        registration.setOrder(6);
        return registration;
    }
    
    /**
     * 注册 AuthAdminFilter 到 Servlet Filter 链。
     *
     * <p>与 AuthFilter 同样拦截所有 URL（{@code /*}），优先级 6。
     * AuthAdminFilter 在运行时通过 {@code isMatchFilter()} 只处理 ADMIN_API 的请求，
     * 与 AuthFilter 互斥。</p>
     *
     * <p>被 Spring 容器自动调用（@Bean 方法）。</p>
     *
     * @param authAdminFilter 管理后台鉴权 Filter
     * @return FilterRegistrationBean
     */
    @Bean
    public FilterRegistrationBean<AuthAdminFilter> authAdminFilterRegistration(
        AuthAdminFilter authAdminFilter) {
        FilterRegistrationBean<AuthAdminFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(authAdminFilter);
        registration.addUrlPatterns("/*");
        registration.setName("authAdminFilter");
        registration.setOrder(6);
        return registration;
    }
    
    /**
     * 创建 AuthFilter Bean（非 ADMIN_API 鉴权）。
     *
     * <p>从 {@code NacosAuthConfigHolder} 获取 scope=OPEN_API 的配置，
     * 注入 {@link InnerApiAuthEnabled} 用于 2.x→3.x 升级期间的兼容处理。</p>
     *
     * <p>被 Spring 容器自动调用（@Bean 工厂方法）。</p>
     *
     * @param methodsCache URL→Method 映射缓存，运行时反射获取 @Secured 注解
     * @param innerApiAuthEnabled 内网 API 身份校验开关（升级兼容）
     * @return AuthFilter 实例，持有 NacosServerAuthConfig（OPEN_API 配置）
     */
    @Bean
    public AuthFilter authFilter(ControllerMethodsCache methodsCache,
        InnerApiAuthEnabled innerApiAuthEnabled) {
        return new AuthFilter(NacosAuthConfigHolder.getInstance()
            .getNacosAuthConfigByScope(NacosServerAuthConfig.NACOS_SERVER_AUTH_SCOPE), methodsCache,
            innerApiAuthEnabled);
    }
    
    /**
     * 创建 AuthAdminFilter Bean（ADMIN_API 鉴权）。
     *
     * <p>从 {@code NacosAuthConfigHolder} 获取 scope=ADMIN_API 的配置。
     * 与 AuthFilter 不同，不需要 {@link InnerApiAuthEnabled}
     * —— ADMIN_API 不涉及升级兼容逻辑。</p>
     *
     * <p>被 Spring 容器自动调用（@Bean 工厂方法）。</p>
     *
     * @param methodsCache URL→Method 映射缓存，运行时反射获取 @Secured 注解
     * @return AuthAdminFilter 实例，持有 NacosServerAdminAuthConfig（ADMIN_API 配置，
     *         默认启用鉴权）
     */
    @Bean
    public AuthAdminFilter authAdminFilter(ControllerMethodsCache methodsCache) {
        return new AuthAdminFilter(NacosAuthConfigHolder.getInstance()
            .getNacosAuthConfigByScope(NacosServerAdminAuthConfig.NACOS_SERVER_ADMIN_AUTH_SCOPE),
            methodsCache);
    }
}
