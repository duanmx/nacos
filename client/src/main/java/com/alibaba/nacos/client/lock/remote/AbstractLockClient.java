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

package com.alibaba.nacos.client.lock.remote;

import com.alibaba.nacos.client.security.SecurityProxy;
import com.alibaba.nacos.client.utils.AppNameUtils;
import com.alibaba.nacos.plugin.auth.api.RequestResource;

import java.util.HashMap;
import java.util.Map;

/**
 * Nacos 分布式锁客户端抽象基类 —— 封装鉴权 Header 注入逻辑，
 * 为 {@link LockGrpcClient} 等具体实现提供共用的认证信息组装能力。
 *
 * <h2>核心职责</h2>
 * <p>将 {@link SecurityProxy} 提供的鉴权信息（accessToken 等）与
 * AppName 合并为 gRPC 请求的 Header，注入到每次锁操作请求中。
 * 这样具体实现（如 LockGrpcClient）只需关注网络通信，无需关心鉴权细节。</p>
 *
 * <h2>与 Naming 模块的对比</h2>
 * <p>与 {@link com.alibaba.nacos.client.naming.remote.AbstractNamingClientProxy}
 * 的角色类似 —— 都是向上层提供统一的鉴权 Header 获取接口，向下委托给 SecurityProxy。</p>
 *
 * @author 985492783@qq.com
 * @date 2023/6/28 17:19
 */
public abstract class AbstractLockClient implements LockClient {
    
    private final SecurityProxy securityProxy;
    
    private static final String APP_FILED = "app";
    
    protected AbstractLockClient(SecurityProxy securityProxy) {
        this.securityProxy = securityProxy;
    }
    
    /**
     * 获取包含鉴权信息的 gRPC 请求 Header。
     *
     * <p>调用链路：SecurityProxy.getIdentityContext(lockResource)
     * → 合并 AppName → 返回完整 Header Map。</p>
     *
     * @return 包含 accessToken + app 等字段的 Header Map
     */
    protected Map<String, String> getSecurityHeaders() {
        RequestResource resource = RequestResource.lockBuilder().build();
        Map<String, String> result = this.securityProxy.getIdentityContext(resource);
        result.putAll(getAppHeaders());
        return result;
    }
    
    /**
     * 获取包含应用名称的 Header。
     *
     * @return 包含 "app" → AppName 的 Map
     */
    protected Map<String, String> getAppHeaders() {
        Map<String, String> result = new HashMap<>(1);
        result.put(APP_FILED, AppNameUtils.getAppName());
        return result;
    }
}
