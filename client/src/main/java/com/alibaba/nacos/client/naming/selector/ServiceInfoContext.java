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

package com.alibaba.nacos.client.naming.selector;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.selector.NamingContext;

import java.util.List;

/**
 * ServiceInfo 上下文适配器 —— 将 ServiceInfo 适配为 NamingContext 接口。
 *
 * <h2>核心职责</h2>
 * <p>将客户端缓存的 ServiceInfo 对象包装为 NamingSelector 所需的 NamingContext，
 * 使 NamingSelector 可以基于 serviceName、groupName、clusters、instances 等信息进行选择。</p>
 *
 * <h2>使用场景</h2>
 * <p>当用户调用 getAllInstances/selectInstances 等方法时，客户端从 ServiceInfoHolder
 * 获取 ServiceInfo 后，通过 ServiceInfoContext 适配为 NamingContext，再交给 selector 过滤。</p>
 *
 * @author xiweng.yy
 */
public class ServiceInfoContext implements NamingContext {
    
    /**
     * 服务端下发的服务信息快照。
     */
    private final ServiceInfo serviceInfo;
    
    public ServiceInfoContext(ServiceInfo serviceInfo) {
        this.serviceInfo = serviceInfo;
    }
    
    @Override
    public String getServiceName() {
        return serviceInfo.getName();
    }
    
    @Override
    public String getGroupName() {
        return serviceInfo.getGroupName();
    }
    
    @Override
    public String getClusters() {
        return serviceInfo.getClusters();
    }
    
    @Override
    public List<Instance> getInstances() {
        return serviceInfo.getHosts();
    }
}
