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

package com.alibaba.nacos.client.naming.event;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.notify.Event;

import java.util.List;

/**
 * 实例变更事件 —— 当服务实例列表发生变化时由 ServiceInfoHolder 发布。
 *
 * <p>这是命名客户端事件体系中的核心事件。当服务端推送新的 ServiceInfo 或定时查询
 * 发现实例列表有变化时，通过 {@link InstancesChangeNotifier} 分发给所有注册的
 * {@link com.alibaba.nacos.api.naming.listener.EventListener}。</p>
 *
 * <h2>事件产生路径</h2>
 * <ol>
 *   <li>服务端推送：NamingPushRequestHandler → ServiceInfoHolder.processServiceInfo()
 *       → InstancesDiffer.doDiff() → 有 diff → 发布本事件</li>
 *   <li>定时轮询：ServiceInfoUpdateService → ServiceInfoHolder.processServiceInfo()
 *       → InstancesDiffer.doDiff() → 有 diff → 发布本事件</li>
 *   <li>订阅拉取：NamingClientProxyDelegate.subscribe() → processServiceInfo()
 *       → 同上</li>
 *   <li>故障转移：FailoverReactor 切换时也发布此事件</li>
 * </ol>
 *
 * <h2>scope 隔离机制</h2>
 * <p>每个 NacosNamingService 实例有唯一的 eventScope（UUID），
 * 事件通过 scope() 方法返回此值，确保事件只被同一 NamingService 实例的
 * Notifier 消费，避免跨实例污染。</p>
 *
 * @author horizonzy
 * @since 1.4.1
 */
public class InstancesChangeEvent extends Event {
    
    private static final long serialVersionUID = -8823087028212249603L;
    
    /** 事件所属的 NamingService 实例 scope（UUID），用于隔离不同实例。 */
    private final String eventScope;
    
    /** 服务名。 */
    private final String serviceName;
    
    /** 组名。 */
    private final String groupName;
    
    /** 集群名。 */
    private final String clusters;
    
    /** 当前完整实例列表（非仅 diff）。 */
    private final List<Instance> hosts;
    
    /** 差异详情（新增/移除/修改），由 InstancesDiffer 计算。 */
    private InstancesDiff instancesDiff;
    
    public InstancesChangeEvent(String eventScope, String serviceName, String groupName,
        String clusters, List<Instance> hosts, InstancesDiff diff) {
        this.eventScope = eventScope;
        this.serviceName = serviceName;
        this.groupName = groupName;
        this.clusters = clusters;
        this.hosts = hosts;
        this.instancesDiff = diff;
    }
    
    public String getServiceName() {
        return serviceName;
    }
    
    public String getGroupName() {
        return groupName;
    }
    
    public String getClusters() {
        return clusters;
    }
    
    public List<Instance> getHosts() {
        return hosts;
    }
    
    public InstancesDiff getInstancesDiff() {
        return instancesDiff;
    }
    
    @Override
    public String scope() {
        return this.eventScope;
    }
}
