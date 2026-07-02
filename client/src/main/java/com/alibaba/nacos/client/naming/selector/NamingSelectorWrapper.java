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

import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.selector.NamingContext;
import com.alibaba.nacos.api.naming.selector.NamingSelector;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesDiff;
import com.alibaba.nacos.client.naming.listener.NamingChangeEvent;
import com.alibaba.nacos.client.selector.AbstractSelectorWrapper;
import com.alibaba.nacos.common.utils.CollectionUtils;

import java.util.Collections;
import java.util.List;

/**
 * Naming 选择器封装 —— 将 NamingSelector 与 EventListener 绑定，连接事件到回调。
 *
 * <h2>核心职责</h2>
 * <p>继承 AbstractSelectorWrapper，将 InstancesChangeEvent（实例级变更）：
 * <ol>
 *   <li>通过 NamingSelector 过滤实例列表（如只关注某个 cluster、某些 metadata 匹配的实例）</li>
 *   <li>计算过滤后的差异（added/removed/modified）</li>
 *   <li>构建 NamingChangeEvent，通过 NamingListenerInvoker 调用用户 EventListener</li>
 * </ol>
 * </p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   InstancesChangeNotifier.onEvent(InstancesChangeEvent)
 *     → SelectorManager.getSelectorWrappers(subId)
 *       → NamingSelectorWrapper.onEvent(event)
 *         ├─ isSelectable(event) —— 验证 event 不为空且有有效 diff
 *         ├─ buildListenerEvent(event)
 *         │   ├─ doSelect(event.getHosts()) —— 对全量实例执行 selector
 *         │   ├─ 对 addedInstances 执行 selector（只有匹配的实例才通知用户）
 *         │   ├─ 对 removedInstances 执行 selector
 *         │   ├─ 对 modifiedInstances 执行 selector
 *         │   └─ new NamingChangeEvent(..., newDiff)
 *         ├─ isCallable(namingEvent) —— 验证 at least 一个 diff 列表非空
 *         └─ getListenerInvoker().invoke(namingEvent)
 *             → NamingListenerInvoker.invoke()
 *               → EventListener.onEvent(NamingEvent)
 * }</pre>
 *
 * <h2>与 AbstractSelectorWrapper 的关系</h2>
 * <p>父类提供 selector + listenerInvoker 的通用管理逻辑（添加/移除/事件分发），
 * 本类负责 Naming 特有的类型适配：
 * InstancesChangeEvent → NamingEvent 的转换。</p>
 *
 * @author lideyou
 */
public class NamingSelectorWrapper
    extends AbstractSelectorWrapper<NamingSelector, NamingEvent, InstancesChangeEvent> {
    
    /**
     * 服务名 —— 用于构建 NamingChangeEvent。
     */
    private String serviceName;
    
    /**
     * 组名 —— 用于构建 NamingChangeEvent。
     */
    private String groupName;
    
    /**
     * 集群 —— 用于构建 NamingChangeEvent。
     */
    private String clusters;
    
    /**
     * 内部命名上下文 —— 复用对象，避免每次 select 都 new。
     */
    private final InnerNamingContext namingContext = new InnerNamingContext();
    
    /**
     * 内部 NamingContext 实现 —— 将 serviceName/groupName/clusters + instances 组合成
     * {@link NamingSelector#select(NamingContext)} 所需的上下文。
     */
    private class InnerNamingContext implements NamingContext {
        
        private List<Instance> instances;
        
        @Override
        public String getServiceName() {
            return serviceName;
        }
        
        @Override
        public String getGroupName() {
            return groupName;
        }
        
        @Override
        public String getClusters() {
            return clusters;
        }
        
        @Override
        public List<Instance> getInstances() {
            return instances;
        }
        
        private void setInstances(List<Instance> instances) {
            this.instances = instances;
        }
    }
    
    public NamingSelectorWrapper(NamingSelector selector, EventListener listener) {
        super(selector, new NamingListenerInvoker(listener));
    }
    
    public NamingSelectorWrapper(String serviceName, String groupName, String clusters,
        NamingSelector selector,
        EventListener listener) {
        this(selector, listener);
        this.serviceName = serviceName;
        this.groupName = groupName;
        this.clusters = clusters;
    }
    
    @Override
    protected boolean isSelectable(InstancesChangeEvent event) {
        return event != null && event.getHosts() != null && event.getInstancesDiff() != null;
    }
    
    @Override
    public boolean isCallable(NamingEvent event) {
        if (event == null) {
            return false;
        }
        NamingChangeEvent changeEvent = (NamingChangeEvent) event;
        return changeEvent.isAdded() || changeEvent.isRemoved() || changeEvent.isModified();
    }
    
    @Override
    protected NamingEvent buildListenerEvent(InstancesChangeEvent event) {
        List<Instance> currentIns = Collections.emptyList();
        if (CollectionUtils.isNotEmpty(event.getHosts())) {
            currentIns = doSelect(event.getHosts());
        }
        
        InstancesDiff diff = event.getInstancesDiff();
        InstancesDiff newDiff = new InstancesDiff();
        if (diff.isAdded()) {
            newDiff.setAddedInstances(doSelect(diff.getAddedInstances()));
        }
        if (diff.isRemoved()) {
            newDiff.setRemovedInstances(doSelect(diff.getRemovedInstances()));
        }
        if (diff.isModified()) {
            newDiff.setModifiedInstances(doSelect(diff.getModifiedInstances()));
        }
        
        return new NamingChangeEvent(serviceName, groupName, clusters, currentIns, newDiff);
    }
    
    private List<Instance> doSelect(List<Instance> instances) {
        NamingContext context = getNamingContext(instances);
        return this.getSelector().select(context).getResult();
    }
    
    private NamingContext getNamingContext(final List<Instance> instances) {
        namingContext.setInstances(instances);
        return namingContext;
    }
}
