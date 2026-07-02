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

import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.naming.selector.NamingSelectorWrapper;
import com.alibaba.nacos.client.selector.SelectorManager;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.listener.Subscriber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * 实例变更通知器 —— 接收 InstancesChangeEvent 并分发给用户注册的 EventListener。
 *
 * <p>相当于命名客户端的"事件路由器"。{@link InstancesChangeEvent} 被发布后，
 * NotifyCenter 根据 scope 匹配找到对应 Notifier，然后查找该 service 的所有
 * SelectorWrapper → 逐个调用 listener 回调。</p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   ServiceInfoHolder.processServiceInfo()
 *     → InstancesDiffer.doDiff() → 发现变化
 *     → NotifyCenter.publishEvent(InstancesChangeEvent)
 *       → InstancesChangeNotifier.onEvent()
 *         → selectorManager.getSelectorWrappers(subId)
 *           → for each NamingSelectorWrapper:
 *               → selectorWrapper.notifyListener(event)
 *                 → NamingListenerInvoker.invoke()
 *                   → EventListener.onEvent(NamingEvent)
 * }</pre>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>SelectorManager</b> —— 管理 service → SelectorWrapper 的映射关系</li>
 *   <li><b>NamingSelectorWrapper</b> —— 封装 EventListener + 集群选择器 + 回调线程调度</li>
 *   <li><b>NamingListenerInvoker</b> —— 实际执行用户回调，支持自定义 Executor</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：NacosNamingService 构造器</li>
 *   <li><b>消费</b>：接收 InstancesChangeEvent → 分发给所有注册的 EventListener</li>
 * </ul>
 *
 * @author horizonzy
 * @since 1.4.1
 */
public class InstancesChangeNotifier extends Subscriber<InstancesChangeEvent> {
    
    /** NamingService 实例 scope，用于事件隔离。 */
    private final String eventScope;
    
    /**
     * 选择器管理器 —— 管理 service → NamingSelectorWrapper 的映射，
     * 支持同一 service 注册多个 listener。
     */
    private final SelectorManager<NamingSelectorWrapper> selectorManager = new SelectorManager<>();
    
    @JustForTest
    public InstancesChangeNotifier() {
        this.eventScope = UUID.randomUUID().toString();
    }
    
    public InstancesChangeNotifier(String eventScope) {
        this.eventScope = eventScope;
    }
    
    /**
     * 注册监听器到指定 service。
     *
     * <p>调用方：NacosNamingService.tryToSubscribe() / doSubscribe()。
     *
     * <p>subId = groupedName(serviceName, groupName)，作为 SelectorManager 中的 key。
     *
     * @param groupName   组名
     * @param serviceName 服务名
     * @param wrapper     SelectorWrapper（包含 EventListener + 集群选择器）
     */
    public void registerListener(String groupName, String serviceName,
        NamingSelectorWrapper wrapper) {
        if (wrapper == null) {
            return;
        }
        String subId = NamingUtils.getGroupedName(serviceName, groupName);
        selectorManager.addSelectorWrapper(subId, wrapper);
    }
    
    /**
     * 注销监听器。
     *
     * <p>调用方：NacosNamingService.doUnsubscribe()。
     *
     * @param groupName   组名
     * @param serviceName 服务名
     * @param wrapper     之前注册的 SelectorWrapper
     */
    public void deregisterListener(String groupName, String serviceName,
        NamingSelectorWrapper wrapper) {
        if (wrapper == null) {
            return;
        }
        String subId = NamingUtils.getGroupedName(serviceName, groupName);
        selectorManager.removeSelectorWrapper(subId, wrapper);
    }
    
    /**
     * 检查是否已订阅指定 service。
     *
     * <p>调用方：NamingClientProxyDelegate.isSubscribed() → NacosNamingService。
     *
     * @param groupName   组名
     * @param serviceName 服务名
     * @return true 已订阅，false 未订阅
     */
    public boolean isSubscribed(String groupName, String serviceName) {
        String subId = NamingUtils.getGroupedName(serviceName, groupName);
        return selectorManager.isSubscribed(subId);
    }
    
    /**
     * 获取所有已订阅的服务列表。
     *
     * <p>调用方：NacosNamingService.getSubscribeServices() —— 运维查询用。
     */
    public List<ServiceInfo> getSubscribeServices() {
        List<ServiceInfo> serviceInfos = new ArrayList<>();
        for (String key : selectorManager.getSubscriptions()) {
            serviceInfos.add(ServiceInfo.fromKey(key));
        }
        return serviceInfos;
    }
    
    /**
     * 事件回调 —— NotifyCenter 分发 InstancesChangeEvent 时调用。
     *
     * <p>查找该 service 对应的所有 SelectorWrapper，逐个调用 notifyListener()。
     * 实际的用户回调（EventListener.onEvent()）在 NamingListenerInvoker 中执行，
     * 可能运行在用户指定的 Executor 上。</p>
     *
     * @param event 实例变更事件
     */
    @Override
    public void onEvent(InstancesChangeEvent event) {
        String subId = NamingUtils.getGroupedName(event.getServiceName(), event.getGroupName());
        Collection<NamingSelectorWrapper> selectorWrappers =
            selectorManager.getSelectorWrappers(subId);
        for (NamingSelectorWrapper selectorWrapper : selectorWrappers) {
            selectorWrapper.notifyListener(event);
        }
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return InstancesChangeEvent.class;
    }
    
    @Override
    public boolean scopeMatches(InstancesChangeEvent event) {
        return this.eventScope.equals(event.scope());
    }
}
