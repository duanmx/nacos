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

package com.alibaba.nacos.client.selector;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 选择器管理器 —— 维护订阅 ID → SelectorWrapper 集合的映射。
 *
 * <h2>核心职责</h2>
 * <p>作为 SelectorWrapper（绑定 selector + listener）的注册表：
 * <ul>
 *   <li><b>subId 粒度</b> —— 以订阅 ID（serviceKey）为维度组织 SelectorWrapper 集合，
 *       一个服务可以被多个 selector + listener 组合订阅</li>
 *   <li><b>addSelectorWrapper</b> —— 注册 selector（NacosNamingService.subscribe() 时调用）</li>
 *   <li><b>removeSelectorWrapper</b> —— 移除 selector（NacosNamingService.unsubscribe() 时调用）</li>
 *   <li><b>getSelectorWrappers</b> —— 批量获取（InstancesChangeNotifier.onEvent() 中获取所有相关 wrapper
 *       逐一调用 notifyListener 分发事件）</li>
 * </ul>
 * </p>
 *
 * <h2>使用场景</h2>
 * <pre>{@code
 *   // 注册
 *   selectorManager.addSelectorWrapper("DEFAULT_GROUP@@my-service", wrapper);
 *
 *   // 事件分发
 *   InstancesChangeNotifier.onEvent(InstancesChangeEvent event)
 *     → Set<NamingSelectorWrapper> wrappers = selectorManager.getSelectorWrappers(subId);
 *       → for each wrapper: wrapper.notifyListener(event);
 * </pre>
 *
 * <h2>线程安全</h2>
 * <p>selectorMap 使用 ConcurrentHashMap，内部 Set 使用 ConcurrentHashSet，
 * 保证多线程环境下的 add/remove/get 操作安全。</p>
 *
 * @param <S> SelectorWrapper 子类型（如 NamingSelectorWrapper）
 * @author lideyou
 */
public class SelectorManager<S extends AbstractSelectorWrapper<?, ?, ?>> {
    
    /**
     * 订阅 ID → SelectorWrapper 集合映射。
     * <p>key = subId（如 "DEFAULT_GROUP@@my-service"），value = 该服务上注册的所有 selector+listener 组合。</p>
     */
    Map<String, Set<S>> selectorMap = new ConcurrentHashMap<>();
    
    /**
     * Add a selectorWrapper to subId.
     *
     * @param subId   subscription id
     * @param wrapper selector wrapper
     */
    public void addSelectorWrapper(String subId, S wrapper) {
        selectorMap.compute(subId, (k, v) -> {
            if (v == null) {
                v = new ConcurrentHashSet<>();
            }
            v.add(wrapper);
            return v;
        });
    }
    
    /**
     * Get all SelectorWrappers by id.
     *
     * @param subId subscription id
     * @return the set of SelectorWrappers
     */
    public Set<S> getSelectorWrappers(String subId) {
        return selectorMap.getOrDefault(subId, Collections.emptySet());
    }
    
    /**
     * Remove a SelectorWrapper by id.
     *
     * @param subId   subscription id
     * @param wrapper selector wrapper
     */
    public void removeSelectorWrapper(String subId, S wrapper) {
        selectorMap.computeIfPresent(subId, (k, v) -> {
            v.remove(wrapper);
            return v.isEmpty() ? null : v;
        });
    }
    
    /**
     * Remove a subscription by id.
     *
     * @param subId subscription id
     */
    public void removeSubscription(String subId) {
        selectorMap.remove(subId);
    }
    
    /**
     * Get all subscriptions.
     *
     * @return all subscriptions
     */
    public Set<String> getSubscriptions() {
        return selectorMap.keySet();
    }
    
    /**
     * Determine whether subId is subscribed.
     *
     * @param subId subscription id
     * @return true if is subscribed
     */
    public boolean isSubscribed(String subId) {
        return CollectionUtils.isNotEmpty(this.getSelectorWrappers(subId));
    }
}
