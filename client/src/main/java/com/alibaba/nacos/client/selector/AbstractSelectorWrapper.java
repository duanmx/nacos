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

import com.alibaba.nacos.api.selector.client.Selector;
import com.alibaba.nacos.common.notify.Event;

import java.util.Objects;

/**
 * 选择器封装基类 —— 将 Selector（过滤规则）与 ListenerInvoker（回调执行器）绑定在一起。
 *
 * <h2>核心职责</h2>
 * <p>作为 SelectorManager 管理的单元，封装了"事件 → 过滤 → 回调"的完整链路：</p>
 * <ol>
 *   <li><b>isSelectable(event)</b> —— 检查事件是否可被处理（如 event 非空、有有效 diff）</li>
 *   <li><b>buildListenerEvent(event)</b> —— 将原始事件转换为监听器能消费的事件类型</li>
 *   <li><b>isCallable(listenerEvent)</b> —— 检查转换后的事件是否有实际变更（避免空通知）</li>
 *   <li><b>notifyListener(event)</b> —— 标准通知路径（每次事件都通知）</li>
 *   <li><b>notifyIfListenerIfNotNotified(event)</b> —— 首次通知路径（仅 isInvoked=false 时通知，
 *       用于首次订阅时的初始同步，避免重复通知）</li>
 * </ol>
 *
 * <h2>类型参数</h2>
 * <ul>
 *   <li><b>S</b> —— Selector 类型（如 NamingSelector），定义过滤规则</li>
 *   <li><b>E</b> —— 监听器回调事件类型（如 NamingEvent）</li>
 *   <li><b>T</b> —— 原始事件类型（如 InstancesChangeEvent）</li>
 * </ul>
 *
 * <h2>equals/hashCode</h2>
 * <p>基于 selector + listener 的组合判断相等性，
 * 防止同一个 selector + listener 组合在 SelectorManager 中重复注册（ConcurrentHashSet 去重）。</p>
 *
 * @param <S> Selector 类型
 * @param <T> 原始事件类型
 * @param <E> 监听器回调事件类型
 * @author lideyou
 */
public abstract class AbstractSelectorWrapper<S extends Selector<?, ?>, E, T extends Event> {
    
    /**
     * 选择器（过滤规则）。
     */
    private final S selector;
    
    /**
     * 监听器调用器（封装回调执行逻辑）。
     */
    private final ListenerInvoker<E> listener;
    
    public AbstractSelectorWrapper(S selector, ListenerInvoker<E> listener) {
        this.selector = selector;
        this.listener = listener;
    }
    
    /**
     * Check whether the event can be callback.
     *
     * @param event original event
     * @return true if the event can be callback
     */
    protected abstract boolean isSelectable(T event);
    
    /**
     * Check whether the result can be callback.
     *
     * @param event select result
     * @return true if the result can be callback
     */
    protected abstract boolean isCallable(E event);
    
    /**
     * Build an event received by the listener.
     *
     * @param event original event
     * @return listener event
     */
    protected abstract E buildListenerEvent(T event);
    
    /**
     * Notify listener.
     *
     * @param event original event
     */
    public void notifyListener(T event) {
        if (!isSelectable(event)) {
            return;
        }
        E newEvent = buildListenerEvent(event);
        if (isCallable(newEvent)) {
            // lock listener to make sure isInvoked is thread safe.
            synchronized (listener) {
                listener.invoke(newEvent);
            }
        }
    }
    
    /**
     * Notify listener If the listener is not invoked.
     *
     * @param event original event
     */
    public void notifyIfListenerIfNotNotified(T event) {
        if (!isSelectable(event)) {
            return;
        }
        E newEvent = buildListenerEvent(event);
        synchronized (listener) {
            if (!listener.isInvoked()) {
                listener.invoke(newEvent);
            }
        }
    }
    
    public ListenerInvoker<E> getListener() {
        return this.listener;
    }
    
    public S getSelector() {
        return this.selector;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractSelectorWrapper<?, ?, ?> that = (AbstractSelectorWrapper<?, ?, ?>) o;
        return Objects.equals(selector, that.selector) && Objects.equals(listener, that.listener);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(selector, listener);
    }
}
