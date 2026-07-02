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

import com.alibaba.nacos.api.naming.listener.AbstractEventListener;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.client.selector.ListenerInvoker;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Naming 监听器调用器 —— 封装 EventListener 的调用逻辑，支持异步执行。
 *
 * <h2>核心职责</h2>
 * <p>实现 ListenerInvoker 接口，负责安全地调用用户的 EventListener：</p>
 * <ul>
 *   <li><b>invoke(NamingEvent)</b> —— 标记已调用 + 打印日志 + 发起回调</li>
 *   <li><b>异步执行支持</b> —— 如果 listener 是 AbstractEventListener 且提供了自定义 Executor，
 *       则将回调提交到用户指定的线程池执行；否则在当前线程同步执行</li>
 *   <li><b>isInvoked()</b> —— 标记是否已被调用过，防止重复通知（与 AbstractSelectorWrapper 配合）</li>
 *   <li><b>equals/hashCode</b> —— 基于 listener 的 identity 判断相等性，
 *       确保同一个 listener 注册多次时能被正确去重</li>
 * </ul>
 *
 * <h2>异步执行的意义</h2>
 * <p>如果用户提供了自定义 Executor，则事件回调在用户线程池中执行，
 * 避免长时间的回调阻塞 Nacos 的推送/拉取处理线程。</p>
 *
 * @author lideyou
 */
public class NamingListenerInvoker implements ListenerInvoker<NamingEvent> {
    
    /**
     * 用户注册的 EventListener 实例。
     */
    private final EventListener listener;
    
    /**
     * 调用标记 —— true 表示该 listener 已被通知过。
     * <p>与 AbstractSelectorWrapper.onEvent() 中 isInvoked 检查配合，
     * 防止每次 diff 都重复通知（如首次订阅时初始同步 + 后续增量的去重）。</p>
     */
    private final AtomicBoolean invoked = new AtomicBoolean(false);
    
    public NamingListenerInvoker(EventListener listener) {
        this.listener = listener;
    }
    
    @Override
    public void invoke(NamingEvent event) {
        invoked.set(true);
        logInvoke(event);
        if (listener instanceof AbstractEventListener
            && ((AbstractEventListener) listener).getExecutor() != null) {
            ((AbstractEventListener) listener).getExecutor().execute(() -> listener.onEvent(event));
        } else {
            listener.onEvent(event);
        }
    }
    
    private void logInvoke(NamingEvent event) {
        NAMING_LOGGER.info("Invoke event groupName: {}, serviceName: {} to Listener: {}",
            event.getGroupName(),
            event.getServiceName(), listener.toString());
    }
    
    @Override
    public boolean isInvoked() {
        return invoked.get();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        
        if (this == o) {
            return true;
        }
        
        NamingListenerInvoker that = (NamingListenerInvoker) o;
        return Objects.equals(listener, that.listener);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(listener);
    }
}
