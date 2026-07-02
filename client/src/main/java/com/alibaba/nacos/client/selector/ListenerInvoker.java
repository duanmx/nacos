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

/**
 * 监听器调用器接口 —— 封装用户回调的调用逻辑。
 *
 * <h2>核心职责</h2>
 * <p>将具体模块（Naming/Config/AI）的 EventListener 调用逻辑抽象为统一接口：
 * <ul>
 *   <li><b>invoke(event)</b> —— 调用用户的回调方法（支持同步/异步）</li>
 *   <li><b>isInvoked()</b> —— 判断是否已至少调用过一次，
 *       用于首次订阅时只做初始通知、后续增量跳过的去重逻辑</li>
 * </ul>
 * </p>
 *
 * <h2>典型实现</h2>
 * <ul>
 *   <li>NamingListenerInvoker —— 封装 EventListener，支持
 *       AbstractEventListener.getExecutor() 异步执行</li>
 * </ul>
 *
 * @param <E> 回调事件类型
 * @author lideyou
 */
public interface ListenerInvoker<E> {
    
    /**
     * 调用内部 listener。
     *
     * @param event 事件对象
     */
    void invoke(E event);
    
    /**
     * 标记该 listener 是否已至少被调用一次。
     * 一旦 invoke() 被调用过一次，此后应始终返回 true。
     *
     * @return true 表示已至少调用过一次
     */
    boolean isInvoked();
}
