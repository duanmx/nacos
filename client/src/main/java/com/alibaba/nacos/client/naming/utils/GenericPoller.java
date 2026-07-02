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

package com.alibaba.nacos.client.naming.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 通用轮询器 —— 基于原子计数器实现 Round-Robin 轮询。
 *
 * <h2>核心职责</h2>
 * <p>实现 Poller 接口，通过 AtomicInteger 原子递增 + 取模的方式
 * 在元素列表中循环选择下一个元素，保证多线程安全和负载均匀分布。</p>
 *
 * <h2>使用场景</h2>
 * <p>被 Chooser.Ref 内部创建，在加权随机选择的基础上提供 Round-Robin 兜底选择。</p>
 *
 * @param <T> 元素类型
 * @author nkorange
 */
public class GenericPoller<T> implements Poller<T> {
    
    /**
     * 原子计数器 —— 每次 next() 递增，取模实现循环。
     */
    private final AtomicInteger index = new AtomicInteger(0);
    
    /**
     * 元素列表 —— refresh() 时创建新列表，不修改旧列表。
     */
    private List<T> items = new ArrayList<>();
    
    public GenericPoller(List<T> items) {
        this.items = items;
    }
    
    @Override
    public T next() {
        return items.get(Math.abs(index.getAndIncrement() % items.size()));
    }
    
    @Override
    public Poller<T> refresh(List<T> items) {
        return new GenericPoller<>(items);
    }
}
