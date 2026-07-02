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
import com.alibaba.nacos.api.naming.selector.NamingContext;
import com.alibaba.nacos.api.naming.selector.NamingResult;
import com.alibaba.nacos.api.naming.selector.NamingSelector;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 默认命名选择器 —— 基于 Predicate 过滤实例列表。
 *
 * <h2>核心职责</h2>
 * <p>实现 NamingSelector 接口，通过构造时传入的 Predicate&lt;Instance&gt; 过滤实例列表。
 * 是 NamingSelectorFactory 创建的集群选择器、IP 选择器、元数据选择器的底层实现。</p>
 *
 * <h2>典型用法</h2>
 * <pre>{@code
 *   // 健康实例选择器
 *   NamingSelector healthy = new DefaultNamingSelector(Instance::isHealthy);
 *   // 空选择器（不过滤任何实例）
 *   NamingSelector all = context -> context::getInstances;
 * }</pre>
 *
 * @author lideyou
 */
public class DefaultNamingSelector implements NamingSelector {
    
    /**
     * 实例过滤器 —— 满足此条件的实例才被保留。
     */
    private final Predicate<Instance> filter;
    
    public DefaultNamingSelector(Predicate<Instance> filter) {
        this.filter = filter;
    }
    
    @Override
    public NamingResult select(NamingContext context) {
        List<Instance> instances = doFilter(context.getInstances());
        return () -> instances;
    }
    
    private List<Instance> doFilter(List<Instance> instances) {
        return instances == null ? Collections.emptyList()
            : instances.stream().filter(filter).collect(Collectors.toList());
    }
}
