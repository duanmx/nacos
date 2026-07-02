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
import com.alibaba.nacos.api.naming.selector.NamingSelector;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * 命名选择器工厂 —— 创建各种内置 NamingSelector 实例的静态工厂类。
 *
 * <h2>核心职责</h2>
 * <p>提供常用的实例选择器创建方法，避免用户手动构造 Predicate：</p>
 * <ul>
 *   <li><b>EMPTY_SELECTOR</b> —— 空选择器，返回全部实例</li>
 *   <li><b>HEALTHY_SELECTOR</b> —— 健康实例选择器，只返回 isHealthy=true 的实例</li>
 *   <li><b>newClusterSelector(clusters)</b> —— 按集群筛选，匹配指定的 cluster 名称</li>
 *   <li><b>newIpSelector(regex)</b> —— 按 IP 正则筛选</li>
 *   <li><b>newMetadataSelector(metadata, isAny)</b> —— 按元数据筛选，
 *       isAny=false 时所有条件必须满足（AND），isAny=true 时满足任一条件即可（OR）</li>
 * </ul>
 *
 * <h2>使用方</h2>
 * <p>NamingSelectorWrapper 在注册事件监听器时调用这些工厂方法创建 selector，
 * 用户也可以通过 NamingSelector 接口自定义选择器实现。</p>
 *
 * @author lideyou
 */
public final class NamingSelectorFactory {
    
    /**
     * 空选择器 —— 不做任何过滤，返回所有实例。
     */
    public static final NamingSelector EMPTY_SELECTOR = context -> context::getInstances;
    
    /**
     * 健康实例选择器 —— 只保留 isHealthy=true 的实例。
     */
    public static final NamingSelector HEALTHY_SELECTOR =
        new DefaultNamingSelector(Instance::isHealthy);
    
    /**
     * 集群选择器 —— 按 cluster 名称筛选实例，内部记录 clusterString 用于 equals/hashCode 去重。
     */
    private static class ClusterSelector extends DefaultNamingSelector {
        
        private final String clusterString;
        
        public ClusterSelector(Predicate<Instance> filter, String clusterString) {
            super(filter);
            this.clusterString = clusterString;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClusterSelector that = (ClusterSelector) o;
            return Objects.equals(this.clusterString, that.clusterString);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(this.clusterString);
        }
    }
    
    private NamingSelectorFactory() {
    }
    
    /**
     * Create a cluster selector.
     *
     * @param clusters target cluster
     * @return cluster selector
     */
    public static NamingSelector newClusterSelector(Collection<String> clusters) {
        if (CollectionUtils.isNotEmpty(clusters)) {
            final Set<String> set = new HashSet<>(clusters);
            Predicate<Instance> filter = instance -> set.contains(instance.getClusterName());
            String clusterString = getUniqueClusterString(clusters);
            return new ClusterSelector(filter, clusterString);
        } else {
            return EMPTY_SELECTOR;
        }
    }
    
    /**
     * Create a IP selector.
     *
     * @param regex regular expression of IP
     * @return IP selector
     */
    public static NamingSelector newIpSelector(String regex) {
        if (regex == null) {
            throw new IllegalArgumentException("The parameter 'regex' cannot be null.");
        }
        return new DefaultNamingSelector(instance -> Pattern.matches(regex, instance.getIp()));
    }
    
    /**
     * Create a metadata selector.
     *
     * @param metadata metadata that needs to be matched
     * @return metadata selector
     */
    public static NamingSelector newMetadataSelector(Map<String, String> metadata) {
        return newMetadataSelector(metadata, false);
    }
    
    /**
     * Create a metadata selector.
     *
     * @param metadata target metadata
     * @param isAny    true if any of the metadata needs to be matched, false if all the metadata need to be matched.
     * @return metadata selector
     */
    public static NamingSelector newMetadataSelector(Map<String, String> metadata, boolean isAny) {
        if (metadata == null) {
            throw new IllegalArgumentException("The parameter 'metadata' cannot be null.");
        }
        
        Predicate<Instance> filter = instance -> instance.getMetadata().size() >= metadata.size();
        
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            Predicate<Instance> nextFilter = instance -> {
                Map<String, String> map = instance.getMetadata();
                return Objects.equals(map.get(entry.getKey()), entry.getValue());
            };
            if (isAny) {
                filter = filter.or(nextFilter);
            } else {
                filter = filter.and(nextFilter);
            }
        }
        return new DefaultNamingSelector(filter);
    }
    
    public static String getUniqueClusterString(Collection<String> cluster) {
        TreeSet<String> treeSet = new TreeSet<>(cluster);
        return StringUtils.join(treeSet, ",");
    }
    
}
