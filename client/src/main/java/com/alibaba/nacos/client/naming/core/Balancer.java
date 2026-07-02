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

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.utils.Chooser;
import com.alibaba.nacos.client.naming.utils.Pair;
import com.alibaba.nacos.common.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 服务实例负载均衡器 —— 基于权重的随机选择算法。
 *
 * <p>Nacos 客户端的内置负载均衡实现。调用 {@code selectOneHealthyInstance()} 时，
 * 先过滤健康实例 → 按权重随机选择（{@link Chooser} 实现加权随机算法）。</p>
 *
 * <h2>算法</h2>
 * <ol>
 *   <li>过滤健康实例（isHealthy()=true 且 weight &gt; 0）</li>
 *   <li>构建 Pair<Instance, weight> 列表</li>
 *   <li>Chooser.refresh() 构建累积权重数组</li>
 *   <li>Chooser.randomWithWeight() 按权重随机选择一个实例</li>
 * </ol>
 *
 * <h2>调用方</h2>
 * <ul>
 *   <li>NacosNamingService.selectOneHealthyInstance() —— 用户业务调用</li>
 *   <li>内部被调用链：selectInstances() → 过滤 → getHostByRandomWeight()</li>
 * </ul>
 *
 * @author xuanyin
 */
public class Balancer {
    
    /**
     * 基于权重的随机选择器（内部类 —— 命名空间隔离）。
     */
    public static class RandomByWeight {
        
        /**
         * 获取所有实例（不做过滤）。
         *
         * @param serviceInfo 服务信息
         * @return 所有实例列表
         * @throws IllegalStateException 实例列表为空时
         */
        public static List<Instance> selectAll(ServiceInfo serviceInfo) {
            List<Instance> hosts = serviceInfo.getHosts();
            if (CollectionUtils.isEmpty(hosts)) {
                throw new IllegalStateException(
                    "no host to srv for serviceInfo: " + serviceInfo.getName());
            }
            return hosts;
        }
        
        /**
         * 从 service 中按权重随机选择一个健康实例。
         *
         * @param dom 服务信息
         * @return 选中的实例，无健康实例时返回 null
         */
        public static Instance selectHost(ServiceInfo dom) {
            return getHostByRandomWeight(selectAll(dom));
        }
    }
    
    /**
     * 从实例列表中按权重随机选择一个健康实例。
     *
     * <p>只过滤健康实例（isHealthy()=true），然后通过 {@link Chooser#randomWithWeight()}
     * 按权重随机选取。权重越高被选中的概率越大。</p>
     *
     * @param hosts 完整实例列表（可能包含不健康实例）
     * @return 选中的实例，无健康实例时返回 null
     */
    protected static Instance getHostByRandomWeight(List<Instance> hosts) {
        NAMING_LOGGER.debug("entry randomWithWeight");
        if (hosts == null || hosts.size() == 0) {
            NAMING_LOGGER.debug("hosts == null || hosts.size() == 0");
            return null;
        }
        // Step 1: 过滤健康实例 + 构建权重列表
        NAMING_LOGGER.debug("new Chooser");
        List<Pair<Instance>> hostsWithWeight = new ArrayList<>();
        for (Instance host : hosts) {
            if (host.isHealthy()) {
                hostsWithWeight.add(new Pair<Instance>(host, host.getWeight()));
            }
        }
        // Step 2: 构建累积权重数组 → 加权随机选择
        NAMING_LOGGER.debug("for (Host host : hosts)");
        Chooser<String, Instance> vipChooser = new Chooser<>("www.taobao.com");
        vipChooser.refresh(hostsWithWeight);
        NAMING_LOGGER.debug("vipChooser.refresh");
        return vipChooser.randomWithWeight();
    }
}
