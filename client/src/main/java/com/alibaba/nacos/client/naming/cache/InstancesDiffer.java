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

package com.alibaba.nacos.client.naming.cache;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.event.InstancesDiff;
import com.alibaba.nacos.api.utils.json.JsonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 服务实例差异计算器 —— 对比新旧两个 ServiceInfo，识别新增/移除/修改的实例。
 *
 * <p>相当于命名客户端的"diff 引擎"（差异检测引擎）。每次收到服务端推送的新 ServiceInfo
 * 时，与本地缓存中旧的 ServiceInfo 做 O(N) 对比，输出三类变化：新增实例、移除实例、
 * 修改实例（IP:port 相同但其他属性变化）。</p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   ServiceInfoHolder.processServiceInfo() 收到新 ServiceInfo
 *     → 从 serviceInfoMap 取出旧 ServiceInfo
 *     → InstancesDiffer.doDiff(oldService, newService)
 *         ├─ null oldService → 全部视为新实例（首次收到）
 *         ├─ oldService.lastRefTime > newService.lastRefTime → 拒绝过期数据
 *         └─ 正常对比：
 *             ├─ 新有旧无 → addedInstances
 *             ├─ 旧有新无 → removedInstances
 *             └─ 新旧都有但 toString 不同 → modifiedInstances
 *     → 返回 InstancesDiff → 发布 InstancesChangeEvent
 * }</pre>
 *
 * <h2>对比算法（O(N)）</h2>
 * <ol>
 *   <li>构建旧实例 {@code Map<ip:port, Instance>}</li>
 *   <li>构建新实例 {@code Map<ip:port, Instance>}</li>
 *   <li>遍历新实例：key 不在旧 Map → 新增；key 在旧 Map 但值不同 → 修改</li>
 *   <li>遍历旧实例：key 不在新 Map → 移除</li>
 * </ol>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>ServiceInfoHolder.processServiceInfo()</b> —— 唯一调用方，每次收到服务端推送后调用</li>
 *   <li><b>InstancesDiff</b> —— 差异结果载体，包含 addedInstances / removedInstances / modifiedInstances</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <p>本类由 ServiceInfoHolder 在 processServiceInfo() 中临时创建（每次 diff 时 new 一个），
 * 无全局状态，用完即销毁。</p>
 *
 * @author xiweng.yy
 */
public final class InstancesDiffer {
    
    /**
     * 对比新旧 ServiceInfo，计算实例变更差异。
     *
     * <p>调用方：ServiceInfoHolder.processServiceInfo() L319 → 唯一调用点，
     * 每次收到服务端推送的新实例列表时调用。</p>
     *
     * <h3>边界情况</h3>
     * <ul>
     *   <li><b>null oldService</b>：首次收到该服务的数据 → 全部标记为新增</li>
     *   <li><b>过期数据</b>：oldService.lastRefTime &gt; newService.lastRefTime
     *       → 返回空 InstancesDiff（拒绝更新），防止网络乱序导致的回退</li>
     *   <li><b>无变化</b>：返回空 InstancesDiff，不发事件</li>
     * </ul>
     *
     * @param oldService 本地缓存中的旧 ServiceInfo（可能为 null）
     * @param newService 服务端推送的新 ServiceInfo
     * @return InstancesDiff（added + removed + modified 三类变化）
     */
    public InstancesDiff doDiff(ServiceInfo oldService, ServiceInfo newService) {
        InstancesDiff instancesDiff = new InstancesDiff();
        // 边界 1: 首次收到该服务的数据 → 全部标记为新增
        if (null == oldService) {
            NAMING_LOGGER.info("init new ips({}) service: {} -> {}", newService.ipCount(),
                newService.getKey(),
                JsonUtils.toJson(newService.getHosts()));
            instancesDiff.setAddedInstances(newService.getHosts());
            return instancesDiff;
        }
        // 边界 2: 过期数据保护 —— 拒绝旧版本覆盖新版本
        if (oldService.getLastRefTime() > newService.getLastRefTime()) {
            NAMING_LOGGER.warn("out of date data received, old-t: {}, new-t: {}",
                oldService.getLastRefTime(),
                newService.getLastRefTime());
            return instancesDiff;
        }
        
        // Step 1: 构建 ip:port → Instance 索引 Map（O(N)）
        Map<String, Instance> oldHostMap = new HashMap<>(oldService.getHosts().size());
        for (Instance host : oldService.getHosts()) {
            oldHostMap.put(host.toInetAddr(), host);
        }
        Map<String, Instance> newHostMap = new HashMap<>(newService.getHosts().size());
        for (Instance host : newService.getHosts()) {
            newHostMap.put(host.toInetAddr(), host);
        }
        
        Set<Instance> modHosts = new HashSet<>();
        Set<Instance> newHosts = new HashSet<>();
        Set<Instance> remvHosts = new HashSet<>();
        
        // Step 2: 遍历新实例 → 识别"新增"和"修改"
        List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<>(newHostMap.entrySet());
        for (Map.Entry<String, Instance> entry : newServiceHosts) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            // 新旧都有但 toString 不同 → 修改（如 weight/healthy 等属性变化）
            if (oldHostMap.containsKey(key)
                && !StringUtils.equals(host.toString(), oldHostMap.get(key).toString())) {
                modHosts.add(host);
                continue;
            }
            // 新有旧无 → 新增
            if (!oldHostMap.containsKey(key)) {
                newHosts.add(host);
            }
        }
        
        // Step 3: 遍历旧实例 → 识别"移除"
        for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (newHostMap.containsKey(key)) {
                continue;
            }
            // 旧有新无 → 移除
            remvHosts.add(host);
        }
        
        // Step 4: 设置三类差异结果
        if (!newHosts.isEmpty()) {
            NAMING_LOGGER.info("new ips({}) service: {} -> {}", newHosts.size(),
                newService.getKey(),
                JsonUtils.toJson(newHosts));
            instancesDiff.setAddedInstances(newHosts);
        }
        
        if (!remvHosts.isEmpty()) {
            NAMING_LOGGER.info("removed ips({}) service: {} -> {}", remvHosts.size(),
                newService.getKey(),
                JsonUtils.toJson(remvHosts));
            instancesDiff.setRemovedInstances(remvHosts);
        }
        
        if (!modHosts.isEmpty()) {
            NAMING_LOGGER.info("modified ips({}) service: {} -> {}", modHosts.size(),
                newService.getKey(),
                JsonUtils.toJson(modHosts));
            instancesDiff.setModifiedInstances(modHosts);
        }
        return instancesDiff;
    }
}
