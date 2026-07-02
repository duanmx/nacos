/*
 * Copyright 1999-2026 Alibaba Group Holding Ltd.
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

import com.alibaba.nacos.api.naming.pojo.ServiceInfo;

/**
 * ServiceInfo 磁盘缓存刷新事件 —— 触发异步写入磁盘文件的信号。
 *
 * <h2>核心职责</h2>
 * <p>携带需要写入磁盘的 ServiceInfo 快照 + 目标缓存目录，由
 * {@link ServiceInfoDiskCacheRefresher} 消费。event 本身只是一个数据传输对象（DTO），
 * 真正的磁盘写入由 ServiceInfoDiskCacheRefresher 的内部定时线程批量执行。</p>
 *
 * <h2>去抖机制</h2>
 * <p>同一 serviceKey 的新 event 会覆盖旧 event（通过
 * {@code pendingEvents.put(serviceKey, event)}），
 * 确保 100ms 内同一服务的多次推送只产生一次磁盘写入。</p>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>发布者</b>：ServiceInfoHolder.publishDiskCacheRefreshEvent() ——
 *       processServiceInfo() 中实例列表有变化时发布</li>
 *   <li><b>消费者</b>：ServiceInfoDiskCacheRefresher —— 100ms 定时扫描 pendingEvents，
 *       逐个调用 DiskCache.writeWithResult() 写入磁盘</li>
 * </ul>
 *
 * @author Zhengcy05
 */
public class ServiceInfoDiskCacheRefreshEvent {
    
    /**
     * 服务 key（groupName@@serviceName），不含 cluster 后缀。
     * <p>作为 pendingEvents Map 的 key，同 key 的事件会被覆盖（去抖）。</p>
     */
    private final String serviceKey;
    
    /**
     * 最新的 ServiceInfo 快照 —— 包含完整实例列表和 JSON 原始数据。
     * <p>磁盘写入时直接使用 getJsonFromServer() 序列化结果，
     * 避免重复 JSON 序列化。</p>
     */
    private final ServiceInfo serviceInfo;
    
    /**
     * 磁盘缓存目录路径 —— {base}/nacos/{registryDir}/naming/{namespace}。
     */
    private final String cacheDir;
    
    /**
     * Create a disk cache refresh event.
     *
     * @param serviceKey service key without clusters
     * @param serviceInfo latest service info snapshot
     * @param cacheDir disk cache directory
     */
    public ServiceInfoDiskCacheRefreshEvent(String serviceKey, ServiceInfo serviceInfo,
        String cacheDir) {
        this.serviceKey = serviceKey;
        this.serviceInfo = serviceInfo;
        this.cacheDir = cacheDir;
    }
    
    /**
     * Get service key.
     *
     * @return service key
     */
    public String getServiceKey() {
        return serviceKey;
    }
    
    /**
     * Get service info snapshot.
     *
     * @return service info snapshot
     */
    public ServiceInfo getServiceInfo() {
        return serviceInfo;
    }
    
    /**
     * Get disk cache directory.
     *
     * @return disk cache directory
     */
    public String getCacheDir() {
        return cacheDir;
    }
}
