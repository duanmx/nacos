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

package com.alibaba.nacos.client.naming.backups;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.cache.InstancesDiffer;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesDiff;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.spi.NacosServiceLoader;
import com.alibaba.nacos.api.utils.json.JsonUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Naming 客户端故障转移（Failover）反应器。
 *
 * <p>当 Nacos 服务端不可达时，客户端可以从本地磁盘读取预先缓存的 ServiceInfo，
 * 继续提供基本的服务发现能力，而不是直接报错或返回空列表。
 *
 * <h3>工作原理</h3>
 * <ol>
 *   <li>每 5 秒由 {@link FailoverSwitchRefresher} 轮询一次故障转移开关状态</li>
 *   <li>开关通过 SPI 加载的 {@link FailoverDataSource} 读取（默认实现为
 *       {@link com.alibaba.nacos.client.naming.backups.datasource.DiskFailoverDataSource}，
 *       从磁盘文件 {@code {cacheDir}/failover/} 读取）</li>
 *   <li>开关开启时：
 *       <ul>
 *         <li>从数据源读取故障转移数据（ServiceInfo），存入 {@link #serviceMap}</li>
 *         <li>对有变化的服务发布 {@link InstancesChangeEvent}，触发用户监听器回调</li>
 *         <li>NacosNamingService.selectInstances() 优先从 failover 数据返回实例</li>
 *       </ul>
 *   </li>
 *   <li>开关关闭时：
 *       <ul>
 *         <li>将 failover 数据与 {@link ServiceInfoHolder} 中的正常数据做 diff</li>
 *         <li>对有差异的服务发布 {@link InstancesChangeEvent}，让用户感知到"从 failover 恢复到正常"</li>
 *         <li>清空 {@link #serviceMap}，恢复正常服务发现流程</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h3>磁盘目录结构</h3>
 * <pre>
 *   {cacheDir}/failover/
 *   ├── 00-00---000-NACOS_SWITCH   ← 开关文件，内容 "1"=开启 "0"=关闭
 *   ├── {encodedServiceKey1}        ← 服务1的缓存文件（JSON 格式的 ServiceInfo）
 *   └── {encodedServiceKey2}        ← 服务2的缓存文件
 * </pre>
 *
 * <h3>调用链路</h3>
 * <pre>
 *   ServiceInfoHolder 构造函数
 *     └─ new FailoverReactor(this, scope)
 *          ├─ SPI 加载 FailoverDataSource → DiskFailoverDataSource
 *          ├─ 创建定时线程池
 *          └─ init() → 每 5s 执行 FailoverSwitchRefresher.run()
 *
 *   NacosNamingService.selectInstances()
 *     └─ if (serviceInfoHolder.isFailoverSwitch())
 *           └─ failoverReactor.getService(key)  ← 直接返回 failover 数据，不请求服务端
 * </pre>
 *
 * @author nkorange
 */
public class FailoverReactor implements Closeable {
    
    /**
     * 故障转移服务数据缓存：serviceKey → ServiceInfo。
     *
     * <p>仅在 failover 开启时有数据；关闭时被 clear。
     */
    private Map<String, ServiceInfo> serviceMap = new ConcurrentHashMap<>();
    
    /**
     * 当前故障转移开关是否开启。
     */
    private boolean failoverSwitchEnable;
    
    /**
     * 持有的 ServiceInfoHolder 引用，用于在 failover 关闭时获取正常缓存数据做 diff。
     */
    private final ServiceInfoHolder serviceInfoHolder;
    
    /**
     * 定时调度线程池，单线程，用于周期性轮询 failover 开关。
     */
    private final ScheduledExecutorService executorService;
    
    /**
     * 实例差异比较器，用于计算新旧 ServiceInfo 之间的实例变更。
     */
    private final InstancesDiffer instancesDiffer;
    
    /**
     * 故障转移数据源，通过 SPI 加载。
     * <p>默认实现：{@link com.alibaba.nacos.client.naming.backups.datasource.DiskFailoverDataSource}
     */
    private FailoverDataSource failoverDataSource;
    
    /**
     * 事件通知 scope，用于隔离不同 NamingService 实例的事件。
     * <p>与 {@link InstancesChangeEvent} 的 scope 对应，确保事件只被对应的 NamingService 实例消费。
     */
    private String notifierEventScope;
    
    /**
     * 构造 FailoverReactor。
     *
     * @param serviceInfoHolder   持有的 ServiceInfoHolder，用于获取正常缓存数据
     * @param notifierEventScope  事件 scope，用于隔离不同 NamingService 实例
     */
    public FailoverReactor(ServiceInfoHolder serviceInfoHolder, String notifierEventScope) {
        this.serviceInfoHolder = serviceInfoHolder;
        this.notifierEventScope = notifierEventScope;
        this.instancesDiffer = new InstancesDiffer();
        // SPI 加载 FailoverDataSource，默认为 DiskFailoverDataSource
        Collection<FailoverDataSource> dataSources =
            NacosServiceLoader.load(FailoverDataSource.class);
        for (FailoverDataSource dataSource : dataSources) {
            failoverDataSource = dataSource;
            NAMING_LOGGER.info("FailoverDataSource type is {}", dataSource.getClass());
            break;
        }
        // init executorService
        this.executorService = new ScheduledThreadPoolExecutor(1,
            new NameThreadFactory("com.alibaba.nacos.naming.failover"));
        this.init();
    }
    
    /**
     * 初始化定时任务：每 5 秒轮询一次 failover 开关状态。
     *
     * <p>使用 scheduleWithFixedDelay 而非 scheduleAtFixedRate，
     * 确保上一次轮询完成后才开始下一次（避免文件 IO 慢导致任务堆积）。
     */
    public void init() {
        executorService.scheduleWithFixedDelay(new FailoverSwitchRefresher(), 0L, 5000L,
            TimeUnit.MILLISECONDS);
    }
    
    /**
     * Failover 开关轮询任务。
     *
     * <p>每 5 秒执行一次，负责：
     * <ul>
     *   <li>检测 failover 开关状态变化</li>
     *   <li>开关开启时：从数据源加载 failover 数据，发布变更事件</li>
     *   <li>开关关闭时：将 failover 数据与正常数据做 diff，发布恢复事件，清空 failover 缓存</li>
     * </ul>
     */
    class FailoverSwitchRefresher implements Runnable {
        
        @Override
        public void run() {
            try {
                // 1. 从数据源读取当前 failover 开关状态
                FailoverSwitch fSwitch = failoverDataSource.getSwitch();
                if (fSwitch == null) {
                    // 数据源返回 null（如切换实现时），安全降级为关闭
                    failoverSwitchEnable = false;
                    return;
                }
                
                // 日志：仅在开关状态发生变化时打印
                if (fSwitch.getEnabled() != failoverSwitchEnable) {
                    NAMING_LOGGER.info("failover switch changed, new: {}", fSwitch.getEnabled());
                }
                
                // ================================================================
                // 分支 A：failover 开关已开启 → 加载/刷新 failover 数据
                // ================================================================
                if (fSwitch.getEnabled()) {
                    Map<String, ServiceInfo> failoverMap = new ConcurrentHashMap<>(200);
                    Map<String, FailoverData> failoverData = failoverDataSource.getFailoverData();
                    for (Map.Entry<String, FailoverData> entry : failoverData.entrySet()) {
                        ServiceInfo newService = (ServiceInfo) entry.getValue().getData();
                        // 取上一轮缓存的旧 failover 数据，用于 diff
                        ServiceInfo oldService = serviceMap.get(entry.getKey());
                        InstancesDiff diff = instancesDiffer.doDiff(oldService, newService);
                        // 有变化才通知用户监听器（避免无变化时频繁回调）
                        if (diff.hasDifferent()) {
                            NAMING_LOGGER.info(
                                "[NA] failoverdata isChangedServiceInfo. newService:{}",
                                JsonUtils.toJson(newService));
                            NotifyCenter.publishEvent(new InstancesChangeEvent(notifierEventScope,
                                newService.getName(),
                                newService.getGroupName(), newService.getClusters(),
                                newService.getHosts(), diff));
                        }
                        failoverMap.put(entry.getKey(), (ServiceInfo) entry.getValue().getData());
                    }
                    
                    // 替换 serviceMap（整批替换，避免并发读到半更新状态）
                    if (!failoverMap.isEmpty()) {
                        failoverServiceCntMetrics();
                        serviceMap = failoverMap;
                    }
                    
                    failoverSwitchEnable = true;
                    return;
                }
                
                // ================================================================
                // 分支 B：failover 开关从开启 → 关闭 → 恢复正常流程
                // ================================================================
                // 仅在之前是开启状态、现在变为关闭时才执行（避免每 5s 重复执行）
                if (failoverSwitchEnable && !fSwitch.getEnabled()) {
                    // 取正常缓存数据，与 failover 数据做 diff
                    // 目的：让用户感知"failover 数据 → 正常数据"的变化
                    Map<String, ServiceInfo> serviceInfoMap = serviceInfoHolder.getServiceInfoMap();
                    for (Map.Entry<String, ServiceInfo> entry : serviceMap.entrySet()) {
                        ServiceInfo oldService = entry.getValue();
                        ServiceInfo newService = serviceInfoMap.get(entry.getKey());
                        if (newService != null) {
                            InstancesDiff diff = instancesDiffer.doDiff(oldService, newService);
                            if (diff.hasDifferent()) {
                                NotifyCenter.publishEvent(
                                    new InstancesChangeEvent(notifierEventScope,
                                        newService.getName(),
                                        newService.getGroupName(), newService.getClusters(),
                                        newService.getHosts(), diff));
                            }
                        }
                    }
                    
                    // 清空 failover 缓存，恢复正常服务发现流程
                    serviceMap.clear();
                    failoverSwitchEnable = false;
                    failoverServiceCntMetricsClear();
                }
            } catch (Exception e) {
                // 捕获所有异常，防止定时任务因异常而停止
                NAMING_LOGGER.error("FailoverSwitchRefresher run err", e);
            }
        }
    }
    
    /**
     * 全局 failover 开关是否开启。
     *
     * <p>被 {@link ServiceInfoHolder#isFailoverSwitch()} 调用，
     * 最终被 NacosNamingService.selectInstances() 使用：
     * 若返回 true，则优先从 failover 数据返回实例，不请求服务端。
     *
     * @return true 表示 failover 已开启
     */
    public boolean isFailoverSwitch() {
        return failoverSwitchEnable;
    }
    
    /**
     * 针对特定服务的 failover 开关是否生效。
     *
     * <p>被 {@link ServiceInfoHolder#processServiceInfo} 调用：
     * 当服务端推送实例变更时，如果该服务正在 failover，则不发布 InstancesChangeEvent
     * （因为 failover 数据优先，服务端推送的数据应该被忽略）。
     *
     * @param serviceName 服务 key（groupName@@serviceName 格式）
     * @return true 表示该服务正在 failover 且有有效的 failover 数据
     */
    public boolean isFailoverSwitch(String serviceName) {
        ServiceInfo serviceInfo = serviceMap.get(serviceName);
        return failoverSwitchEnable && serviceInfo != null && serviceInfo.ipCount() > 0;
    }
    
    /**
     * 获取指定服务的 failover 数据。
     *
     * <p>被 {@link ServiceInfoHolder#getFailoverServiceInfo} 调用，
     * 最终被 NacosNamingService.getServiceInfoByFailover() 使用。
     *
     * @param key 服务 key（groupName@@serviceName 格式）
     * @return failover 中的 ServiceInfo；若不存在则返回空的 ServiceInfo（不返回 null，避免 NPE）
     */
    public ServiceInfo getService(String key) {
        ServiceInfo serviceInfo = serviceMap.get(key);
        
        if (serviceInfo == null) {
            serviceInfo = new ServiceInfo();
            serviceInfo.setName(key);
        }
        
        return serviceInfo;
    }
    
    /**
     * shutdown ThreadPool.
     *
     * @throws NacosException Nacos exception
     */
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
    
    /**
     * 注册 Micrometer Gauge 指标，监控每个 failover 服务的实例数量。
     *
     * <p>指标名：{@code nacos_naming_client_failover_instances}
     * <br>标签：{@code service_name={服务key}}
     * <p>仅在 failover 开启时注册，关闭时由 {@link #failoverServiceCntMetricsClear()} 移除。
     */
    private void failoverServiceCntMetrics() {
        for (Map.Entry<String, ServiceInfo> entry : serviceMap.entrySet()) {
            String serviceName = entry.getKey();
            List<Tag> tags = new ArrayList<>();
            tags.add(new ImmutableTag("service_name", serviceName));
            if (Metrics.globalRegistry.find("nacos_naming_client_failover_instances").tags(tags)
                .gauge() == null) {
                Gauge.builder("nacos_naming_client_failover_instances",
                    () -> serviceMap.get(serviceName).ipCount())
                    .tags(tags).register(Metrics.globalRegistry);
            }
        }
    }
    
    /**
     * 移除所有 failover 服务的 Micrometer Gauge 指标。
     *
     * <p>在 failover 开关从开启 → 关闭时调用，避免残留无效指标。
     */
    private void failoverServiceCntMetricsClear() {
        for (Map.Entry<String, ServiceInfo> entry : serviceMap.entrySet()) {
            Gauge gauge = Metrics.globalRegistry.find("nacos_naming_client_failover_instances")
                .tag("service_name", entry.getKey()).gauge();
            if (gauge != null) {
                Metrics.globalRegistry.remove(gauge);
            }
        }
    }
}
