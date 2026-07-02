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

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Naming 客户端 ServiceInfo 磁盘缓存异步刷新器。
 *
 * <p>采用 <b>批处理 + 去抖（Batch + Debounce）</b> 模式，将高频的磁盘写入操作
 * 合并为低频的批量写入，避免每次实例变更都同步刷盘导致 IO 瓶颈。
 *
 * <h3>设计动机</h3>
 * <p>服务端推送实例变更时，ServiceInfoHolder.processServiceInfo() 会更新内存缓存
 * 并发布 InstancesChangeEvent。如果不做异步化，磁盘写入（JSON 序列化 + 文件 IO）
 * 会阻塞推送处理线程，导致后续推送被延迟。
 *
 * <h3>工作流程</h3>
 * <pre>
 *   服务端推送 ServiceInfo
 *     └─ ServiceInfoHolder.processServiceInfo()
 *          └─ if (diff.hasDifferent())                     ← 仅实例有变化才刷盘
 *               └─ publishDiskCacheRefreshEvent(key, info)
 *                    └─ refresher.publishEvent(event)       ← 存入 pendingEvents（去抖）
 *                         │  同一 serviceKey 的新事件会覆盖旧事件
 *                         │  → 100ms 内多次推送只写最后一次
 *                         │
 *    ┌─────────────────────┘
 *    │
 *    │  定时线程每 100ms 执行一次：
 *    │  ┌─ safeFlushPendingEvents()
 *    │  │   └─ flushPendingEvents()
 *    │  │        └─ for each pendingEvent:
 *    │  │             ├─ DiskCache.writeWithResult(serviceInfo, cacheDir)
 *    │  │             │    → 序列化为 JSON → 写入 {cacheDir}/{encodedServiceKey} 文件
 *    │  │             └─ 写入成功 → 从 pendingEvents 移除
 *    │  │                写入失败 → 保留在 pendingEvents，下次再试
 *    │  └─ （异常被 catch，不影响下次执行）
 *    │
 *    │  客户端关闭时：
 *    │  shutdown() → flushPendingEvents() → 等待线程池终止(最多3s) → 再 flush 一次
 * </pre>
 *
 * <h3>磁盘文件结构</h3>
 * <pre>
 *   {cacheDir}/
 *   ├── DEFAULT_GROUP%40%40order-service   ← URL编码后的 serviceKey 作为文件名
 *   │     内容：{"name":"order-service","groupName":"DEFAULT_GROUP","hosts":[...]}
 *   └── DEFAULT_GROUP%40%40user-service
 *         内容：{"name":"user-service","groupName":"DEFAULT_GROUP","hosts":[...]}
 * </pre>
 *
 * <h3>与其他组件的关系</h3>
 * <ul>
 *   <li>被 {@link ServiceInfoHolder} 持有，在构造时创建</li>
 *   <li>写入委托给 {@link DiskCache}（通过 {@link DiskCacheWriter} 函数式接口）</li>
 *   <li>写入的文件在客户端重启时由 {@link DiskCache#read} 读取（当 namingLoadCacheAtStart=true）</li>
 *   <li>这些文件也是 Failover 的数据来源（运维可复制到 failover 目录）</li>
 * </ul>
 *
 * @author Zhengcy05
 */
public class ServiceInfoDiskCacheRefresher implements Closeable {
    
    /**
     * 默认刷盘间隔：100 毫秒。
     *
     * <p>意味着同一服务在 100ms 内的多次推送最多只产生一次磁盘写入。
     */
    static final long DEFAULT_FLUSH_INTERVAL_MILLISECONDS = 100L;
    
    /**
     * 默认关闭超时：3 秒。
     *
     * <p>客户端 shutdown 时等待刷盘线程完成的最长时间，超时后打印警告但不强制中断。
     */
    static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLISECONDS = 3000L;
    
    private static final String REFRESHER_THREAD_NAME =
        "com.alibaba.nacos.client.naming.disk.cache.refresher";
    
    /**
     * 待刷盘事件队列：serviceKey → 最新事件。
     *
     * <p>使用 {@link ConcurrentHashMap} 保证多线程安全。
     * <p>key 是 serviceKey（groupName@@serviceName），value 是最新的 {@link ServiceInfoDiskCacheRefreshEvent}。
     * <p>同一 serviceKey 的多次 {@link #publishEvent} 会覆盖，实现去抖：100ms 内只有最后一次推送被写入磁盘。
     */
    private final ConcurrentMap<String, ServiceInfoDiskCacheRefreshEvent> pendingEvents;
    
    /**
     * 定时刷盘线程池，单线程。
     *
     * <p>使用 {@link ScheduledThreadPoolExecutor} + scheduleWithFixedDelay，
     * 确保上一次刷盘完成后才开始下一次（避免文件 IO 慢导致任务堆积）。
     */
    private final ScheduledThreadPoolExecutor refreshExecutor;
    
    /**
     * 磁盘写入器，默认委托给 {@link DiskCache#writeWithResult}。
     *
     * <p>通过函数式接口注入，方便测试时 mock（不写真实文件）。
     */
    private final DiskCacheWriter diskCacheWriter;
    
    /**
     * 关闭时的等待超时时间（毫秒）。
     */
    private final long shutdownTimeoutMilliseconds;
    
    /**
     * Create a disk cache refresher with default flush and shutdown settings.
     */
    public ServiceInfoDiskCacheRefresher() {
        this(DEFAULT_FLUSH_INTERVAL_MILLISECONDS, DEFAULT_SHUTDOWN_TIMEOUT_MILLISECONDS,
            DiskCache::writeWithResult);
    }
    
    /**
     * Create a disk cache refresher for tests or custom runtime settings.
     * This constructor keeps the production constructor simple while allowing deterministic tests.
     *
     * @param flushIntervalMilliseconds flush interval in milliseconds
     * @param shutdownTimeoutMilliseconds shutdown wait timeout in milliseconds
     * @param diskCacheWriter writer used to persist disk cache
     */
    ServiceInfoDiskCacheRefresher(long flushIntervalMilliseconds, long shutdownTimeoutMilliseconds,
        DiskCacheWriter diskCacheWriter) {
        this.pendingEvents = new ConcurrentHashMap<>(16);
        this.refreshExecutor = new ScheduledThreadPoolExecutor(1,
            new NameThreadFactory(REFRESHER_THREAD_NAME));
        this.diskCacheWriter = diskCacheWriter;
        this.shutdownTimeoutMilliseconds = shutdownTimeoutMilliseconds;
        // 启动定时刷盘任务：每隔 flushIntervalMilliseconds 执行一次
        this.refreshExecutor.scheduleWithFixedDelay(this::safeFlushPendingEvents,
            flushIntervalMilliseconds, flushIntervalMilliseconds, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 发布磁盘缓存刷新事件。
     *
     * <p>由 ServiceInfoHolder.publishDiskCacheRefreshEvent() 调用。
     * 将事件存入 {@link #pendingEvents}，同一 serviceKey 的新事件覆盖旧事件（去抖），
     * 实际写入由定时线程在下一个刷盘周期执行。
     *
     * @param event 包含 serviceKey、ServiceInfo 快照、cacheDir 的刷新事件
     */
    public void publishEvent(ServiceInfoDiskCacheRefreshEvent event) {
        pendingEvents.put(event.getServiceKey(), event);
    }
    
    /**
     * 立即刷盘（主要用于测试）。
     *
     * <p>同步执行一次 {@link #safeFlushPendingEvents}，不等待定时调度。
     */
    void flushNow() {
        safeFlushPendingEvents();
    }
    
    /**
     * Get pending refresh event size.
     *
     * @return pending refresh event size
     */
    int pendingEventSize() {
        return pendingEvents.size();
    }
    
    /**
     * Check whether refresher executor has been shutdown.
     *
     * @return {@code true} if shutdown, otherwise {@code false}
     */
    boolean isShutdown() {
        return refreshExecutor.isShutdown();
    }
    
    /**
     * 安全刷盘：包装 {@link #flushPendingEvents}，捕获所有异常防止定时任务停止。
     *
     * <p>ScheduledThreadPoolExecutor 的 scheduleWithFixedDelay 如果抛出未捕获异常，
     * 后续任务会被取消。这里用 try-catch 兜底，确保即使刷盘失败，定时任务仍继续运行。
     */
    private void safeFlushPendingEvents() {
        try {
            flushPendingEvents();
        } catch (Throwable e) {
            NAMING_LOGGER.error("[NA] failed to flush service info disk cache refresh event", e);
        }
    }
    
    /**
     * 遍历所有待刷盘事件，逐个写入磁盘。
     *
     * <p>写入成功的从 {@link #pendingEvents} 移除；写入失败的保留，下次刷盘周期重试。
     * <p>使用 {@code remove(key, value)} 而非 {@code remove(key)}，确保移除的是本次处理的事件，
     * 而非在处理期间被新事件覆盖后的新事件（CAS 语义）。
     */
    private void flushPendingEvents() {
        for (String serviceKey : pendingEvents.keySet()) {
            ServiceInfoDiskCacheRefreshEvent event = pendingEvents.get(serviceKey);
            if (null == event) {
                continue;
            }
            // 委托给 DiskCacheWriter 写入磁盘
            boolean writeResult =
                diskCacheWriter.write(event.getServiceInfo(), event.getCacheDir());
            if (writeResult) {
                // 写入成功 → 移除（CAS：仅当 value 仍是本次处理的 event 时才移除）
                pendingEvents.remove(serviceKey, event);
            }
            // 写入失败 → 保留在 pendingEvents，下个周期重试
        }
    }
    
    /**
     * 关闭刷新器：先刷盘剩余事件，再关闭线程池，最后再刷一次确保无遗漏。
     *
     * <p>关闭流程：
     * <ol>
     *   <li>同步刷盘一次（把 pendingEvents 中的事件尽量写入磁盘）</li>
     *   <li>关闭线程池（停止定时任务）</li>
     *   <li>等待线程池终止，最多等待 {@link #shutdownTimeoutMilliseconds} 毫秒</li>
     *   <li>超时则打印警告（不强制中断，避免数据损坏）</li>
     *   <li>再刷一次（防止等待期间有新事件进入）</li>
     * </ol>
     *
     * @throws NacosException if interrupted during shutdown
     */
    @Override
    public void shutdown() throws NacosException {
        // 1. 关闭前先刷一次盘，尽量不丢数据
        flushPendingEvents();
        // 2. 停止定时任务
        refreshExecutor.shutdown();
        try {
            // 3. 等待线程池终止（最多等 shutdownTimeoutMilliseconds）
            if (!refreshExecutor.awaitTermination(shutdownTimeoutMilliseconds,
                TimeUnit.MILLISECONDS)) {
                NAMING_LOGGER.warn("[NA] timeout while waiting service info disk cache refresher "
                    + "to shutdown, pending event size: {}", pendingEvents.size());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NacosException(NacosException.CLIENT_DISCONNECT,
                "Interrupted while shutting down service info disk cache refresher", e);
        }
        // 4. 最后再刷一次，防止 awaitTermination 期间有新事件进入
        flushPendingEvents();
    }
    
    /**
     * 磁盘写入器函数式接口。
     *
     * <p>默认实现：{@link DiskCache#writeWithResult}
     * <p>将 ServiceInfo 序列化为 JSON，通过 {@link com.alibaba.nacos.client.utils.ConcurrentDiskUtil}
     * 并发安全地写入 {@code {cacheDir}/{encodedServiceKey}} 文件。
     *
     * <p>测试时可通过构造函数注入 mock 实现，避免真实文件 IO。
     */
    @FunctionalInterface
    interface DiskCacheWriter {
        
        /**
         * Write service info to disk cache.
         *
         * @param serviceInfo service info
         * @param cacheDir cache dir
         * @return {@code true} if write success, otherwise {@code false}
         */
        boolean write(ServiceInfo serviceInfo, String cacheDir);
    }
}
