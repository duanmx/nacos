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

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * 服务信息定时更新服务。
 *
 * <p>作用：当用户 subscribe 了某个服务后，即使服务端没有主动推送，客户端也会周期性地向服务端拉取最新实例列表，
 * 作为推送机制的兜底保障。
 *
 * <p>核心机制：每个被订阅的服务对应一个 {@link UpdateTask}，由 {@link ScheduledExecutorService} 定时调度执行。
 * 任务之间互相独立，通过 serviceKey 隔离。
 *
 * <p>注意：默认不启用，需要通过配置 {@code nacos.naming.async.query.subscribe.service=true} 开启。
 * 启用后，subscribe 时会调用 {@link #scheduleUpdateIfAbsent} 启动定时拉取任务。
 *
 * @author xiweng.yy
 */
public class ServiceInfoUpdateService implements Closeable {
    
    /** 默认首次延迟：1 秒后开始第一次拉取 */
    private static final long DEFAULT_DELAY = 1000L;
    
    /** 拉取间隔倍数：服务端返回的 cacheMillis × 6 = 实际拉取间隔 */
    private static final int DEFAULT_UPDATE_CACHE_TIME_MULTIPLE = 6;
    
    /** 最少线程数 */
    private static final int MIN_THREAD_NUM = 1;
    
    /** serviceKey → 定时任务的映射，管理每个服务的拉取任务 */
    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<>();
    
    private final ServiceInfoHolder serviceInfoHolder;
    
    /** 定时调度器，负责周期性执行 UpdateTask */
    private final ScheduledExecutorService executor;
    
    /** Naming 客户端代理，用于向服务端查询实例列表 */
    private final NamingClientProxy namingClientProxy;
    
    /** 事件通知器，用于判断某服务是否还在被订阅 */
    private final InstancesChangeNotifier changeNotifier;
    
    /** 是否启用异步查询（默认 false，需要配置开启） */
    private final boolean asyncQuerySubscribeService;
    
    public ServiceInfoUpdateService(NacosClientProperties properties,
        ServiceInfoHolder serviceInfoHolder,
        NamingClientProxy namingClientProxy, InstancesChangeNotifier changeNotifier) {
        this.asyncQuerySubscribeService = isAsyncQueryForSubscribeService(properties);
        this.executor = new ScheduledThreadPoolExecutor(initPollingThreadCount(properties),
            new NameThreadFactory("com.alibaba.nacos.client.naming.updater"));
        this.serviceInfoHolder = serviceInfoHolder;
        this.namingClientProxy = namingClientProxy;
        this.changeNotifier = changeNotifier;
    }
    
    /**
     * 判断是否启用异步查询订阅服务。
     *
     * <p>配置项：{@code nacos.naming.async.query.subscribe.service}
     * 默认 false —— Nacos 2.x+ 主要靠 gRPC 服务端主动推送，定时拉取只是兜底。
     */
    private boolean isAsyncQueryForSubscribeService(NacosClientProperties properties) {
        if (properties == null
            || !properties.containsKey(PropertyKeyConst.NAMING_ASYNC_QUERY_SUBSCRIBE_SERVICE)) {
            return false;
        }
        return ConvertUtils.toBoolean(
            properties.getProperty(PropertyKeyConst.NAMING_ASYNC_QUERY_SUBSCRIBE_SERVICE),
            false);
    }
    
    /**
     * 计算拉取任务的线程池大小。
     *
     * <p>默认值 = CPU 核心数 / 2，可通过配置覆盖：
     * <ul>
     *   <li>{@code nacos.naming.polling.max.thread.count} —— 最大线程数上限</li>
     *   <li>{@code nacos.naming.polling.thread.count} —— 精确指定线程数</li>
     * </ul>
     */
    private int initPollingThreadCount(NacosClientProperties properties) {
        int count = ThreadUtils.getSuitableThreadCount(1) > 1
            ? ThreadUtils.getSuitableThreadCount(1) / 2 : 1;
        if (properties == null) {
            return count;
        }
        count = Math.min(
            properties.getInteger(PropertyKeyConst.NAMING_POLLING_MAX_THREAD_COUNT, count),
            count);
        count = Math.max(count, MIN_THREAD_NUM);
        return properties.getInteger(PropertyKeyConst.NAMING_POLLING_THREAD_COUNT, count);
    }
    
    /**
     * 为指定服务启动定时拉取任务（如果尚未启动）。
     *
     * <p>使用双重检查锁（DCL）确保同一服务只创建一个定时任务：
     * <ol>
     *   <li>第一次检查（无锁）：快速判断，避免不必要的 synchronized</li>
     *   <li>synchronized 加锁</li>
     *   <li>第二次检查（有锁）：防止并发创建重复任务</li>
     * </ol>
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void scheduleUpdateIfAbsent(String serviceName, String groupName, String clusters) {
        // 未启用异步查询 → 直接返回
        if (!asyncQuerySubscribeService) {
            return;
        }
        String serviceKey =
            ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        // 第一次检查（无锁）
        if (futureMap.get(serviceKey) != null) {
            return;
        }
        synchronized (futureMap) {
            // 第二次检查（有锁）—— 防止并发创建重复任务
            if (futureMap.get(serviceKey) != null) {
                return;
            }
            
            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, groupName, clusters));
            futureMap.put(serviceKey, future);
        }
    }
    
    /**
     * 提交一个拉取任务到调度器，延迟 1 秒后首次执行。
     */
    private synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 停止指定服务的定时拉取任务（如果存在）。
     *
     * <p>同样使用双重检查锁，从 futureMap 中移除任务。
     * 注意：移除后已提交的任务仍会执行一次，但在 run() 中会检测到 futureMap 不含该 key 从而自动取消。
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void stopUpdateIfContain(String serviceName, String groupName, String clusters) {
        String serviceKey =
            ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (!futureMap.containsKey(serviceKey)) {
            return;
        }
        synchronized (futureMap) {
            if (!futureMap.containsKey(serviceKey)) {
                return;
            }
            futureMap.remove(serviceKey);
        }
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
    
    /**
     * 服务实例定时拉取任务。
     *
     * <p>每个被订阅的服务对应一个 UpdateTask，由 {@link ScheduledExecutorService} 周期性调度执行。
     * 每次执行后会在 finally 中重新调度自身，形成自循环定时拉取。
     *
     * <p>拉取策略：
     * <ul>
     *   <li>本地缓存为空 → 立即向服务端查询</li>
     *   <li>本地缓存有但 lastRefTime 没变 → 可能服务端推送丢失，主动查询</li>
     *   <li>查询失败 → 递增 failCount，下次延迟加倍（指数退避，上限 60 秒）</li>
     * </ul>
     */
    public class UpdateTask implements Runnable {
        
        /** 上次拉取到数据的服务端时间戳，用于判断本地缓存是否过期 */
        long lastRefTime = Long.MAX_VALUE;
        
        /** 是否已取消（取消后不再重新调度） */
        private boolean isCancel;
        
        private final String serviceName;
        
        private final String groupName;
        
        private final String clusters;
        
        /** 组合后的服务名：groupName@@serviceName */
        private final String groupedServiceName;
        
        /** 服务唯一标识：groupedServiceName + clusters */
        private final String serviceKey;
        
        /**
         * 连续失败次数。失败场景：1.连不上服务端 2.返回的 hosts 为空。
         * 用于指数退避：delayTime << failCount，最大延迟 60 秒。
         */
        private int failCount = 0;
        
        public UpdateTask(String serviceName, String groupName, String clusters) {
            this.serviceName = serviceName;
            this.groupName = groupName;
            this.clusters = clusters;
            this.groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
            this.serviceKey = ServiceInfo.getKey(groupedServiceName, clusters);
        }
        
        @Override
        public void run() {
            // 默认延迟时间，后续可能根据服务端返回值调整
            long delayTime = DEFAULT_DELAY;
            
            try {
                // 1. 检查是否已取消订阅 —— 如果用户已经 unsubscribe，任务自动终止
                if (!changeNotifier.isSubscribed(groupName, serviceName) && !futureMap.containsKey(
                    serviceKey)) {
                    NAMING_LOGGER.info("update task is stopped, service:{}, clusters:{}",
                        groupedServiceName, clusters);
                    isCancel = true;
                    return;
                }
                
                // 2. 查本地缓存
                ServiceInfo serviceObj = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
                
                // 2a. 本地缓存为空 → 第一次拉取，立即向服务端查询
                if (serviceObj == null) {
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName,
                        clusters, false);
                    serviceInfoHolder.processServiceInfo(serviceObj);  // 更新缓存 + 发布事件
                    delayTime = serviceObj.getCacheMillis() * DEFAULT_UPDATE_CACHE_TIME_MULTIPLE;
                    lastRefTime = serviceObj.getLastRefTime();
                    return;
                }
                
                // 2b. 本地缓存有，但服务端的 lastRefTime 没变 → 可能推送丢失，主动查询
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName,
                        clusters, false);
                    serviceInfoHolder.processServiceInfo(serviceObj);
                }
                lastRefTime = serviceObj.getLastRefTime();
                
                // 3. 检查结果是否为空 —— 空列表视为失败
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                
                // 4. 成功 → 用服务端返回的 cacheMillis 计算下次拉取间隔，重置失败计数
                delayTime = serviceObj.getCacheMillis() * DEFAULT_UPDATE_CACHE_TIME_MULTIPLE;
                resetFailCount();
            } catch (NacosException e) {
                handleNacosException(e);
            } catch (Throwable e) {
                handleUnknownException(e);
            } finally {
                // 5. 重新调度自身（除非已取消）
                //    指数退避：delayTime << failCount（每次失败延迟翻倍）
                //    上限：DEFAULT_DELAY * 60 = 60 秒
                if (!isCancel) {
                    executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60),
                        TimeUnit.MILLISECONDS);
                }
            }
        }
        
        /**
         * 处理 Nacos 业务异常 —— 递增失败计数，服务端错误按未知异常处理。
         */
        private void handleNacosException(NacosException e) {
            incFailCount();
            int errorCode = e.getErrCode();
            if (NacosException.SERVER_ERROR == errorCode) {
                handleUnknownException(e);
            }
            NAMING_LOGGER.warn("Can't update serviceName: {}, reason: {}", groupedServiceName,
                e.getErrMsg());
        }
        
        /**
         * 处理未知异常（网络断开等）—— 递增失败计数。
         */
        private void handleUnknownException(Throwable throwable) {
            incFailCount();
            NAMING_LOGGER.warn("[NA] failed to update serviceName: {}", groupedServiceName,
                throwable);
        }
        
        /**
         * 递增失败计数，上限 6 次（防止指数退避延迟过大）。
         */
        private void incFailCount() {
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }
        
        /**
         * 重置失败计数为 0 —— 拉取成功后调用。
         */
        private void resetFailCount() {
            failCount = 0;
        }
    }
}
