/*
 * Copyright 1999-2025 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.redo.service;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.redo.data.RedoData;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.remote.client.Connection;
import com.alibaba.nacos.common.remote.client.ConnectionEventListener;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 抽象重做服务 —— 在服务端断连时管理注册/注销操作的重试。
 *
 * <h2>核心职责</h2>
 * <p>作为 ConnectionEventListener 监听 gRPC 连接状态变化：
 * <ul>
 *   <li><b>onDisConnect</b> —— 连接断开时，将所有 RedoData 标记为 registered=false，
 *       启动 RedoScheduledTask 周期遍历 redoDataMap，通过 getRedoType() 判断是否需要重做</li>
 *   <li><b>onConnected</b> —— 连接恢复时，标记 connected=true，RedoScheduledTask 恢复执行重做</li>
 *   <li><b>缓存管理</b> —— 提供 cachedRedoData/removeRedoData/dataRegistered/dataDeregister 等方法，
 *       管理 Class → key → RedoData 的三级映射</li>
 * </ul>
 * </p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   注册操作：
 *   NamingGrpcClientProxy.registerInstance()
 *     → grpcClient.request(InstanceRequest)  // 发送 gRPC 请求
 *       ├─ 成功 → redoService.dataRegistered(key, clazz)
 *       └─ 失败 → 不标记 registered，等待 RedoScheduledTask 重试
 *
 *   断连重做：
 *   gRPC 连接断开 → onDisConnect() → 标记所有 RedoData.registered = false
 *     → RedoScheduledTask
 *    .run()
 *       → findRedoData(clazz) → 过滤 isNeedRedo()=true 的数据
 *         → getRedoType() == REGISTER → redoRegister()
 *         → getRedoType() == UNREGISTER → redoDeregister()
 *         → getRedoType() == REMOVE → removeRedoData(key, clazz)
 * }</pre>
 *
 * <h2>子类</h2>
 * <ul>
 *   <li>NamingGrpcRedoService —— Naming 模块重做服务</li>
 *   <li>ConfigGrpcRedoService —— Config 模块重做服务</li>
 *   <li>AiGrpcRedoService —— AI 模块重做服务</li>
 * </ul>
 *
 * @author xiweng.yy
 */
public abstract class AbstractRedoService implements ConnectionEventListener, Closeable {
    
    private static final String REDO_THREAD_NAME_PATTERN = "com.alibaba.nacos.client.%s.redo";
    
    private final Logger logger;
    
    /**
     * 定时重做线程池 —— 按 redoDelayTime 周期执行 buildRedoTask()。
     */
    private final ScheduledExecutorService redoExecutor;
    
    /**
     * 重做数据的三级映射：数据 Class → key → RedoData。
     * <p>外层 key 是 RedoData 泛型的实际类型（如 Instance.class），
     * 内层 key 是业务的唯一标识（如 serviceKey）。</p>
     */
    private final Map<Class<?>, Map<String, RedoData<?>>> redoDataMap;
    
    /**
     * 重做线程数，默认 1。
     */
    private int redoThreadCount;
    
    /**
     * 重做间隔时间（毫秒），默认 3000ms。
     */
    private long redoDelayTime;
    
    /**
     * gRPC 连接状态 —— true 表示已连接，RedoScheduledTask 仅在此状态下执行重做。
     */
    private volatile boolean connected = false;
    
    protected AbstractRedoService(Logger logger, NacosClientProperties properties, String module) {
        this.logger = logger;
        setProperties(properties);
        this.redoExecutor = new ScheduledThreadPoolExecutor(redoThreadCount,
            new NameThreadFactory(String.format(REDO_THREAD_NAME_PATTERN, module)));
        this.redoDataMap = new ConcurrentHashMap<>(2);
    }
    
    private void setProperties(NacosClientProperties properties) {
        redoDelayTime = properties.getLong(PropertyKeyConst.REDO_DELAY_TIME,
            Constants.DEFAULT_REDO_DELAY_TIME);
        redoThreadCount = properties.getInteger(PropertyKeyConst.REDO_DELAY_THREAD_COUNT,
            Constants.DEFAULT_REDO_THREAD_COUNT);
    }
    
    protected void startRedoTask() {
        this.redoExecutor.scheduleWithFixedDelay(buildRedoTask(), redoDelayTime, redoDelayTime,
            TimeUnit.MILLISECONDS);
    }
    
    /**
     * Build redo task to do redo work.
     *
     * @return redo task
     */
    protected abstract AbstractRedoTask buildRedoTask();
    
    @Override
    public void onConnected(Connection connection) {
        connected = true;
        logger.info("Grpc connection connect");
    }
    
    @Override
    public void onDisConnect(Connection connection) {
        connected = false;
        logger.warn("Grpc connection disconnect, mark to redo");
        for (Class<?> each : redoDataMap.keySet()) {
            Map<String, RedoData<?>> actualRedoData = this.redoDataMap.get(each);
            synchronized (actualRedoData) {
                actualRedoData.values().forEach(redoData -> redoData.setRegistered(false));
            }
        }
        logger.warn("mark to redo completed");
    }
    
    @Override
    public void shutdown() {
        logger.info("Shutdown grpc redo service executor {}", redoExecutor);
        redoDataMap.clear();
        redoExecutor.shutdownNow();
    }
    
    public boolean isConnected() {
        return connected;
    }
    
    /**
     * Cache the redo data by class and redo data key.
     *
     * @param key       key of redo data
     * @param redoData  the redo data
     * @param clazz     clazz of stored in {@link RedoData}.
     */
    public <T> void cachedRedoData(String key, RedoData<T> redoData, Class<T> clazz) {
        Map<String, RedoData<?>> actualRedoData = this.redoDataMap.computeIfAbsent(clazz,
            k -> new ConcurrentHashMap<>(2));
        synchronized (actualRedoData) {
            actualRedoData.put(key, redoData);
        }
    }
    
    /**
     * Remove data for redo.
     *
     * @param key       key of redo data
     * @param clazz     clazz of stored in {@link RedoData}.
     */
    public <T> void removeRedoData(String key, Class<T> clazz) {
        Map<String, RedoData<?>> actualRedoData = this.redoDataMap.computeIfAbsent(clazz,
            k -> new ConcurrentHashMap<>(2));
        synchronized (actualRedoData) {
            RedoData<?> redoData = actualRedoData.get(key);
            if (null != redoData && !redoData.isExpectedRegistered()) {
                actualRedoData.remove(key);
            }
        }
    }
    
    /**
     * Data register successfully, mark registered status as {@code true}.
     *
     * @param key   key of redo data
     * @param clazz clazz of stored in {@link RedoData}.
     */
    public <T> void dataRegistered(String key, Class<T> clazz) {
        Map<String, RedoData<?>> actualRedoData = this.redoDataMap.computeIfAbsent(clazz,
            k -> new ConcurrentHashMap<>(2));
        synchronized (actualRedoData) {
            RedoData<?> redoData = actualRedoData.get(key);
            if (null != redoData) {
                redoData.registered();
            }
        }
    }
    
    /**
     * Data deregister, mark unregistering status as {@code true}.
     *
     * @param key   key of redo data
     * @param clazz clazz of stored in {@link RedoData}.
     */
    public <T> void dataDeregister(String key, Class<T> clazz) {
        Map<String, RedoData<?>> actualRedoData = this.redoDataMap.computeIfAbsent(clazz,
            k -> new ConcurrentHashMap<>(2));
        synchronized (actualRedoData) {
            RedoData<?> redoData = actualRedoData.get(key);
            if (null != redoData) {
                redoData.setUnregistering(true);
                redoData.setExpectedRegistered(false);
            }
        }
    }
    
    /**
     * Data deregister finished, mark unregistering status as {@code true}.
     *
     * @param key   key of redo data
     * @param clazz clazz of stored in {@link RedoData}.
     */
    public <T> void dataDeregistered(String key, Class<T> clazz) {
        Map<String, RedoData<?>> actualRedoData = this.redoDataMap.computeIfAbsent(clazz,
            k -> new ConcurrentHashMap<>(2));
        synchronized (actualRedoData) {
            RedoData<?> redoData = actualRedoData.get(key);
            if (null != redoData) {
                redoData.unregistered();
            }
        }
    }
    
    /**
     * Judge data has registered to server.
     *
     * @param key   key of redo data
     * @param clazz clazz of stored in {@link RedoData}.
     * @return {@code true} if registered, otherwise {@code false}
     */
    public boolean isDataRegistered(String key, Class<?> clazz) {
        Map<String, RedoData<?>> actualRedoData = this.redoDataMap.computeIfAbsent(clazz,
            k -> new ConcurrentHashMap<>(2));
        synchronized (actualRedoData) {
            RedoData<?> redoData = actualRedoData.get(key);
            return null != redoData && redoData.isRegistered();
        }
    }
    
    /**
     * Find all redo data which need to do redo.
     *
     * @return set of {@link RedoData} need to do redo.
     */
    public <T> Set<RedoData<T>> findRedoData(Class<T> clazz) {
        Set<RedoData<T>> result = new HashSet<>();
        Map<String, RedoData<?>> actualRedoData = this.redoDataMap.computeIfAbsent(clazz,
            k -> new ConcurrentHashMap<>(2));
        synchronized (actualRedoData) {
            for (RedoData<?> each : actualRedoData.values()) {
                if (each.isNeedRedo()) {
                    result.add((RedoData<T>) each);
                }
            }
        }
        return result;
    }
    
    /**
     * get Cache redo data.
     *
     * @param key   key of redo data
     * @param clazz clazz of stored in {@link RedoData}.
     * @return cache redo data
     */
    public <T> RedoData<T> getRedoData(String key, Class<?> clazz) {
        Map<String, RedoData<?>> actualRedoData = this.redoDataMap.computeIfAbsent(clazz,
            k -> new ConcurrentHashMap<>(2));
        synchronized (actualRedoData) {
            return (RedoData<T>) actualRedoData.get(key);
        }
    }
}
