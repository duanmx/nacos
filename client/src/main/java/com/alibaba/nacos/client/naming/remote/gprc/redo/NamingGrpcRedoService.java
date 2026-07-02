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

package com.alibaba.nacos.client.naming.remote.gprc.redo;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.naming.cache.NamingFuzzyWatchServiceListHolder;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.BatchInstanceRedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.InstanceRedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.SubscriberRedoData;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.remote.client.Connection;
import com.alibaba.nacos.common.remote.client.ConnectionEventListener;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Nacos gRPC 命名客户端 Redo（重做）服务 —— 断连重连后自动补偿未完成的操作。
 *
 * <p>该类扮演两个角色：
 * <ol>
 *   <li><b>ConnectionEventListener</b>：监听 gRPC 连接的建立与断开，断连时将
 *       所有缓存的注册/订阅标记为"未完成"，等待重做</li>
 *   <li><b>Redo 数据仓库</b>：维护两份 ConcurrentMap 缓存，分别管理
 *       实例注册数据（InstanceRedoData）和订阅数据（SubscriberRedoData），
 *       通过 RedoScheduledTask 定时扫描需要重做的条目</li>
 * </ol>
 *
 * <h2>ASCII 数据流转全景图</h2>
 * <pre>{@code
 *   NamingGrpcClientProxy                         RpcClient (gRPC 连接)
 *          │                                              │
 *          │  注册/注销/订阅/取消订阅                          │  onConnected()
 *          ▼                                              │  onDisConnect()
 *   cacheInstanceForRedo()  ──►  registeredInstances ◄─── redoService ◄────
 *   cacheSubscriberForRedo() ──►  subscribes                    │
 *   instanceRegistered() ──► 标记 registered=true              │
 *   instanceDeregister() ──► 标记 unregistering=true           │
 *          │                                              │
 *          ▼                                              ▼
 *   [定时器触发] RedoScheduledTask.run()
 *          │
 *          ├── findInstanceRedoData() ──► 遍历 registeredInstances
 *          │        │                         ↓ 每个 InstanceRedoData
 *          │        └── getRedoType() ──► REGISTER / UNREGISTER / REMOVE / NONE
 *          │               │
 *          │               ├── REGISTER → clientProxy.doRegisterService() 或 doBatchRegisterService()
 *          │               ├── UNREGISTER → clientProxy.doDeregisterService()
 *          │               └── REMOVE → removeInstanceForRedo()
 *          │
 *          └── findSubscriberRedoData() ──► 遍历 subscribes
 *                   │                         ↓ 每个 SubscriberRedoData
 *                   └── getRedoType() ──► REGISTER / UNREGISTER / REMOVE / NONE
 *                          │
 *                          ├── REGISTER → clientProxy.doSubscribe()
 *                          ├── UNREGISTER → clientProxy.doUnsubscribe()
 *                          └── REMOVE → removeSubscriberForRedo()
 * }</pre>
 *
 * <h2>RedoData 状态机（由父类 RedoData 定义）</h2>
 * <p>每个 RedoData 有三个核心布尔字段，共同决定是否需要重做以及重做类型：</p>
 * <table>
 *   <tr><th>registered</th><th>unregistering</th><th>expectedRegistered</th>
 *        <th>getRedoType()</th><th>含义</th></tr>
 *   <tr><td>true</td><td>false</td><td>true</td><td>NONE</td>
 *        <td>正常注册状态，无需重做</td></tr>
 *   <tr><td>true</td><td>false</td><td>false</td><td>UNREGISTER</td>
 *        <td>已注册但期望注销，需重做注销</td></tr>
 *   <tr><td>true</td><td>true</td><td>-</td><td>UNREGISTER</td>
 *        <td>正在注销中，需继续重做注销</td></tr>
 *   <tr><td>false</td><td>false</td><td>-</td><td>REGISTER</td>
 *        <td>未注册（断连丢失），需重做注册</td></tr>
 *   <tr><td>false</td><td>true</td><td>true</td><td>REGISTER</td>
 *        <td>期望注册但标记了注销，需重做注册</td></tr>
 *   <tr><td>false</td><td>true</td><td>false</td><td>REMOVE</td>
 *        <td>期望注销且已完成，可移除缓存</td></tr>
 * </table>
 *
 * <h2>核心协作者</h2>
 * <ul>
 *   <li><b>NamingGrpcClientProxy</b> — 直接调用方，所有注册/注销/订阅操作都通过
 *       它缓存到 redo；RedoScheduledTask 也通过它执行实际的重做请求</li>
 *   <li><b>RedoScheduledTask</b> — 定时任务（ScheduledExecutorService），
 *       周期性扫描需要重做的数据并调用 clientProxy 重试</li>
 *   <li><b>RpcClient</b> — 将 this 注册为 ConnectionEventListener，
 *       在 gRPC 连接断开/恢复时触发回调</li>
 *   <li><b>NamingFuzzyWatchServiceListHolder</b> — 断连时重置模糊监听一致性状态</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建</b>：NamingGrpcClientProxy 构造器 L261 —
 *       {@code new NamingGrpcRedoService(this, namingFuzzyWatchServiceListHolder, properties)}</li>
 *   <li><b>注册为 ConnectionEventListener</b>：NamingGrpcClientProxy.start() L229 —
 *       {@code rpcClient.registerConnectionListener(redoService)}</li>
 *   <li><b>定时重做</b>：构造器内通过 scheduleWithFixedDelay 启动 RedoScheduledTask</li>
 *   <li><b>关闭</b>：NamingGrpcClientProxy.shutdown() L856 —
 *       {@code redoService.shutdown()}</li>
 * </ul>
 *
 * @author xiweng.yy
 * @see com.alibaba.nacos.client.redo.data.RedoData
 * @see RedoScheduledTask
 * @see com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy
 */
public class NamingGrpcRedoService implements ConnectionEventListener {
    
    /** 定时重做线程名，可通过 jstack 识别。 */
    private static final String REDO_THREAD_NAME = "com.alibaba.nacos.client.naming.grpc.redo";
    
    /**
     * 重做线程池大小，来自属性 PropertyKeyConst.REDO_DELAY_THREAD_COUNT，
     * 默认 Constants.DEFAULT_REDO_THREAD_COUNT（1）。
     */
    private int redoThreadCount;
    
    /**
     * 重做间隔时间（毫秒），来自属性 PropertyKeyConst.REDO_DELAY_TIME，
     * 默认 Constants.DEFAULT_REDO_DELAY_TIME（3000ms）。
     */
    private long redoDelayTime;
    
    /**
     * 已注册实例缓存。key = groupedServiceName (group@@service)，
     * value = InstanceRedoData（单实例）或 BatchInstanceRedoData（批量）。
     * NamingGrpcClientProxy 通过 getRegisteredInstances() 直接操作此 Map 实现批量注销的差集计算。
     */
    private final ConcurrentMap<String, InstanceRedoData> registeredInstances =
        new ConcurrentHashMap<>();
    
    /**
     * 订阅者缓存。key = ServiceInfo.getKey(groupedName, cluster)，
     * value = SubscriberRedoData。
     */
    private final ConcurrentMap<String, SubscriberRedoData> subscribes = new ConcurrentHashMap<>();
    
    /** 模糊监听服务列表持有者，断连时重置一致性状态。 */
    private final NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder;
    
    /** 定时重做执行器，运行 RedoScheduledTask。 */
    private final ScheduledExecutorService redoExecutor;
    
    /**
     * 连接状态标志（volatile 保证线程可见性）。
     * RedoScheduledTask 在执行重做前检查此标志，断连期间跳过重做。
     */
    private volatile boolean connected = false;
    
    /**
     * 构造 Redo 服务并启动定时重做任务。
     *
     * <p>创建者：NamingGrpcClientProxy 构造器 L261 —
     * {@code new NamingGrpcRedoService(this, namingFuzzyWatchServiceListHolder, properties)}</p>
     *
     * @param clientProxy                       gRPC 命名代理（用于重做时发送实际请求）
     * @param namingFuzzyWatchServiceListHolder 模糊监听持有者（断连时重置状态）
     * @param properties                       客户端配置（读取 redoDelayTime 和 redoThreadCount）
     */
    public NamingGrpcRedoService(NamingGrpcClientProxy clientProxy,
        NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder,
        NacosClientProperties properties) {
        // Step 1: 读取配置（REDO_DELAY_TIME + REDO_DELAY_THREAD_COUNT）
        setProperties(properties);
        // Step 2: 缓存 FuzzyWatchServiceListHolder（断连时使用）
        this.namingFuzzyWatchServiceListHolder = namingFuzzyWatchServiceListHolder;
        // Step 3: 创建定时重做线程池（线程名 = com.alibaba.nacos.client.naming.grpc.redo）
        this.redoExecutor = new ScheduledThreadPoolExecutor(redoThreadCount,
            new NameThreadFactory(REDO_THREAD_NAME));
        // Step 4: 启动定时重做任务（固定延迟 redoDelayTime ms，默认 3000ms）
        this.redoExecutor.scheduleWithFixedDelay(new RedoScheduledTask(clientProxy, this),
            redoDelayTime, redoDelayTime,
            TimeUnit.MILLISECONDS);
    }
    
    private void setProperties(NacosClientProperties properties) {
        redoDelayTime = properties.getLong(PropertyKeyConst.REDO_DELAY_TIME,
            Constants.DEFAULT_REDO_DELAY_TIME);
        redoThreadCount = properties.getInteger(PropertyKeyConst.REDO_DELAY_THREAD_COUNT,
            Constants.DEFAULT_REDO_THREAD_COUNT);
    }
    
    public ConcurrentMap<String, InstanceRedoData> getRegisteredInstances() {
        return registeredInstances;
    }
    
    public boolean isConnected() {
        return connected;
    }
    
    /**
     * gRPC 连接建立回调。
     * <p>仅设置 connected 标志为 true，后续 RedoScheduledTask 会检测到此标志并开始重做。</p>
     */
    @Override
    public void onConnected(Connection connection) {
        connected = true;
        LogUtils.NAMING_LOGGER.info("Grpc connection connect");
    }
    
    /**
     * gRPC 连接断开回调 —— Redo 机制的核心触发器。
     *
     * <p>断连时执行三个同步块，将所有缓存数据标记为"需要重做"：</p>
     * <ol>
     *   <li>遍历 registeredInstances → 全部 setRegistered(false)（下次扫描触发 REGISTER 重做）</li>
     *   <li>遍历 subscribes → 全部 setRegistered(false)（下次扫描触发 REGISTER 重做）</li>
     *   <li>重置 FuzzyWatchServiceListHolder 一致性状态</li>
     * </ol>
     *
     * <p>调用方：RpcClient 在 gRPC 连接断开时通过 ConnectionEventListener 回调。</p>
     */
    @Override
    public void onDisConnect(Connection connection) {
        connected = false;
        LogUtils.NAMING_LOGGER.warn("Grpc connection disconnect, mark to redo");
        // Step 1: 所有已注册实例标记为未注册（断连丢失，需要重做注册）
        synchronized (registeredInstances) {
            registeredInstances.values()
                .forEach(instanceRedoData -> instanceRedoData.setRegistered(false));
        }
        // Step 2: 所有订阅标记为未注册（断连丢失，需要重做订阅）
        synchronized (subscribes) {
            subscribes.values()
                .forEach(subscriberRedoData -> subscriberRedoData.setRegistered(false));
        }
        // Step 3: 重置模糊监听一致性状态
        synchronized (namingFuzzyWatchServiceListHolder) {
            namingFuzzyWatchServiceListHolder.resetConsistenceStatus();
        }
        LogUtils.NAMING_LOGGER.warn("mark to redo completed");
    }
    
    /**
     * 缓存单实例注册数据，用于后续重做。
     *
     * <p>调用方：NamingGrpcClientProxy.registerServiceForEphemeral() L379 —
     * 临时实例注册时，先缓存 redo 再发送请求。</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     * @param instance    要注册的实例
     */
    public void cacheInstanceForRedo(String serviceName, String groupName, Instance instance) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        InstanceRedoData redoData = InstanceRedoData.build(serviceName, groupName, instance);
        synchronized (registeredInstances) {
            registeredInstances.put(key, redoData);
        }
    }
    
    /**
     * 缓存批量实例注册数据，用于后续重做。
     *
     * <p>与单实例版本的区别：使用 BatchInstanceRedoData 存储实例列表，
     * 重做时调用 doBatchRegisterService() 而非 doRegisterService()。</p>
     *
     * <p>调用方：NamingGrpcClientProxy.batchRegisterService() L392</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     * @param instances   要批量注册的实例列表
     */
    public void cacheInstanceForRedo(String serviceName, String groupName,
        List<Instance> instances) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        BatchInstanceRedoData redoData =
            BatchInstanceRedoData.build(serviceName, groupName, instances);
        synchronized (registeredInstances) {
            registeredInstances.put(key, redoData);
        }
    }
    
    /**
     * 实例注册成功后调用 —— 标记注册状态为 true。
     *
     * <p>调用 RedoData.registered() 设置 registered=true, unregistering=false，
     * 后续 RedoScheduledTask 扫描时 getRedoType() 返回 NONE，不再重做。</p>
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>NamingGrpcClientProxy.doRegisterService() L467</li>
     *   <li>NamingGrpcClientProxy.doBatchRegisterService() L451</li>
     * </ul>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     */
    public void instanceRegistered(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                redoData.registered();
            }
        }
    }
    
    /**
     * 实例开始注销时调用 —— 标记注销中状态。
     *
     * <p>设置 unregistering=true + expectedRegistered=false，
     * 后续 RedoScheduledTask 扫描时 getRedoType() 返回 UNREGISTER 或 REMOVE。</p>
     *
     * <p>调用方：NamingGrpcClientProxy.deregisterServiceForEphemeral() L531-532</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     */
    public void instanceDeregister(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                redoData.setUnregistering(true);
                redoData.setExpectedRegistered(false);
            }
        }
    }
    
    /**
     * 实例注销完成后调用 —— 标记已注销状态。
     *
     * <p>调用 RedoData.unregistered() 设置 registered=false, unregistering=true，
     * 后续 RedoScheduledTask 扫描时 getRedoType() 返回 REMOVE，将从缓存中清除。</p>
     *
     * <p>调用方：NamingGrpcClientProxy.doDeregisterService() L550</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     */
    public void instanceDeregistered(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                redoData.unregistered();
            }
        }
    }
    
    /**
     * 从缓存中移除不再需要重做的实例数据。
     *
     * <p>仅在 expectedRegistered=false 时执行移除（即确认不再需要注册），
     * 防止误删仍需重做的数据。</p>
     *
     * <p>调用方：RedoScheduledTask.redoForInstance() L91 —
     * getRedoType() 返回 REMOVE 时调用</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     */
    public void removeInstanceForRedo(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData && !redoData.isExpectedRegistered()) {
                registeredInstances.remove(key);
            }
        }
    }
    
    /**
     * 查找所有需要重做的实例数据。
     *
     * <p>遍历 registeredInstances，调用每个 RedoData.isNeedRedo()（即 getRedoType() != NONE），
     * 将需要重做的条目收集到 Set 中返回。</p>
     *
     * <p>调用方：RedoScheduledTask.redoForInstances() L60</p>
     *
     * @return 需要重做的 InstanceRedoData 集合
     */
    public Set<InstanceRedoData> findInstanceRedoData() {
        Set<InstanceRedoData> result = new HashSet<>();
        synchronized (registeredInstances) {
            for (InstanceRedoData each : registeredInstances.values()) {
                if (each.isNeedRedo()) {
                    result.add(each);
                }
            }
        }
        return result;
    }
    
    /**
     * 缓存订阅数据，用于后续重做。
     *
     * <p>key = ServiceInfo.getKey(groupedName, cluster)，如 "group@@service##cluster"。</p>
     *
     * <p>调用方：NamingGrpcClientProxy.subscribe() L629</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     * @param cluster     集群名
     */
    public void cacheSubscriberForRedo(String serviceName, String groupName, String cluster) {
        String key =
            ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        SubscriberRedoData redoData = SubscriberRedoData.build(serviceName, groupName, cluster);
        synchronized (subscribes) {
            subscribes.put(key, redoData);
        }
    }
    
    /**
     * 订阅成功后调用 —— 标记订阅状态为 true。
     *
     * <p>调用方：NamingGrpcClientProxy.doSubscribe() L643</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     * @param cluster     集群名
     */
    public void subscriberRegistered(String serviceName, String groupName, String cluster) {
        String key =
            ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            if (null != redoData) {
                redoData.setRegistered(true);
            }
        }
    }
    
    /**
     * 取消订阅开始调用 —— 标记注销中状态。
     *
     * <p>调用方：NamingGrpcClientProxy.unsubscribe() L666</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     * @param cluster     集群名
     */
    public void subscriberDeregister(String serviceName, String groupName, String cluster) {
        String key =
            ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            if (null != redoData) {
                redoData.setUnregistering(true);
                redoData.setExpectedRegistered(false);
            }
        }
    }
    
    /**
     * 判断某个服务是否已订阅。
     *
     * <p>调用方：NamingGrpcClientProxy.isSubscribed() L674</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     * @param cluster     集群名
     * @return true 如果已订阅
     */
    public boolean isSubscriberRegistered(String serviceName, String groupName, String cluster) {
        String key =
            ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            return null != redoData && redoData.isRegistered();
        }
    }
    
    /**
     * 从缓存中移除不再需要重做的订阅数据。
     *
     * <p>调用方：RedoScheduledTask.redoForSubscribe() L143-144 —
     * getRedoType() 返回 REMOVE 时调用；以及
     * NamingGrpcClientProxy.doUnsubscribe() L691</p>
     *
     * @param serviceName 服务名
     * @param groupName   分组名
     * @param cluster     集群名
     */
    public void removeSubscriberForRedo(String serviceName, String groupName, String cluster) {
        String key =
            ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            if (null != redoData && !redoData.isExpectedRegistered()) {
                subscribes.remove(key);
            }
        }
    }
    
    /**
     * 查找所有需要重做的订阅数据。
     *
     * <p>调用方：RedoScheduledTask.redoForSubscribes() L111</p>
     *
     * @return 需要重做的 SubscriberRedoData 集合
     */
    public Set<SubscriberRedoData> findSubscriberRedoData() {
        Set<SubscriberRedoData> result = new HashSet<>();
        synchronized (subscribes) {
            for (SubscriberRedoData each : subscribes.values()) {
                if (each.isNeedRedo()) {
                    result.add(each);
                }
            }
        }
        return result;
    }
    
    /**
     * 根据组合服务名查询已注册实例的 RedoData。
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>NamingGrpcClientProxy.getRetainInstance() L208-209 — 批量注销时计算差集</li>
     *   <li>NamingGrpcClientProxy.deregisterServiceForEphemeral() L521 —
     *       判断是否为批量注册模式</li>
     * </ul>
     *
     * @param combinedServiceName 组合服务名（group@@service）或 NamingUtils.getGroupedName 结果
     * @return 对应的 InstanceRedoData，不存在则返回 null
     */
    public InstanceRedoData getRegisteredInstancesByKey(String combinedServiceName) {
        return registeredInstances.get(combinedServiceName);
    }
    
    /**
     * 关闭 Redo 服务 —— 清空缓存并停止定时重做。
     *
     * <p>调用方：NamingGrpcClientProxy.shutdown() L856</p>
     */
    public void shutdown() {
        LogUtils.NAMING_LOGGER.info("Shutdown grpc redo service executor " + redoExecutor);
        registeredInstances.clear();
        subscribes.clear();
        redoExecutor.shutdownNow();
    }
    
}
