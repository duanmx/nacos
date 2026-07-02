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

package com.alibaba.nacos.client.naming.cache;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesDiff;
import com.alibaba.nacos.client.naming.utils.CacheDirUtil;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.api.utils.json.JsonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Naming 客户端服务信息持有者 —— 客户端本地实例列表的"中央仓库"。
 *
 * <h2>核心职责</h2>
 * <p>ServiceInfoHolder 是 Nacos Naming 客户端中所有服务实例数据的唯一内存入口。
 * 无论数据来源是 gRPC 服务端推送、定时拉取、还是主动订阅，最终都汇聚到
 * {@link #processServiceInfo(ServiceInfo)} 方法进行统一处理。</p>
 *
 * <h2>数据流转全景</h2>
 * <pre>{@code
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │                     数据来源（3 条入口）                        │
 *   ├──────────────────┬───────────────────┬──────────────────────────┤
 *   │ gRPC 服务端推送   │ 定时拉取（兜底）   │ 主动订阅                  │
 *   │ NamingPush       │ ServiceInfo       │ NamingClientProxyDelegate │
 *   │ RequestHandler   │ UpdateService     │ .subscribe()              │
 *   │ .requestReply()  │ .updateServiceNow │                           │
 *   └────────┬─────────┴────────┬──────────┴────────────┬─────────────┘
 *            │                  │                       │
 *            └──────────────────┼───────────────────────┘
 *                               ▼
 *                   processServiceInfo(ServiceInfo)
 *                               │
 *               ┌───────────────┼───────────────┐
 *               ▼               ▼               ▼
 *         ① 更新内存       ② 计算差异       ③ 发布事件
 *      serviceInfoMap    InstancesDiffer    InstancesChangeEvent
 *      .put(key, info)   .doDiff(old,new)   + DiskCacheRefreshEvent
 *                                                 │
 *                                                 ▼
 *                                    NotifyCenter 分发
 *                                    → InstancesChangeNotifier
 *                                      → SelectorManager
 *                                        → 用户 EventListener
 * }</pre>
 *
 * <h2>关键组件协作</h2>
 * <ul>
 *   <li><b>FailoverReactor</b> —— 故障转移反应器，每 5s 轮询磁盘开关文件；
 *       开启时 {@link #isFailoverSwitch()} 返回 true，
 *       {@link #getFailoverServiceInfo(String, String)} 直接返回磁盘缓存数据，
 *       跳过 serviceInfoMap。由 NacosNamingService.getServiceInfo() 在查询前检查。</li>
 *   <li><b>ServiceInfoDiskCacheRefresher</b> —— 异步磁盘缓存刷新器，
 *       processServiceInfo 中实例列表有变化时，通过
 *       publishDiskCacheRefreshEvent 异步写入磁盘文件（100ms 批量刷盘）。</li>
 *   <li><b>InstancesDiffer</b> —— 实例差异计算器，比较新旧 ServiceInfo，
 *       产出 InstancesDiff（added/removed/modified 三集合）。</li>
 * </ul>
 *
 * <h2>创建与生命周期</h2>
 * <ul>
 *   <li><b>创建</b>：NacosNamingService.init() 中创建，
 *       传入 namespace、notifierEventScope（UUID，用于事件隔离）、properties。</li>
 *   <li><b>传递</b>：创建后传给 NamingClientProxyDelegate 构造器，
 *       再传给 NamingGrpcClientProxy，最终传给 NamingPushRequestHandler。</li>
 *   <li><b>关闭</b>：NacosNamingService.shutDown() 调用
 *       {@link #shutdown()}，依次关闭 FailoverReactor 和 DiskCacheRefresher。</li>
 * </ul>
 *
 * <h2>pushEmptyProtection（空推送保护）</h2>
 * <p>当服务端推送的实例列表为空时，可能是服务端数据异常（如数据被误删）。
 * 如果开启 pushEmptyProtection=true，客户端会拒绝接收空列表，保留旧数据不变，
 * 避免因服务端异常导致客户端服务发现全部为空。默认关闭。</p>
 *
 * @author xiweng.yy
 */
public class ServiceInfoHolder implements Closeable {
    
    /**
     * 服务实例列表的内存缓存 —— 客户端本地"数据库"。
     * <p>key = {@code groupName@@serviceName}（不含 cluster），
     * value = ServiceInfo（含完整实例列表）。
     * 并发安全：使用 ConcurrentHashMap，多线程读写无需加锁。</p>
     */
    private final ConcurrentMap<String, ServiceInfo> serviceInfoMap;
    
    /**
     * 故障转移反应器 —— 当服务端不可用时，从磁盘读取缓存数据替代。
     * <p>每 5s 轮询磁盘开关文件（failover/00-00---000-000---000），开关开启时
     * isFailoverSwitch 返回 true，NacosNamingService 查询时走磁盘数据而非内存。</p>
     */
    private final FailoverReactor failoverReactor;
    
    /**
     * 空推送保护开关 —— true 时拒绝接收空实例列表，保留旧数据。
     * <p>配置项：PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION，默认 false。</p>
     */
    private final boolean pushEmptyProtection;
    
    /**
     * 实例差异计算器 —— 比较新旧 ServiceInfo，计算 added/removed/modified。
     * <p>用于判断是否需要发布 InstancesChangeEvent（只有有变化时才发事件）。</p>
     */
    private final InstancesDiffer instancesDiffer;
    
    /**
     * 异步磁盘缓存刷新器 —— 实例变化时异步写入磁盘文件。
     * <p>采用"批处理 + 去抖"模式：pendingEvents Map 同 key 覆盖，
     * 100ms 定时器批量刷盘，避免高频推送导致磁盘 IO 过载。</p>
     */
    private final ServiceInfoDiskCacheRefresher serviceInfoDiskCacheRefresher;
    
    /**
     * 磁盘缓存目录路径 —— {base}/nacos/{registryDir}/naming/{namespace}。
     * <p>由 CacheDirUtil.initCacheDir() 计算，用于 DiskCache 读取/写入和 FailoverReactor。</p>
     */
    private String cacheDir;
    
    /**
     * 事件通知作用域 —— UUID，用于事件隔离。
     * <p>每个 NacosNamingService 实例有独立的 notifierEventScope，
     * InstancesChangeNotifier.scopeMatches() 据此过滤，
     * 防止多个 NamingService 实例的事件互相干扰。</p>
     */
    private String notifierEventScope;
    
    /**
     * 是否启用客户端指标监控 —— true 时更新 MetricsMonitor 中的 serviceInfoMapSize。
     * <p>配置项：PropertyKeyConst.ENABLE_CLIENT_METRICS，默认 true。</p>
     */
    private boolean enableClientMetrics = true;
    
    /**
     * 构造 ServiceInfoHolder，初始化所有核心组件。
     *
     * <p>调用方：NacosNamingService.init() L126-127。
     * 创建后传给 NamingClientProxyDelegate，再传给 NamingGrpcClientProxy，
     * 最终注册到 NamingPushRequestHandler 中接收 gRPC 推送。</p>
     *
     * <h3>初始化流程</h3>
     * <ol>
     *   <li>计算磁盘缓存目录路径（CacheDirUtil.initCacheDir）</li>
     *   <li>如果配置了 NAMING_LOAD_CACHE_AT_START=true，从磁盘加载历史缓存到内存，
     *       实现"客户端重启后立即有数据"（不依赖服务端推送）</li>
     *   <li>创建 FailoverReactor（启动 5s 轮询线程）</li>
     *   <li>创建 ServiceInfoDiskCacheRefresher（启动 100ms 定时刷盘线程）</li>
     *   <li>读取 pushEmptyProtection 和 enableClientMetrics 配置</li>
     * </ol>
     *
     * @param namespace          命名空间，用于拼接缓存目录路径
     * @param notifierEventScope 事件通知作用域（UUID），用于事件隔离
     * @param properties         Nacos 客户端配置
     */
    public ServiceInfoHolder(String namespace, String notifierEventScope,
        NacosClientProperties properties) {
        cacheDir = CacheDirUtil.initCacheDir(namespace, properties);
        instancesDiffer = new InstancesDiffer();
        // 如果配置了 NAMING_LOAD_CACHE_AT_START=true，从磁盘加载历史缓存
        // 场景：客户端重启后、服务端尚未推送前，本地已有数据可用
        if (isLoadCacheAtStart(properties)) {
            this.serviceInfoMap = new ConcurrentHashMap<>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<>(16);
        }
        this.failoverReactor = new FailoverReactor(this, notifierEventScope);
        this.serviceInfoDiskCacheRefresher = new ServiceInfoDiskCacheRefresher();
        this.pushEmptyProtection = isPushEmptyProtect(properties);
        this.notifierEventScope = notifierEventScope;
        this.enableClientMetrics = Boolean.parseBoolean(
            properties.getProperty(PropertyKeyConst.ENABLE_CLIENT_METRICS, "true"));
    }
    
    /**
     * 读取 NAMING_LOAD_CACHE_AT_START 配置项。
     * <p>true 时构造器从磁盘加载历史缓存到 serviceInfoMap，
     * 实现"客户端重启后立即有数据"。默认 false。</p>
     */
    private boolean isLoadCacheAtStart(NacosClientProperties properties) {
        boolean loadCacheAtStart = false;
        if (properties != null && StringUtils.isNotEmpty(
            properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START))) {
            loadCacheAtStart = ConvertUtils.toBoolean(
                properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START));
        }
        return loadCacheAtStart;
    }
    
    /**
     * 读取 NAMING_PUSH_EMPTY_PROTECTION 配置项。
     * <p>true 时拒绝接收空实例列表推送，保留旧数据不变，防止服务端异常导致空发现。</p>
     */
    private boolean isPushEmptyProtect(NacosClientProperties properties) {
        boolean pushEmptyProtection = false;
        if (properties != null && StringUtils.isNotEmpty(
            properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION))) {
            pushEmptyProtection = ConvertUtils.toBoolean(
                properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION));
        }
        return pushEmptyProtection;
    }
    
    /**
     * 获取完整的 serviceInfoMap 引用（非副本）。
     * <p>调用方：NamingClientProxyDelegate.subscribe() 中订阅时先查缓存是否存在。</p>
     *
     * @return serviceInfoMap 的直接引用，外部可遍历但不应修改
     */
    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }
    
    /**
     * 从内存缓存中查询指定服务的实例列表（返回深拷贝，线程安全）。
     *
     * <p>调用方：</p>
     * <ul>
     *   <li>NacosNamingService.getServiceInfoBySubscribe() —— 用户查询实例时先查本地缓存</li>
     *   <li>NacosNamingService.notifyIfSubscribed() —— 重复订阅时用缓存立即通知 listener</li>
     * </ul>
     *
     * @param serviceName 服务名
     * @param groupName   组名
     * @return ServiceInfo 的 clone 副本（防止外部修改污染缓存），不存在时返回 null
     */
    public ServiceInfo getServiceInfo(final String serviceName, final String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        ServiceInfo serviceInfo = serviceInfoMap.get(key);
        // 返回 clone 副本，防止调用方修改对象导致缓存数据被污染
        return serviceInfo == null ? null : serviceInfo.clone();
    }
    
    /**
     * 从 JSON 字符串解析并处理服务实例信息。
     *
     * <p>调用方：NamingPushRequestHandler.requestReply() —— gRPC 服务端推送
     * NotifySubscriberRequest 时，内部携带的 ServiceInfo 通过 JSON 传输，
     * 到达客户端后反序列化再交给 {@link #processServiceInfo(ServiceInfo)} 处理。</p>
     *
     * @param json 服务端推送的 JSON 字符串
     * @return 处理后的 ServiceInfo，如果 serviceKey 为 null 则返回 null
     */
    public ServiceInfo processServiceInfo(String json) {
        ServiceInfo serviceInfo = JsonUtils.toObj(json, ServiceInfo.class);
        // 保留服务端原始 JSON，后续磁盘缓存时直接写入，避免再次序列化
        serviceInfo.setJsonFromServer(json);
        return processServiceInfo(serviceInfo);
    }
    
    /**
     * 处理新的服务实例信息 —— ServiceInfoHolder 的核心方法。
     *
     * <p>这是客户端接收服务实例数据的唯一统一入口，所有数据来源最终都调用此方法：</p>
     * <ul>
     *   <li><b>gRPC 推送</b>：NamingPushRequestHandler.requestReply()
     *       → processServiceInfo(ServiceInfo)</li>
     *   <li><b>定时拉取</b>：ServiceInfoUpdateService.updateServiceNow()
     *       → namingClientProxy.queryInstancesOfService()
     *       → processServiceInfo(ServiceInfo)</li>
     *   <li><b>主动订阅</b>：NamingClientProxyDelegate.subscribe()
     *       → grpcClientProxy.subscribe()
     *       → processServiceInfo(ServiceInfo)</li>
     * </ul>
     *
     * <h3>处理流程（5 步）</h3>
     * <ol>
     *   <li><b>校验 serviceKey</b>：key 为 null 说明数据非法，直接丢弃</li>
     *   <li><b>空推送保护</b>：如果开启 pushEmptyProtection 且推送为空，
     *       保留旧数据不变（防止服务端异常导致空发现）</li>
     *   <li><b>更新内存 + 计算差异</b>：写入 serviceInfoMap，
     *       用 InstancesDiffer 对比新旧数据计算 added/removed/modified</li>
     *   <li><b>更新指标</b>：如果 enableClientMetrics=true，
     *       更新 MetricsMonitor 中的 serviceInfoMapSize</li>
     *   <li><b>发布事件（仅有变化时）</b>：
     *     <ul>
     *       <li>InstancesChangeEvent —— 通知用户 EventListener（通过 InstancesChangeNotifier → SelectorManager 路由）</li>
     *       <li>DiskCacheRefreshEvent —— 异步写入磁盘文件（通过 ServiceInfoDiskCacheRefresher）</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * <h3>failover 交互</h3>
     * <p>如果 failover 开关已开启（failoverReactor.isFailoverSwitch 返回 true），
     * 则跳过发布 InstancesChangeEvent（但不跳过磁盘缓存刷新），
     * 因为 failover 模式下用户查询走磁盘数据，不需要事件通知。</p>
     *
     * @param serviceInfo 新的服务实例信息
     * @return 处理后的 ServiceInfo（即传入的对象本身，已写入内存）；空推送时返回旧数据
     */
    public ServiceInfo processServiceInfo(ServiceInfo serviceInfo) {
        // Step 1: 校验 serviceKey（groupName@@serviceName 格式）
        String serviceKey = serviceInfo.getKeyWithoutClusters();
        if (serviceKey == null) {
            NAMING_LOGGER.warn("process service info but serviceKey is null, service host: {}",
                JsonUtils.toJson(serviceInfo.getHosts()));
            return null;
        }
        // 获取旧数据用于差异比较
        ServiceInfo oldService = serviceInfoMap.get(serviceKey);
        // Step 2: 空推送保护 —— 拒绝接收空列表，保留旧数据
        if (isEmptyOrErrorPush(serviceInfo)) {
            NAMING_LOGGER.warn(
                "process service info but found empty or error push, serviceKey: {}, "
                    + "pushEmptyProtection: {}, hosts: {}",
                serviceKey, pushEmptyProtection, serviceInfo.getHosts());
            return oldService;
        }
        // Step 3: 更新内存缓存 + 计算差异
        serviceInfoMap.put(serviceKey, serviceInfo);
        InstancesDiff diff = getServiceInfoDiff(oldService, serviceInfo);
        // 如果没有服务端原始 JSON（如定时拉取场景），用本地序列化补上
        if (StringUtils.isBlank(serviceInfo.getJsonFromServer())) {
            serviceInfo.setJsonFromServer(JsonUtils.toJson(serviceInfo));
        }
        
        // Step 4: 更新指标监控（可选）
        if (enableClientMetrics) {
            try {
                MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());
            } catch (Throwable t) {
                NAMING_LOGGER.error("Failed to update metrics for service info map size", t);
            }
        }
        
        // Step 5: 实例列表有变化时，发布事件 + 刷盘
        if (diff.hasDifferent()) {
            NAMING_LOGGER.info("current ips:({}) service: {} -> {}", serviceInfo.ipCount(),
                serviceKey,
                JsonUtils.toJson(serviceInfo.getHosts()));
            
            // 5a. 发布 InstancesChangeEvent 通知用户 listener
            //     如果 failover 开关开启，跳过事件通知（failover 模式下用户走磁盘数据）
            if (!failoverReactor.isFailoverSwitch(serviceKey)) {
                NotifyCenter.publishEvent(
                    new InstancesChangeEvent(notifierEventScope, serviceInfo.getName(),
                        serviceInfo.getGroupName(),
                        serviceInfo.getClusters(), serviceInfo.getHosts(), diff));
            }
            // 5b. 异步刷新磁盘缓存（无论是否 failover，都写入磁盘）
            publishDiskCacheRefreshEvent(serviceKey, serviceInfo);
        }
        return serviceInfo;
    }
    
    /**
     * 发布磁盘缓存刷新事件，触发 ServiceInfoDiskCacheRefresher 异步写盘。
     *
     * <p>这是一个轻量级操作：只是往 pendingEvents Map 中 put 一个事件，
     * 由 ServiceInfoDiskCacheRefresher 内部的 100ms 定时器批量执行磁盘写入。</p>
     *
     * @param serviceKey  服务 key（groupName@@serviceName）
     * @param serviceInfo 最新的服务实例信息快照
     */
    private void publishDiskCacheRefreshEvent(String serviceKey, ServiceInfo serviceInfo) {
        serviceInfoDiskCacheRefresher.publishEvent(
            new ServiceInfoDiskCacheRefreshEvent(serviceKey, serviceInfo, cacheDir));
    }
    
    /**
     * 判断是否为空推送或错误推送。
     * <p>两种情况视为异常：</p>
     * <ul>
     *   <li>hosts 为 null —— 服务端推送的数据不完整</li>
     *   <li>pushEmptyProtection=true 且 validate() 失败 —— 开启了空推送保护，
     *       且实例列表不合法（如全部实例被摘除）</li>
     * </ul>
     *
     * @param serviceInfo 待检查的服务实例信息
     * @return true 表示应拒绝此次推送，保留旧数据
     */
    private boolean isEmptyOrErrorPush(ServiceInfo serviceInfo) {
        return null == serviceInfo.getHosts() || (pushEmptyProtection && !serviceInfo.validate());
    }
    
    /**
     * 委托 InstancesDiffer 计算新旧 ServiceInfo 的差异。
     * <p>InstancesDiffer.doDiff() 通过对比 IP 地址列表，
     * 计算出 added（新增）、removed（移除）、modified（属性变化）三个集合。</p>
     *
     * @param oldService 旧的服务信息（可能为 null，表示首次推送）
     * @param newService 新的服务信息
     * @return InstancesDiff 差异对象，hasDifferent() 判断是否有任何变化
     */
    private InstancesDiff getServiceInfoDiff(ServiceInfo oldService, ServiceInfo newService) {
        return instancesDiffer.doDiff(oldService, newService);
    }
    
    /**
     * 获取磁盘缓存目录路径。
     * <p>调用方：FailoverReactor 通过持有 ServiceInfoHolder 引用间接调用，
     * 用于读取/写入 failover 目录下的开关文件和缓存文件。</p>
     *
     * @return 缓存目录路径，格式：{base}/nacos/{registryDir}/naming/{namespace}
     */
    public String getCacheDir() {
        return cacheDir;
    }
    
    /**
     * 检查故障转移开关是否已全局开启。
     * <p>调用方：NacosNamingService.getServiceInfo() 在查询实例前检查，
     * 如果返回 true，则走 getFailoverServiceInfo() 读取磁盘数据而非内存缓存。</p>
     *
     * @return true 表示 failover 已开启，用户查询应走磁盘数据
     */
    public boolean isFailoverSwitch() {
        return failoverReactor.isFailoverSwitch();
    }
    
    /**
     * 从故障转移磁盘缓存中查询指定服务的实例列表。
     * <p>调用方：NacosNamingService.getServiceInfoByFailover() —— 当 isFailoverSwitch()
     * 返回 true 时，不查内存缓存，而是从 {cacheDir}/failover/ 目录读取磁盘文件。</p>
     *
     * @param serviceName 服务名
     * @param groupName   组名
     * @return 磁盘缓存的 ServiceInfo，不存在时返回 null
     */
    public ServiceInfo getFailoverServiceInfo(final String serviceName, final String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        return failoverReactor.getService(key);
    }
    
    /**
     * 关闭 ServiceInfoHolder，释放所有资源。
     * <p>调用方：NacosNamingService.shutDown() —— 客户端关闭时调用。</p>
     *
     * <p>关闭顺序：</p>
     * <ol>
     *   <li>FailoverReactor.shutdown() —— 关闭 5s 轮询线程</li>
     *   <li>ServiceInfoDiskCacheRefresher.shutdown() —— 关闭 100ms 刷盘线程，
     *       并执行最后一次 flush（将 pendingEvents 中剩余数据写入磁盘）</li>
     * </ol>
     */
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        failoverReactor.shutdown();
        serviceInfoDiskCacheRefresher.shutdown();
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
