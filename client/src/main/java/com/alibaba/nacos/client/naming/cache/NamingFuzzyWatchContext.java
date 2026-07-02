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

import com.alibaba.nacos.api.naming.listener.FuzzyWatchChangeEvent;
import com.alibaba.nacos.api.naming.listener.FuzzyWatchEventWatcher;
import com.alibaba.nacos.api.naming.listener.FuzzyWatchLoadWatcher;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.FuzzyGroupKeyPattern;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.nacos.api.common.Constants.FUZZY_WATCH_DIFF_SYNC_NOTIFY;
import static com.alibaba.nacos.api.common.Constants.FUZZY_WATCH_INIT_NOTIFY;
import static com.alibaba.nacos.api.common.Constants.ServiceChangedType.ADD_SERVICE;
import static com.alibaba.nacos.api.common.Constants.ServiceChangedType.DELETE_SERVICE;
import static com.alibaba.nacos.api.model.v2.ErrorCode.FUZZY_WATCH_PATTERN_MATCH_COUNT_OVER_LIMIT;
import static com.alibaba.nacos.api.model.v2.ErrorCode.FUZZY_WATCH_PATTERN_OVER_LIMIT;

/**
 * Naming 模糊监听上下文 —— 单个通配符 pattern 的完整运行时状态。
 *
 * <h2>核心职责</h2>
 * <p>每个 {@code NamingFuzzyWatchContext} 对应一个通配符模式（如 {@code svc-*}），
 * 维护该模式已匹配的服务列表（receivedServiceKeys）、注册的监听器集合（fuzzyWatchEventWatcherWrappers）、
 * 以及与服务器的同步一致性状态。它是 Naming 模块模糊监听功能的核心状态单元。</p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   场景1: 服务端推送新服务（ADD_SERVICE）
 *   NamingFuzzyWatchNotifyRequestHandler.requestReply()
 *     → NamingFuzzyWatchChangeNotifyRequest
 *       → NamingFuzzyWatchServiceListHolder.onEvent(NamingFuzzyWatchNotifyEvent)
 *         → context.notifyFuzzyWatchers(serviceKey, ADD_SERVICE, ...)
 *           → doNotifyWatcher() → FuzzyWatchEventWatcher.onEvent(FuzzyWatchChangeEvent)
 *           → addReceivedServiceKey(serviceKey) —— 记录已匹配的服务
 *
 *   场景2: 初始化同步（FUZZY_WATCH_INIT_NOTIFY）
 *   NamingFuzzyWatchNotifyRequestHandler.requestReply()
 *     → NamingFuzzyWatchSyncRequest（服务端回传已匹配的全部服务列表）
 *       → NamingFuzzyWatchServiceListHolder.onEvent(NamingFuzzyWatchNotifyEvent)
 *         → context.notifyFuzzyWatchers() + context.markInitializationComplete()
 *           → 唤醒 createNewFuture() 中等待的 Future.get() 调用方
 *
 *   场景3: 定时全量同步
 *   NamingFuzzyWatchServiceListHolder.executeNamingFuzzyWatch()
 *     → context.syncFuzzyWatchers() —— 将 context 状态与各 watcher 状态对齐
 *       → diffGroupKeys() 计算差异 → doNotifyWatcher() 补齐遗漏事件
 * }</pre>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>NamingFuzzyWatchServiceListHolder</b> —— 持有 Map&lt;pattern,Context&gt;，
 *       调用 notifyFuzzyWatchers/syncFuzzyWatchers/markInitializationComplete</li>
 *   <li><b>FuzzyWatchEventWatcherWrapper</b> —— 封装用户的 FuzzyWatchEventWatcher，
 *       附带 syncServiceKeys（该 watcher 已知晓的服务列表）和 syncVersion（增量同步版本）</li>
 *   <li><b>FuzzyGroupKeyPattern</b> —— 通配符匹配引擎，
 *       diffGroupKeys() 用于对比服务端已知列表与客户端已知列表的差异</li>
 *   <li><b>NotfiyCenter</b> —— 事件总线，
 *       通过 NamingFuzzyWatchNotifyEvent/NamingFuzzyWatchLoadEvent 接收入站通知</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：
 *       NamingFuzzyWatchServiceListHolder.initFuzzyWatchContextIfNeed() L194-195</li>
 *   <li><b>使用方</b>：
 *       NamingFuzzyWatchServiceListHolder（主管理器）、
 *       NamingFuzzyWatchNotifyRequestHandler（服务端推送入口）</li>
 *   <li><b>废弃标记</b>：removeWatcher() 中当监听器集合为空时 → setDiscard(true)；
 *       下次 sync 时发送 WATCH_TYPE_CANCEL_WATCH 请求给服务端</li>
 * </ul>
 *
 * <h2>限流抑制机制</h2>
 * <p>当服务端返回 FUZZY_WATCH_PATTERN_MATCH_COUNT_OVER_LIMIT 或
 * FUZZY_WATCH_PATTERN_OVER_LIMIT 错误码时，设置 patternLimitTs 时间戳，
 * 此后 60s 内不再重复通知 onPatternOverLimit/onServiceReachUpLimit 给监听器，
 * 避免日志/通知轰炸。成功同步后调用 clearOverLimitTs() 重置。</p>
 *
 * @author stone-98
 * @date 2024/3/4
 */
public class NamingFuzzyWatchContext {
    
    private static final Logger LOGGER = LogUtils.logger(NamingFuzzyWatchContext.class);
    
    /**
     * 事件作用域（命名空间），用于事件隔离。
     * <p>等于 ServiceInfoHolder.notifierEventScope，确保同名服务在不同 NamingService 实例间的事件不互相干扰。</p>
     */
    private String envName;
    
    /**
     * 通配符模式（groupKeyPattern 格式），如 {@code DEFAULT_GROUP@@svc-*}。
     * <p>由 FuzzyGroupKeyPattern.generatePattern() 生成，作为 fuzzyMatchContextMap 的 key。</p>
     */
    private String groupKeyPattern;
    
    /**
     * 已匹配的服务 key 集合 —— 服务端推送过的所有匹配此 pattern 的服务。
     * <p>key 格式：{@code namespace@@groupName@@serviceName}。</p>
     * <p>并发安全：ConcurrentHashSet。</p>
     */
    private Set<String> receivedServiceKeys = new ConcurrentHashSet<>();
    
    /**
     * 同步版本号 —— 每次 receivedServiceKeys 变化时更新为当前时间戳。
     * <p>用于判断各 watcher 是否需要增量同步（watcher.syncVersion != this.syncVersion）。</p>
     */
    private long syncVersion = 0;
    
    /**
     * 与服务器同步一致性标志。
     * <p>true 表示该 pattern 已成功向服务端注册模糊订阅（WATCH_TYPE_WATCH），
     * executeNamingFuzzyWatch() 中已同步的 context 只做 syncFuzzyWatchers() 无需再发 RPC。</p>
     */
    private final AtomicBoolean isConsistentWithServer = new AtomicBoolean();
    
    /**
     * 初始化完成标志。
     * <p>服务端推送 FINISH_INIT_NOTIFY 时调用 markInitializationComplete() 设置为 true，
     * 同时 notifyAll() 唤醒 createNewFuture() 中阻塞等待的 Future.get() 调用方。</p>
     */
    final AtomicBoolean initializationCompleted = new AtomicBoolean(false);
    
    /**
     * 废弃标志 —— true 表示该 context 没有对应的 watcher，下次 sync 时发送取消订阅。
     * <p>removeWatcher() 中当 fuzzyWatchEventWatcherWrappers 为空时设置为 true。</p>
     */
    private volatile boolean isDiscard = false;
    
    /**
     * 模糊监听器封装集合 —— 一个 pattern 可注册多个 watcher。
     * <p>每个 watcher 独立维护 syncServiceKeys（该 watcher 已知晓的服务）和 syncVersion。</p>
     */
    private final Set<FuzzyWatchEventWatcherWrapper> fuzzyWatchEventWatcherWrappers =
        new HashSet<>();
    
    /**
     * 限流触发时间戳 —— 用于抑制超限通知的重复发送。
     * <p>值为 0 表示未触发限流；非 0 表示最近一次触发的时间。
     * 通过 patternLimitSuppressed() 判断 60s 内是否需抑制。</p>
     */
    long patternLimitTs = 0;
    
    /**
     * 限流抑制周期：60 秒。
     */
    private static final long SUPPRESSED_PERIOD = 60 * 1000L;
    
    boolean patternLimitSuppressed() {
        return patternLimitTs > 0
            && System.currentTimeMillis() - patternLimitTs < SUPPRESSED_PERIOD;
    }
    
    public void clearOverLimitTs() {
        this.patternLimitTs = 0;
    }
    
    public void refreshOverLimitTs() {
        this.patternLimitTs = System.currentTimeMillis();
    }
    
    public void refreshSyncVersion() {
        this.syncVersion = System.currentTimeMillis();
    }
    
    /**
     * Constructor with environment name, data ID pattern, and group.
     *
     * @param envName         Environment name
     * @param groupKeyPattern groupKeyPattern
     */
    public NamingFuzzyWatchContext(String envName, String groupKeyPattern) {
        this.envName = envName;
        this.groupKeyPattern = groupKeyPattern;
    }
    
    /**
     * 通知单个 watcher 某个服务发生了变化。
     *
     * <p>处理逻辑：</p>
     * <ol>
     *   <li><b>去重检查</b>：ADD_SERVICE 时若 watcher 已知晓该 serviceKey → 跳过；
     *       DELETE_SERVICE 时若 watcher 不知晓 → 跳过（防止事件乱序）</li>
     *   <li><b>重置 syncType</b>：初始化未完成时统一标记为 FUZZY_WATCH_INIT_NOTIFY，
     *       避免初始化过程中 Delta Sync 事件被误认为增量通知</li>
     *   <li><b>异步/同步执行</b>：若 watcher 配置了自定义 Executor → 提交到 Executor；
     *       否则在当前线程同步执行</li>
     *   <li><b>更新 watcher 状态</b>：DELETE_SERVICE → 从 syncServiceKeys 移除；
     *       ADD_SERVICE → 添加到 syncServiceKeys</li>
     * </ol>
     *
     * @param serviceKey                  服务 key（namespace@@group@@serviceName）
     * @param changedType                 变更类型：ADD_SERVICE / DELETE_SERVICE
     * @param syncType                    同步类型：FUZZY_WATCH_INIT_NOTIFY / FUZZY_WATCH_DIFF_SYNC_NOTIFY
     * @param fuzzyWatchEventWatcherWrapper 目标 watcher 封装
     */
    private void doNotifyWatcher(final String serviceKey, final String changedType,
        final String syncType,
        FuzzyWatchEventWatcherWrapper fuzzyWatchEventWatcherWrapper) {
        
        if (ADD_SERVICE.equals(changedType) && fuzzyWatchEventWatcherWrapper.getSyncServiceKeys()
            .contains(serviceKey)) {
            return;
        }
        
        if (DELETE_SERVICE.equals(changedType)
            && !fuzzyWatchEventWatcherWrapper.getSyncServiceKeys()
                .contains(serviceKey)) {
            return;
        }
        
        String[] serviceKeyItems = NamingUtils.parseServiceKey(serviceKey);
        String namespace = serviceKeyItems[0];
        String groupName = serviceKeyItems[1];
        String serviceName = serviceKeyItems[2];
        
        final String resetSyncType =
            !initializationCompleted.get() ? FUZZY_WATCH_INIT_NOTIFY : syncType;
        
        Runnable job = () -> {
            long start = System.currentTimeMillis();
            FuzzyWatchChangeEvent event =
                new FuzzyWatchChangeEvent(serviceName, groupName, namespace, changedType,
                    resetSyncType);
            if (fuzzyWatchEventWatcherWrapper != null) {
                fuzzyWatchEventWatcherWrapper.fuzzyWatchEventWatcher.onEvent(event);
            }
            LOGGER.info(
                "[{}] [notify-watcher-ok] serviceName={}, groupName={}, namespace={}, watcher={},changedType={}, job run cost={} millis.",
                envName, serviceName, groupName, namespace,
                fuzzyWatchEventWatcherWrapper.fuzzyWatchEventWatcher,
                changedType, (System.currentTimeMillis() - start));
            if (changedType.equals(DELETE_SERVICE)) {
                fuzzyWatchEventWatcherWrapper.getSyncServiceKeys()
                    .remove(NamingUtils.getServiceKey(namespace, groupName, serviceName));
            } else if (changedType.equals(ADD_SERVICE)) {
                fuzzyWatchEventWatcherWrapper.getSyncServiceKeys()
                    .add(NamingUtils.getServiceKey(namespace, groupName, serviceName));
            }
        };
        
        try {
            if (null != fuzzyWatchEventWatcherWrapper.fuzzyWatchEventWatcher.getExecutor()) {
                LOGGER.info(
                    "[{}] [notify-watcher] task submitted to user executor, serviceName={}, groupName={}, namespace={}, listener={}.",
                    envName, serviceName, groupName, namespace, fuzzyWatchEventWatcherWrapper);
                fuzzyWatchEventWatcherWrapper.fuzzyWatchEventWatcher.getExecutor().execute(job);
            } else {
                LOGGER.info(
                    "[{}] [notify-watcher] task execute in nacos thread, serviceName={}, groupName={}, namespace={}, listener={}.",
                    envName, serviceName, groupName, namespace, fuzzyWatchEventWatcherWrapper);
                job.run();
            }
        } catch (Throwable t) {
            LOGGER.error(
                "[{}] [notify-watcher-error] serviceName={}, groupName={}, namespace={}, listener={}, throwable={}.",
                envName, serviceName, groupName, namespace, fuzzyWatchEventWatcherWrapper,
                t.getCause());
        }
    }
    
    /**
     * 标记初始化完成并唤醒所有等待线程。
     *
     * <p>调用方：NamingFuzzyWatchNotifyRequestHandler.requestReply()
     * —— 收到 FINISH_INIT_NOTIFY 类型的 NamingFuzzyWatchSyncRequest 时调用。</p>
     *
     * <p>设置 initializationCompleted = true，并 notifyAll() 唤醒
     * createNewFuture().get() 中阻塞等待的调用方（如 NacosNamingService.fuzzyWatchWithServiceKeys()）。</p>
     */
    public void markInitializationComplete() {
        LOGGER.info(
            "[{}] [fuzzy-watch] pattern init notify finish pattern={},match service count {}",
            envName,
            groupKeyPattern, receivedServiceKeys.size());
        initializationCompleted.set(true);
        synchronized (this) {
            notifyAll();
        }
    }
    
    /**
     * 从上下文中移除一个 watcher。
     *
     * <p>调用方：NacosNamingService.cancelFuzzyWatch()。
     * 遍历 fuzzyWatchEventWatcherWrappers，equals() 匹配后移除。
     * 如果移除后集合为空，标记 isDiscard=true 且 isConsistentWithServer=false，
     * 下次 sync 时向服务端发送 WATCH_TYPE_CANCEL_WATCH。</p>
     *
     * @param watcher 要移除的模糊监听器
     */
    public synchronized void removeWatcher(FuzzyWatchEventWatcher watcher) {
        Iterator<FuzzyWatchEventWatcherWrapper> iterator =
            fuzzyWatchEventWatcherWrappers.iterator();
        while (iterator.hasNext()) {
            FuzzyWatchEventWatcherWrapper next = iterator.next();
            if (next.fuzzyWatchEventWatcher.equals(watcher)) {
                iterator.remove();
                LOGGER.info("[{}] [remove-watcher-ok] groupKeyPattern={}, watcher={},uuid={} ",
                    getEnvName(),
                    this.groupKeyPattern, watcher, next.getUuid());
            }
        }
        if (fuzzyWatchEventWatcherWrappers.isEmpty()) {
            this.setConsistentWithServer(false);
            this.setDiscard(true);
        }
    }
    
    /**
     * Get the environment name.
     *
     * @return Environment name
     */
    public String getEnvName() {
        return envName;
    }
    
    /**
     * Set the environment name.
     *
     * @param envName Environment name to be set
     */
    public void setEnvName(String envName) {
        this.envName = envName;
    }
    
    public String getGroupKeyPattern() {
        return groupKeyPattern;
    }
    
    /**
     * Get the flag indicating whether the context is consistent with the server.
     *
     * @return AtomicBoolean indicating whether the context is consistent with the server
     */
    public boolean isConsistentWithServer() {
        return isConsistentWithServer.get();
    }
    
    public void setConsistentWithServer(boolean isConsistentWithServer) {
        this.isConsistentWithServer.set(isConsistentWithServer);
    }
    
    /**
     * Check if the context is discarded.
     *
     * @return True if the context is discarded, otherwise false
     */
    public boolean isDiscard() {
        return isDiscard;
    }
    
    /**
     * Set the flag indicating whether the context is discarded.
     *
     * @param discard True to mark the context as discarded, otherwise false
     */
    public void setDiscard(boolean discard) {
        isDiscard = discard;
    }
    
    /**
     * Check if the context is initializing.
     *
     * @return True if the context is initializing, otherwise false
     */
    public boolean isInitializing() {
        return !initializationCompleted.get();
    }
    
    /**
     * Get the set of data IDs associated with the context.
     *
     * @return Set of data IDs
     */
    public Set<String> getReceivedServiceKeys() {
        return Collections.unmodifiableSet(receivedServiceKeys);
    }
    
    /**
     * add received service key.
     *
     * @param serviceKey service key.
     * @return
     */
    public boolean addReceivedServiceKey(String serviceKey) {
        boolean added = receivedServiceKeys.add(serviceKey);
        if (added) {
            refreshSyncVersion();
        }
        return added;
    }
    
    /**
     * remove received service key.
     *
     * @param serviceKey service key.
     * @return
     */
    public boolean removeReceivedServiceKey(String serviceKey) {
        
        boolean removed = receivedServiceKeys.remove(serviceKey);
        if (removed) {
            refreshSyncVersion();
        }
        return removed;
    }
    
    /**
     * Get the set of listeners associated with the context.
     *
     * @return Set of listeners
     */
    public Set<FuzzyWatchEventWatcherWrapper> getFuzzyWatchEventWatcherWrappers() {
        return fuzzyWatchEventWatcherWrappers;
    }
    
    /**
     * 将 context 中已匹配的服务列表与各 watcher 的已知列表对齐。
     *
     * <p>调用方：NamingFuzzyWatchServiceListHolder.executeNamingFuzzyWatch()
     * —— 定时同步任务中调用。</p>
     *
     * <p>遍历所有 watcher，若 watcher.syncVersion != this.syncVersion：
     * 通过 FuzzyGroupKeyPattern.diffGroupKeys() 计算
     * context.receivedServiceKeys 与 watcher.syncServiceKeys 的差异，
     * 对新增的发布 ADD_SERVICE 事件，对移除的发布 DELETE_SERVICE 事件。</p>
     */
    void syncFuzzyWatchers() {
        for (FuzzyWatchEventWatcherWrapper namingFuzzyWatcher : fuzzyWatchEventWatcherWrappers) {
            
            if (namingFuzzyWatcher.syncVersion == this.syncVersion) {
                continue;
            }
            
            Set<String> receivedServiceKeysContext = new HashSet<>(this.getReceivedServiceKeys());
            Set<String> syncGroupKeys = namingFuzzyWatcher.getSyncServiceKeys();
            List<FuzzyGroupKeyPattern.GroupKeyState> groupKeyStates =
                FuzzyGroupKeyPattern.diffGroupKeys(
                    receivedServiceKeysContext, syncGroupKeys);
            if (CollectionUtils.isEmpty(groupKeyStates)) {
                namingFuzzyWatcher.syncVersion = this.syncVersion;
            } else {
                for (FuzzyGroupKeyPattern.GroupKeyState groupKeyState : groupKeyStates) {
                    String changedType = groupKeyState.isExist() ? ADD_SERVICE : DELETE_SERVICE;
                    doNotifyWatcher(groupKeyState.getGroupKey(), changedType,
                        FUZZY_WATCH_DIFF_SYNC_NOTIFY,
                        namingFuzzyWatcher);
                }
            }
        }
    }
    
    void notifyFuzzyWatchers(String serviceKey, String changedType, String syncType,
        String watcherUuid) {
        for (FuzzyWatchEventWatcherWrapper namingFuzzyWatcher : filterWatchers(watcherUuid)) {
            doNotifyWatcher(serviceKey, changedType, syncType, namingFuzzyWatcher);
        }
    }
    
    void notifyOverLimitWatchers(int code) {
        
        if (this.patternLimitSuppressed()) {
            return;
        }
        boolean notify = false;
        
        for (FuzzyWatchEventWatcherWrapper namingFuzzyWatcherWrapper : filterWatchers(null)) {
            if (namingFuzzyWatcherWrapper.fuzzyWatchEventWatcher instanceof FuzzyWatchLoadWatcher) {
                
                if (FUZZY_WATCH_PATTERN_MATCH_COUNT_OVER_LIMIT.getCode().equals(code)) {
                    ((FuzzyWatchLoadWatcher) namingFuzzyWatcherWrapper.fuzzyWatchEventWatcher)
                        .onServiceReachUpLimit();
                    notify = true;
                }
                if (FUZZY_WATCH_PATTERN_OVER_LIMIT.getCode().equals(code)) {
                    ((FuzzyWatchLoadWatcher) namingFuzzyWatcherWrapper.fuzzyWatchEventWatcher)
                        .onPatternOverLimit();
                    notify = true;
                }
            }
        }
        if (notify) {
            this.refreshOverLimitTs();
        }
    }
    
    private Set<FuzzyWatchEventWatcherWrapper> filterWatchers(String uuid) {
        if (StringUtils.isBlank(uuid)
            || CollectionUtils.isEmpty(getFuzzyWatchEventWatcherWrappers())) {
            return getFuzzyWatchEventWatcherWrappers();
        } else {
            return getFuzzyWatchEventWatcherWrappers().stream()
                .filter(a -> a.getUuid().equals(uuid))
                .collect(Collectors.toSet());
        }
    }
    
    /**
     * 创建 Future 对象，阻塞等待初始化完成并返回已匹配的服务列表。
     *
     * <p>调用方：NacosNamingService.fuzzyWatchWithServiceKeys() ——
     * 用户调用模糊订阅时，通过 Future.get() 阻塞等待服务端回传已匹配的服务列表。</p>
     *
     * <p>Future.get() 内部通过 wait/notifyAll 等待 markInitializationComplete() 被调用。
     * get(timeout) 有超时保护，超时后抛出 TimeoutException。</p>
     *
     * @return Future 对象，get() 返回已匹配服务列表的 ListView 包装
     */
    public Future<ListView<String>> createNewFuture() {
        Future<ListView<String>> completableFuture = new Future<ListView<String>>() {
            
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                throw new UnsupportedOperationException("not support to cancel fuzzy watch");
            }
            
            @Override
            public boolean isCancelled() {
                return false;
            }
            
            @Override
            public boolean isDone() {
                return NamingFuzzyWatchContext.this.initializationCompleted.get();
            }
            
            @Override
            public ListView<String> get() throws InterruptedException {
                synchronized (NamingFuzzyWatchContext.this) {
                    while (!NamingFuzzyWatchContext.this.initializationCompleted.get()) {
                        NamingFuzzyWatchContext.this.wait();
                    }
                }
                
                ListView<String> result = new ListView<>();
                result.setData(Arrays.asList(
                    NamingFuzzyWatchContext.this.receivedServiceKeys.toArray(new String[0])));
                result.setCount(result.getData().size());
                return result;
            }
            
            @Override
            public ListView<String> get(long timeout, TimeUnit unit)
                throws InterruptedException, TimeoutException {
                
                if (!NamingFuzzyWatchContext.this.initializationCompleted.get()) {
                    synchronized (NamingFuzzyWatchContext.this) {
                        NamingFuzzyWatchContext.this.wait(unit.toMillis(timeout));
                    }
                }
                
                if (!NamingFuzzyWatchContext.this.initializationCompleted.get()) {
                    throw new TimeoutException(
                        "fuzzy watch result future timeout for " + unit.toMillis(timeout)
                            + " millis");
                }
                
                ListView<String> result = new ListView<>();
                result.setData(Arrays.asList(
                    NamingFuzzyWatchContext.this.receivedServiceKeys.toArray(new String[0])));
                result.setCount(result.getData().size());
                return result;
            }
        };
        return completableFuture;
    }
}
