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

import com.alibaba.nacos.api.ability.constant.AbilityKey;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.api.naming.listener.FuzzyWatchEventWatcher;
import com.alibaba.nacos.api.naming.remote.request.NamingFuzzyWatchRequest;
import com.alibaba.nacos.api.naming.remote.response.NamingFuzzyWatchResponse;
import com.alibaba.nacos.client.naming.event.NamingFuzzyWatchLoadEvent;
import com.alibaba.nacos.client.naming.event.NamingFuzzyWatchNotifyEvent;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.nacos.api.common.Constants.FUZZY_WATCH_INIT_NOTIFY;
import static com.alibaba.nacos.api.common.Constants.ServiceChangedType.ADD_SERVICE;
import static com.alibaba.nacos.api.common.Constants.WATCH_TYPE_CANCEL_WATCH;
import static com.alibaba.nacos.api.common.Constants.WATCH_TYPE_WATCH;
import static com.alibaba.nacos.api.model.v2.ErrorCode.FUZZY_WATCH_PATTERN_MATCH_COUNT_OVER_LIMIT;
import static com.alibaba.nacos.api.model.v2.ErrorCode.FUZZY_WATCH_PATTERN_OVER_LIMIT;

/**
 * Naming 客户端模糊监听服务列表管理器 —— 所有模糊订阅（Fuzzy Watch）的中央调度器。
 *
 * <h2>核心职责</h2>
 * <p>作为 Naming 模块模糊监听功能的总控制器，管理所有通配符 pattern → NamingFuzzyWatchContext 的映射，
 * 负责模糊订阅的注册/取消、与服务端同步（RPC）、以及监听器通知的派发。</p>
 *
 * <h2>架构定位</h2>
 * <p>该类是 SmartSubscriber（订阅 NamingFuzzyWatchNotifyEvent + NamingFuzzyWatchLoadEvent 两个事件），
 * 同时实现 Closeable，由 NacosNamingService 持有。</p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   用户调用 NacosNamingService.fuzzyWatch()
 *     → NamingFuzzyWatchServiceListHolder.registerFuzzyWatcher(pattern, watcher)
 *       → initFuzzyWatchContextIfNeed(pattern) —— 创建 NamingFuzzyWatchContext
 *       → notifyFuzzyWatchSync() —— 放入 BlockingQueue 唤醒 worker 线程
 *
 *   Worker 线程（单线程，事件驱动）:
 *   ┌─────────────────────────────────────────────────────┐
 *   │ fuzzyWatchExecuteBell.poll(5s) —— 阻塞等待 bell     │
 *   │   │  notifyFuzzyWatchSync() 放入 bellItem 唤醒       │
 *   │   ▼                                                  │
 *   │ executeNamingFuzzyWatch()                            │
 *   │   ├─ 遍历 fuzzyMatchContextMap                       │
 *   │   ├─ 对已同步的 context → syncFuzzyWatchers()        │
 *   │   │   （本地将新增/删除的服务通知给各 watcher）        │
 *   │   ├─ 对未同步的 context → doExecuteNamingFuzzyWatch()│
 *   │   │   → buildFuzzyWatchNamingRequest()               │
 *   │   │   → namingGrpcClientProxy.fuzzyWatchRequest()    │
 *   │   │     WATCH_TYPE_WATCH / WATCH_TYPE_CANCEL_WATCH   │
 *   │   └─ 每 3 分钟做一次全量同步                          │
 *   └─────────────────────────────────────────────────────┘
 *
 *   服务端推送（gRPC 双向流）:
 *   NamingFuzzyWatchNotifyRequestHandler.requestReply()
 *     → NamingFuzzyWatchSyncRequest（初始化同步）
 *       → 发布 NamingFuzzyWatchNotifyEvent
 *         → 本类.onEvent() → context.notifyFuzzyWatchers()
 *           → FuzzyWatchEventWatcher.onEvent(FuzzyWatchChangeEvent)
 *     → NamingFuzzyWatchChangeNotifyRequest（增量变更）
 *       → 同上
 * }</pre>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>NamingFuzzyWatchContext</b> —— 单个 pattern 的上下文，维护已匹配服务列表 + watcher 集合</li>
 *   <li><b>NamingFuzzyWatchNotifyRequestHandler</b> —— gRPC 服务端推送处理，
 *       接收 SyncRequest/ChangeNotifyRequest，发布 NamingFuzzyWatchNotifyEvent</li>
 *   <li><b>NamingGrpcClientProxy</b> —— gRPC 代理，提供 fuzzyWatchRequest() RPC 调用</li>
 *   <li><b>NotifyCenter</b> —— 事件总线，发布/订阅 NamingFuzzyWatchNotifyEvent 和
 *       NamingFuzzyWatchLoadEvent</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：NacosNamingService.init() —— 用户创建 NamingService 时</li>
 *   <li><b>启动</b>：NamingGrpcClientProxy.start() 中调用 start()，
 *       启动单线程 Worker 从 BlockingQueue 消费同步任务</li>
 *   <li><b>gRPC 代理注册</b>：registerNamingGrpcClientProxy() 注入 NamingGrpcClientProxy</li>
 *   <li><b>关闭</b>：NamingClientProxyDelegate.shutdown() → shutdown() 注销订阅者 + 关闭线程池</li>
 * </ul>
 *
 * <h2>限流与错误处理</h2>
 * <p>当服务端返回 FUZZY_WATCH_PATTERN_OVER_LIMIT 或 FUZZY_WATCH_PATTERN_MATCH_COUNT_OVER_LIMIT 时，
 * 发布 NamingFuzzyWatchLoadEvent（非 WATCH_TYPE_CANCEL_WATCH），
 * 由 context.notifyOverLimitWatchers() 通知用户（60s 内抑制重复通知）。</p>
 *
 * @author tanyongquan
 */
public class NamingFuzzyWatchServiceListHolder extends SmartSubscriber implements Closeable {
    
    private static final Logger LOGGER = LogUtils.logger(NamingFuzzyWatchServiceListHolder.class);
    
    /**
     * 事件作用域，用于隔离不同 NamingService 实例的事件。
     * <p>在 {@link #onEvent(Event)} 中通过 event.scope().equals(notifierEventScope) 过滤。</p>
     */
    private String notifierEventScope;
    
    /**
     * gRPC 代理，用于向服务端发送模糊订阅 RPC 请求。
     * <p>由 start() 外部调用 registerNamingGrpcClientProxy() 注入。</p>
     */
    private NamingGrpcClientProxy namingGrpcClientProxy;
    
    /**
     * 模糊同步执行信号铃 —— 容量为 1 的阻塞队列，实现事件驱动同步。
     * <p>notifyFuzzyWatchSync() 放入一个 bellItem，Worker 线程 poll(5s) 获取后执行同步。
     * 单容量保证多次通知合并为一次执行（自动去抖）。</p>
     */
    private final BlockingQueue<Object> fuzzyWatchExecuteBell = new ArrayBlockingQueue<>(1);
    
    /**
     * 信号铃的标记对象，仅用于唤醒 Worker 线程。
     */
    private final Object bellItem = new Object();
    
    /**
     * 上次全量同步时间戳 —— 控制全量同步间隔（默认 3 分钟）。
     */
    private final AtomicLong fuzzyWatchLastAllSyncTime = new AtomicLong(System.currentTimeMillis());
    
    /**
     * 全量同步间隔：3 分钟。
     */
    private static final long FUZZY_LISTEN_ALL_SYNC_INTERNAL = 3 * 60 * 1000;
    
    /**
     * 单线程定时执行器 —— Worker 线程在此运行。
     */
    ScheduledExecutorService executorService;
    
    /**
     * 通配符模式 → 模糊监听上下文映射。
     * <p>key = groupKeyPattern（如 DEFAULT_GROUP@@svc-*），value = NamingFuzzyWatchContext。</p>
     */
    private Map<String, NamingFuzzyWatchContext> fuzzyMatchContextMap = new ConcurrentHashMap<>();
    
    public NamingFuzzyWatchServiceListHolder(String notifierEventScope) {
        this.notifierEventScope = notifierEventScope;
        NotifyCenter.registerSubscriber(this);
    }
    
    /**
     * shut down.
     */
    @Override
    public void shutdown() {
        // deregister subscriber which registered in constructor
        NotifyCenter.deregisterSubscriber(this);
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
        }
    }
    
    /**
     * start.
     */
    public void start() {
        
        executorService = Executors.newSingleThreadScheduledExecutor(
            new NameThreadFactory("com.alibaba.nacos.client.naming.fuzzy.watch.Worker"));
        executorService.submit(() -> {
            while (!executorService.isShutdown() && !executorService.isTerminated()) {
                try {
                    fuzzyWatchExecuteBell.poll(5L, TimeUnit.SECONDS);
                    if (executorService.isShutdown() || executorService.isTerminated()) {
                        continue;
                    }
                    executeNamingFuzzyWatch();
                } catch (Throwable e) {
                    LOGGER.error("[rpc-fuzzy-watch-execute] rpc fuzzy watch exception", e);
                    try {
                        Thread.sleep(50L);
                    } catch (InterruptedException interruptedException) {
                        //ignore
                    }
                    notifyFuzzyWatchSync();
                }
            }
        });
    }
    
    public void registerNamingGrpcClientProxy(NamingGrpcClientProxy namingGrpcClientProxy) {
        this.namingGrpcClientProxy = namingGrpcClientProxy;
    }
    
    public NamingFuzzyWatchContext getFuzzyWatchContext(String groupKeyPattern) {
        return fuzzyMatchContextMap.get(groupKeyPattern);
    }
    
    /**
     * Add a watcher to the context.
     *
     * @param watcher watcher to be added
     */
    public NamingFuzzyWatchContext registerFuzzyWatcher(String groupKeyPattern,
        FuzzyWatchEventWatcher watcher) {
        if (!namingGrpcClientProxy.isAbilitySupportedByServer(AbilityKey.SERVER_FUZZY_WATCH)) {
            throw new NacosRuntimeException(NacosException.SERVER_NOT_IMPLEMENTED,
                "Request Nacos server version is too low, not support fuzzy watch feature.");
        }
        NamingFuzzyWatchContext namingFuzzyWatchContext =
            initFuzzyWatchContextIfNeed(groupKeyPattern);
        namingFuzzyWatchContext.setDiscard(false);
        synchronized (namingFuzzyWatchContext) {
            FuzzyWatchEventWatcherWrapper fuzzyWatchEventWatcherWrapper =
                new FuzzyWatchEventWatcherWrapper(watcher);
            if (namingFuzzyWatchContext.getFuzzyWatchEventWatcherWrappers()
                .add(fuzzyWatchEventWatcherWrapper)) {
                LOGGER.info(" [add-watcher-ok] groupKeyPattern={}, watcher={},uuid={} ",
                    groupKeyPattern, watcher,
                    fuzzyWatchEventWatcherWrapper.getUuid());
                Set<String> receivedServiceKeys = namingFuzzyWatchContext.getReceivedServiceKeys();
                if (CollectionUtils.isNotEmpty(receivedServiceKeys)) {
                    for (String serviceKey : receivedServiceKeys) {
                        NamingFuzzyWatchNotifyEvent namingFuzzyWatchNotifyEvent =
                            NamingFuzzyWatchNotifyEvent.build(
                                notifierEventScope, groupKeyPattern, serviceKey,
                                ADD_SERVICE, FUZZY_WATCH_INIT_NOTIFY,
                                fuzzyWatchEventWatcherWrapper.getUuid());
                        NotifyCenter.publishEvent(namingFuzzyWatchNotifyEvent);
                    }
                }
            }
        }
        
        return namingFuzzyWatchContext;
    }
    
    /**
     * init fuzzy watch context.
     *
     * @param groupKeyPattern groupKeyPattern.
     * @return fuzzy context.
     */
    public NamingFuzzyWatchContext initFuzzyWatchContextIfNeed(String groupKeyPattern) {
        if (!fuzzyMatchContextMap.containsKey(groupKeyPattern)) {
            synchronized (fuzzyMatchContextMap) {
                if (fuzzyMatchContextMap.containsKey(groupKeyPattern)) {
                    return fuzzyMatchContextMap.get(groupKeyPattern);
                }
                LOGGER.info("[fuzzy-watch] init fuzzy watch context for pattern {}",
                    groupKeyPattern);
                fuzzyMatchContextMap.putIfAbsent(groupKeyPattern,
                    new NamingFuzzyWatchContext(notifierEventScope, groupKeyPattern));
                notifyFuzzyWatchSync();
            }
        }
        return fuzzyMatchContextMap.get(groupKeyPattern);
    }
    
    /**
     * remove fuzzy watch context for pattern.
     *
     * @param groupKeyPattern group key pattern.
     */
    public synchronized void removePatternMatchCache(String groupKeyPattern) {
        NamingFuzzyWatchContext namingFuzzyWatchContext = fuzzyMatchContextMap.get(groupKeyPattern);
        if (namingFuzzyWatchContext == null) {
            return;
        }
        if (namingFuzzyWatchContext.isDiscard()
            && namingFuzzyWatchContext.getFuzzyWatchEventWatcherWrappers()
                .isEmpty()) {
            LOGGER.info("[fuzzy-watch] remove fuzzy watch context for pattern {}", groupKeyPattern);
            fuzzyMatchContextMap.remove(groupKeyPattern);
        }
    }
    
    /**
     * notify sync fuzzy watch with server.
     */
    void notifyFuzzyWatchSync() {
        fuzzyWatchExecuteBell.offer(bellItem);
    }
    
    /**
     * Execute fuzzy listen configuration changes.
     *
     * <p>This method iterates through all fuzzy listen contexts and determines whether they need to be added or
     * removed based on their consistency with the server and discard status. It then calls the appropriate method to
     * execute the fuzzy listen operation.
     *
     * @throws NacosException If an error occurs during the execution of fuzzy listen configuration changes.
     */
    public void executeNamingFuzzyWatch() throws NacosException {
        
        // Obtain the current timestamp
        long now = System.currentTimeMillis();
        
        // Determine whether a full synchronization is needed
        boolean needAllSync =
            now - fuzzyWatchLastAllSyncTime.get() >= FUZZY_LISTEN_ALL_SYNC_INTERNAL;
        
        List<NamingFuzzyWatchContext> needSyncContexts = new ArrayList<>();
        // Iterate through all fuzzy listen contexts
        for (NamingFuzzyWatchContext context : fuzzyMatchContextMap.values()) {
            // Check if the context is consistent with the server
            if (context.isConsistentWithServer()) {
                context.syncFuzzyWatchers();
                // Skip if a full synchronization is not needed
                if (!needAllSync) {
                    continue;
                }
            }
            
            needSyncContexts.add(context);
        }
        
        // Execute fuzzy listen operation for addition
        doExecuteNamingFuzzyWatch(needSyncContexts);
        
        // Update last all sync time if a full synchronization was performed
        if (needAllSync) {
            fuzzyWatchLastAllSyncTime.set(now);
        }
    }
    
    public void resetConsistenceStatus() {
        fuzzyMatchContextMap.values()
            .forEach(fuzzyWatcherContext -> fuzzyWatcherContext.setConsistentWithServer(false));
    }
    
    /**
     * Execute fuzzy listen configuration changes for a specific map of contexts.
     *
     * <p>This method submits tasks to execute fuzzy listen operations asynchronously for the provided contexts. It
     * waits for all tasks to complete and logs any errors that occur.
     *
     * @param contextLists The map of contexts to execute fuzzy listen operations for.
     * @throws NacosException If an error occurs during the execution of fuzzy listen configuration changes.
     */
    private void doExecuteNamingFuzzyWatch(List<NamingFuzzyWatchContext> contextLists)
        throws NacosException {
        // Return if the context map is null or empty
        if (CollectionUtils.isEmpty(contextLists)) {
            return;
        }
        
        // Iterate through the context map and submit tasks for execution
        for (NamingFuzzyWatchContext entry : contextLists) {
            // Submit task for execution
            NamingFuzzyWatchRequest configFuzzyWatchRequest = buildFuzzyWatchNamingRequest(entry);
            try {
                
                // Execute the fuzzy listen operation
                NamingFuzzyWatchResponse listenResponse = namingGrpcClientProxy.fuzzyWatchRequest(
                    configFuzzyWatchRequest);
                if (listenResponse != null && listenResponse.isSuccess()) {
                    
                    if (configFuzzyWatchRequest.getWatchType().equals(WATCH_TYPE_CANCEL_WATCH)) {
                        removePatternMatchCache(entry.getGroupKeyPattern());
                    } else {
                        entry.setConsistentWithServer(true);
                    }
                    entry.clearOverLimitTs();
                }
                
            } catch (NacosException e) {
                // Log error and retry after a short delay
                
                if (FUZZY_WATCH_PATTERN_OVER_LIMIT.getCode() == e.getErrCode()
                    || FUZZY_WATCH_PATTERN_MATCH_COUNT_OVER_LIMIT.getCode() == e.getErrCode()) {
                    LOGGER.error(
                        " fuzzy watch pattern over limit,pattern ->{} ,fuzzy watch will be suppressed,msg={}",
                        entry.getGroupKeyPattern(), e.getErrMsg());
                    NamingFuzzyWatchLoadEvent namingFuzzyWatchLoadEvent =
                        NamingFuzzyWatchLoadEvent.buildEvent(
                            e.getErrCode(), entry.getGroupKeyPattern(), notifierEventScope);
                    NotifyCenter.publishEvent(namingFuzzyWatchLoadEvent);
                    
                } else {
                    LOGGER.error(" fuzzy watch request fail.", e);
                    
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException interruptedException) {
                        // Ignore interruption
                    }
                    // Retry notification
                    notifyFuzzyWatchSync();
                }
                
            }
        }
        
    }
    
    private NamingFuzzyWatchRequest buildFuzzyWatchNamingRequest(
        NamingFuzzyWatchContext namingFuzzyWatchContext) {
        NamingFuzzyWatchRequest namingFuzzyWatchRequest = new NamingFuzzyWatchRequest();
        namingFuzzyWatchRequest.setInitializing(namingFuzzyWatchContext.isInitializing());
        namingFuzzyWatchRequest.setNamespace(namingGrpcClientProxy.getNamespaceId());
        namingFuzzyWatchRequest
            .setReceivedGroupKeys(namingFuzzyWatchContext.getReceivedServiceKeys());
        namingFuzzyWatchRequest.setGroupKeyPattern(namingFuzzyWatchContext.getGroupKeyPattern());
        if (namingFuzzyWatchContext.isDiscard()
            && namingFuzzyWatchContext.getFuzzyWatchEventWatcherWrappers()
                .isEmpty()) {
            namingFuzzyWatchRequest.setWatchType(WATCH_TYPE_CANCEL_WATCH);
        } else {
            namingFuzzyWatchRequest.setWatchType(WATCH_TYPE_WATCH);
        }
        return namingFuzzyWatchRequest;
    }
    
    public Map<String, NamingFuzzyWatchContext> getFuzzyMatchContextMap() {
        return fuzzyMatchContextMap;
    }
    
    @Override
    public void onEvent(Event event) {
        
        if (event instanceof NamingFuzzyWatchNotifyEvent) {
            if (!event.scope().equals(notifierEventScope)) {
                return;
            }
            NamingFuzzyWatchNotifyEvent watchNotifyEvent = (NamingFuzzyWatchNotifyEvent) event;
            String changedType = watchNotifyEvent.getChangedType();
            String syncType = watchNotifyEvent.getSyncType();
            
            String serviceKey = watchNotifyEvent.getServiceKey();
            String pattern = watchNotifyEvent.getPattern();
            String watchUuid = watchNotifyEvent.getWatcherUuid();
            NamingFuzzyWatchContext namingFuzzyWatchContext = fuzzyMatchContextMap.get(pattern);
            if (namingFuzzyWatchContext == null) {
                return;
            }
            namingFuzzyWatchContext.notifyFuzzyWatchers(serviceKey, changedType, syncType,
                watchUuid);
        }
        if (event instanceof NamingFuzzyWatchLoadEvent) {
            if (!event.scope().equals(notifierEventScope)) {
                return;
            }
            
            NamingFuzzyWatchLoadEvent overLimitEvent = (NamingFuzzyWatchLoadEvent) event;
            NamingFuzzyWatchContext namingFuzzyWatchContext = fuzzyMatchContextMap.get(
                overLimitEvent.getGroupKeyPattern());
            if (namingFuzzyWatchContext == null) {
                return;
            }
            
            namingFuzzyWatchContext.notifyOverLimitWatchers(overLimitEvent.getCode());
        }
        
    }
    
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        result.add(NamingFuzzyWatchNotifyEvent.class);
        result.add(NamingFuzzyWatchLoadEvent.class);
        return result;
    }
    
    public String getNotifierEventScope() {
        return notifierEventScope;
    }
}
