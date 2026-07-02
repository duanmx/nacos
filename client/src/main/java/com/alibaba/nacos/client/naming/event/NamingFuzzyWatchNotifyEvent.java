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

package com.alibaba.nacos.client.naming.event;

import com.alibaba.nacos.common.notify.Event;

/**
 * 模糊监听服务变更通知事件 —— 从服务端推送过来的服务级变更通知。
 *
 * <h2>核心职责</h2>
 * <p>作为 Naming 模糊监听系统中服务端推送事件的载体，承载服务级变更信息
 * （ADD_SERVICE / DELETE_SERVICE），由 NamingFuzzyWatchNotifyRequestHandler 发布，
 * 由 NamingFuzzyWatchServiceListHolder 消费。</p>
 *
 * <h2>字段说明</h2>
 * <ul>
 *   <li><b>scope</b> —— 事件作用域，用于 NamingFuzzyWatchServiceListHolder.onEvent() 中的 scope 过滤</li>
 *   <li><b>pattern</b> —— 通配符模式，用于定位到具体的 NamingFuzzyWatchContext</li>
 *   <li><b>serviceKey</b> —— 变更的服务 key（namespace@@group@@serviceName）</li>
 *   <li><b>changedType</b> —— 变更类型：ADD_SERVICE / DELETE_SERVICE</li>
 *   <li><b>syncType</b> —— 同步类型：FUZZY_WATCH_INIT_NOTIFY（初始同步）或
 *       FUZZY_WATCH_DIFF_SYNC_NOTIFY（增量同步）</li>
 *   <li><b>watcherUuid</b> —— 目标 watcher UUID，非空时只通知该 watcher；
 *       为空时通知 context 中所有 watcher</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>发布者</b>：NamingFuzzyWatchNotifyRequestHandler.requestReply()（接收 gRPC 推送时）</li>
 *   <li><b>发布者</b>：NamingFuzzyWatchServiceListHolder.registerFuzzyWatcher()（首次注册时回放已有服务）</li>
 *   <li><b>订阅者</b>：NamingFuzzyWatchServiceListHolder.onEvent()</li>
 * </ul>
 *
 * @author tanyongquan
 */
public class NamingFuzzyWatchNotifyEvent extends Event {
    
    private final String scope;
    
    /**
     * 目标 watcher UUID —— 非空时只通知该 watcher（精准投递）；为空时广播给 context 中所有 watcher。
     */
    private String watcherUuid;
    
    /**
     * 变更的服务 key（namespace@@group@@serviceName）。
     */
    private String serviceKey;
    
    /**
     * 通配符模式，用于定位 NamingFuzzyWatchContext。
     */
    private String pattern;
    
    /**
     * 变更类型：ADD_SERVICE / DELETE_SERVICE。
     */
    private final String changedType;
    
    /**
     * 同步类型：FUZZY_WATCH_INIT_NOTIFY（初始化）/ FUZZY_WATCH_DIFF_SYNC_NOTIFY（增量）。
     */
    private final String syncType;
    
    private NamingFuzzyWatchNotifyEvent(String scope, String pattern, String serviceKey,
        String changedType,
        String syncType, String watcherUuid) {
        this.scope = scope;
        this.pattern = pattern;
        this.serviceKey = serviceKey;
        this.changedType = changedType;
        this.syncType = syncType;
        this.watcherUuid = watcherUuid;
    }
    
    public static NamingFuzzyWatchNotifyEvent build(String eventScope, String pattern,
        String serviceKey,
        String changedType, String syncType) {
        return new NamingFuzzyWatchNotifyEvent(eventScope, pattern, serviceKey, changedType,
            syncType, null);
    }
    
    public static NamingFuzzyWatchNotifyEvent build(String eventScope, String pattern,
        String serviceKey,
        String changedType, String syncType, String watcherUuid) {
        return new NamingFuzzyWatchNotifyEvent(eventScope, pattern, serviceKey, changedType,
            syncType, watcherUuid);
    }
    
    public String getPattern() {
        return pattern;
    }
    
    public String getChangedType() {
        return changedType;
    }
    
    @Override
    public String scope() {
        return this.scope;
    }
    
    public String getWatcherUuid() {
        return watcherUuid;
    }
    
    public String getServiceKey() {
        return serviceKey;
    }
    
    public String getScope() {
        return scope;
    }
    
    public String getSyncType() {
        return syncType;
    }
}
