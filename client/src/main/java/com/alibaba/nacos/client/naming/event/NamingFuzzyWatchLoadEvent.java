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
 * 模糊监听超限事件 —— 当服务端返回通配符超限错误时通知用户。
 *
 * <h2>触发场景</h2>
 * <p>当模糊订阅同步（executeNamingFuzzyWatch）向服务端发送 RPC 请求时，
 * 如果服务端返回以下错误码，则发布此事件：
 * <ul>
 *   <li><b>FUZZY_WATCH_PATTERN_OVER_LIMIT</b> —— 通配符模式数量超过服务端限制</li>
 *   <li><b>FUZZY_WATCH_PATTERN_MATCH_COUNT_OVER_LIMIT</b> —— 某个模式匹配的服务数量超过限制</li>
 * </ul>
 * </p>
 *
 * <h2>消费流程</h2>
 * <pre>{@code
 *   NamingFuzzyWatchServiceListHolder.doExecuteNamingFuzzyWatch()
 *     → 捕获 NacosException（超限错误码）
 *       → NamingFuzzyWatchLoadEvent.buildEvent()
 *         → NotifyCenter.publishEvent()
 *           → NamingFuzzyWatchServiceListHolder.onEvent()
 *             → context.notifyOverLimitWatchers(code)
 *               → 遍历所有实现 FuzzyWatchLoadWatcher 的 watcher
 *                 → onPatternOverLimit() / onServiceReachUpLimit()
 * }</pre>
 *
 * <h2>限流</h2>
 * <p>notifyOverLimitWatchers() 内部有 60s 抑制期（patternLimitSuppressed），
 * 避免超限错误高频触发导致日志/通知轰炸。</p>
 *
 * @author shiyiyue
 */
public class NamingFuzzyWatchLoadEvent extends Event {
    
    /**
     * 事件作用域，用于 scope 过滤。
     */
    private String eventScope;
    
    /**
     * 触发超限的通配符模式。
     */
    private String groupKeyPattern;
    
    /**
     * 错误码：FUZZY_WATCH_PATTERN_OVER_LIMIT 或 FUZZY_WATCH_PATTERN_MATCH_COUNT_OVER_LIMIT。
     */
    private int code;
    
    /**
     * Constructs a new FuzzyListenNotifyEvent with the specified group, dataId, and type.
     *
     * @param code            The type of notification.
     * @param groupKeyPattern The groupKeyPattern of notification.
     */
    private NamingFuzzyWatchLoadEvent(int code, String groupKeyPattern, String eventScope) {
        this.code = code;
        this.groupKeyPattern = groupKeyPattern;
        this.eventScope = eventScope;
    }
    
    /**
     * Builds a new FuzzyListenNotifyEvent with the specified group, dataId, and type.
     *
     * @param groupKeyPattern The groupKey of the configuration.
     * @return A new FuzzyListenNotifyEvent instance.
     */
    public static NamingFuzzyWatchLoadEvent buildEvent(int code, String groupKeyPattern,
        String scope) {
        return new NamingFuzzyWatchLoadEvent(code, groupKeyPattern, scope);
    }
    
    @Override
    public String scope() {
        return eventScope;
    }
    
    public String getGroupKeyPattern() {
        return groupKeyPattern;
    }
    
    public int getCode() {
        return code;
    }
}
