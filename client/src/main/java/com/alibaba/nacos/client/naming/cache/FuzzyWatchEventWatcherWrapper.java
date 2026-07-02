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

import com.alibaba.nacos.api.naming.listener.FuzzyWatchEventWatcher;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * 模糊监听器封装 —— 为用户的 FuzzyWatchEventWatcher 附加同步状态元数据。
 *
 * <h2>核心职责</h2>
 * <p>将用户的 {@link FuzzyWatchEventWatcher} 实例包装起来，附加以下运行时状态：</p>
 * <ul>
 *   <li><b>uuid</b> —— 唯一标识，用于服务端推送时精确定位目标 watcher（避免广播给所有 watcher）</li>
 *   <li><b>syncVersion</b> —— 该 watcher 的增量同步版本号，
 *       与 NamingFuzzyWatchContext.syncVersion 对比判断是否需要差异化通知</li>
 *   <li><b>syncServiceKeys</b> —— 该 watcher 已知晓的服务 key 集合，
 *       用于去重：ADD_SERVICE 时跳过已通知的、DELETE_SERVICE 时跳过未知的</li>
 * </ul>
 *
 * <h2>equals / hashCode 语义</h2>
 * <p>两个 FuzzyWatchEventWatcherWrapper 相等当且仅当它们包装的 FuzzyWatchEventWatcher 是同一个对象。
 * 这意味着同一个 watcher 在同一个 context 中不会被重复添加（HashSet 去重）。</p>
 *
 * @author shiyiyue
 */
public class FuzzyWatchEventWatcherWrapper {
    
    /**
     * 该 watcher 的同步版本号 —— 与 NamingFuzzyWatchContext.syncVersion 比较判断是否需要同步。
     * <p>syncFuzzyWatchers() 中若 syncVersion 相等则跳过该 watcher；不相等则执行 diff 并更新。</p>
     */
    long syncVersion = 0;
    
    /**
     * 用户注册的模糊监听器实例。
     */
    FuzzyWatchEventWatcher fuzzyWatchEventWatcher;
    
    /**
     * 唯一标识符（UUID），用于服务端推送时精确定位 watcher。
     */
    String uuid = UUID.randomUUID().toString();
    
    public FuzzyWatchEventWatcherWrapper(FuzzyWatchEventWatcher fuzzyWatchEventWatcher) {
        this.fuzzyWatchEventWatcher = fuzzyWatchEventWatcher;
    }
    
    private Set<String> syncServiceKeys = new HashSet<>();
    
    final String getUuid() {
        return uuid;
    }
    
    Set<String> getSyncServiceKeys() {
        return syncServiceKeys;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FuzzyWatchEventWatcherWrapper that = (FuzzyWatchEventWatcherWrapper) o;
        return Objects.equals(fuzzyWatchEventWatcher, that.fuzzyWatchEventWatcher);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(fuzzyWatchEventWatcher);
    }
}
