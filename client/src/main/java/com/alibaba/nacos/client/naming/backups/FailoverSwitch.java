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

package com.alibaba.nacos.client.naming.backups;

/**
 * 故障转移开关 —— 一个简单的布尔标志封装。
 *
 * <h2>核心职责</h2>
 * <p>由 FailoverDataSource（如 DiskFailoverDataSource）从磁盘文件
 * {@code {cacheDir}/failover/00-00---000-NACOS_SWITCH} 读取，
 * 内容为 "1"=开启、"0"=关闭。
 * FailoverReactor 每 5s 轮询此开关状态决定是否进入 failover 模式。</p>
 *
 * @author zongkang.guo
 */
public class FailoverSwitch {
    
    /**
     * 故障转移开关状态 —— true=开启，false=关闭。
     */
    private final boolean enabled;
    
    public boolean getEnabled() {
        return enabled;
    }
    
    public FailoverSwitch(boolean enabled) {
        this.enabled = enabled;
    }
}
