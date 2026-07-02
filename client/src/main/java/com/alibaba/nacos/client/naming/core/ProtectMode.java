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

/**
 * 保护模式配置 —— 客户端健康实例保护阈值。
 *
 * <h2>核心职责</h2>
 * <p>保存保护阈值（protectThreshold），当健康实例比例低于此阈值时，
 * 服务端启用保护模式，客户端返回所有实例（含不健康实例）而非仅健康实例，
 * 防止因网络分区导致服务不可用。默认阈值 0.8（80%）。</p>
 *
 * <h2>使用方式</h2>
 * <p>该值由服务端通过 ServiceInfo 下发给客户端，客户端在
 * {@link com.alibaba.nacos.client.naming.core.Balancer} 等组件中引用此阈值做保护判断。</p>
 *
 * @author nkorange
 */
public class ProtectMode {
    
    /**
     * 保护阈值 —— 健康实例比例低于此值时启用保护模式，默认 0.8（80%）。
     */
    private float protectThreshold;
    
    public ProtectMode() {
        this.protectThreshold = 0.8F;
    }
    
    public float getProtectThreshold() {
        return protectThreshold;
    }
    
    public void setProtectThreshold(float protectThreshold) {
        this.protectThreshold = protectThreshold;
    }
}
