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

package com.alibaba.nacos.client.utils;

import com.alibaba.nacos.api.utils.json.JsonUtils;
import com.alibaba.nacos.client.auth.ram.utils.SpasAdapter;
import com.alibaba.nacos.common.json.JsonAdapterLogUtils;

/**
 * 预初始化工具类 —— 在后台线程中异步加载耗时组件，减少客户端首次调用的延迟。
 *
 * <h2>预加载的组件</h2>
 * <ul>
 *   <li><b>JsonUtils</b>：JSON 序列化适配器的首次初始化可能耗时数百毫秒，
 *       提前触发预加载避免首次 API 调用卡顿</li>
 *   <li><b>SpasAdapter</b>：RAM 鉴权插件在缺少显式配置时需从环境变量/系统属性
 *       获取 AK/SK，提前触发避免首次登录超时</li>
 * </ul>
 *
 * <h2>调用方式</h2>
 * <p>在客户端服务初始化完成后，通过 {@link #asyncPreLoadCostComponent()}
 * 启动一个后台守护线程执行预加载。由于是异步的，不影响主流程返回。</p>
 *
 * @author xiweng.yy
 */
public class PreInitUtils {
    
    /**
     * Async pre load cost component.
     */
    public static void asyncPreLoadCostComponent() {
        Thread preLoadThread = new Thread(PreInitUtils::preLoadCostComponent);
        preLoadThread.start();
    }
    
    static void preLoadCostComponent() {
        // JSON adapter initialization may cost hundreds milliseconds on first use.
        JsonUtils.preload();
        JsonAdapterLogUtils.logSelectedAdapter();
        // Ram auth plugin will try to get credential from env and system when leak input identity by properties.
        SpasAdapter.getAk();
    }
}
