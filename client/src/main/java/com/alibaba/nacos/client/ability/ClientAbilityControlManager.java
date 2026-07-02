/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.ability;

import com.alibaba.nacos.api.ability.constant.AbilityKey;
import com.alibaba.nacos.api.ability.constant.AbilityMode;
import com.alibaba.nacos.api.ability.register.impl.SdkClientAbilities;
import com.alibaba.nacos.common.ability.AbstractAbilityControlManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Nacos 客户端能力协商管理器 —— 声明当前 SDK 客户端所具备的能力集合，
 * 用于与服务端进行能力协商（ability negotiation）。
 *
 * <h2>核心职责</h2>
 * <p>继承自 {@link AbstractAbilityControlManager}，负责声明客户端 SDK
 * 支持哪些功能特性（如分布式锁、模糊监听等）。服务端通过 gRPC 连接建立时的
 * 能力协商流程获知客户端能力，从而决定是否启用特定功能。</p>
 *
 * <h2>初始化过程</h2>
 * <p>构造时通过 {@link SdkClientAbilities#getStaticAbilities()} 获取
 * 当前 SDK 版本的静态能力表（硬编码的能力矩阵），并注册为
 * {@link AbilityMode#SDK_CLIENT} 类型的能力集。</p>
 *
 * <h2>优先级</h2>
 * <p>返回 0 表示最低优先级 —— 如果服务端能力管理器存在，应优先使用服务端的能力声明。</p>
 *
 * <h2>使用场景</h2>
 * <p>在 gRPC 连接建立时，客户端将能力表发送给服务端。例如：
 * {@link LockGrpcClient#lock(LockInstance)} 在执行锁操作前，
 * 通过 {@code isAbilitySupportedByServer()} 检查服务端是否支持分布式锁功能。</p>
 *
 * @author Daydreamer
 * @date 2022/7/13 13:38
 */
public class ClientAbilityControlManager extends AbstractAbilityControlManager {
    
    public ClientAbilityControlManager() {
    }
    
    /**
     * 初始化当前节点（客户端 SDK）的能力表。
     *
     * @return 以 SDK_CLIENT 模式注册的能力矩阵
     */
    @Override
    protected Map<AbilityMode, Map<AbilityKey, Boolean>> initCurrentNodeAbilities() {
        Map<AbilityMode, Map<AbilityKey, Boolean>> abilities = new HashMap<>(1);
        abilities.put(AbilityMode.SDK_CLIENT, SdkClientAbilities.getStaticAbilities());
        return abilities;
    }
    
    /**
     * 返回能力管理器的优先级。
     *
     * @return 0 表示最低优先级，服务端能力声明优先于客户端
     */
    @Override
    public int getPriority() {
        // if server ability manager exist, you should choose the server one
        return 0;
    }
    
}
