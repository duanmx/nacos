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

package com.alibaba.nacos.core.ability.config;

import com.alibaba.nacos.api.ability.constant.AbilityKey;
import com.alibaba.nacos.api.ability.register.impl.ServerAbilities;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.ability.AbstractAbilityControlManager;
import com.alibaba.nacos.common.ability.discover.NacosAbilityManagerHolder;
import com.alibaba.nacos.common.event.ServerConfigChangeEvent;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 能力配置动态桥接器 —— 将运行时的 {@code application.properties} 变更翻译为
 * {@link AbstractAbilityControlManager} 的能力开关调用。
 *
 * <h2>核心职责</h2>
 * <p>监听 {@link ServerConfigChangeEvent}，扫描所有以 {@value #PREFIX} 开头的配置项，
 * 根据其布尔值调用 {@link AbstractAbilityControlManager#enableCurrentNodeAbility}
 * 或 {@link AbstractAbilityControlManager#disableCurrentNodeAbility}。</p>
 *
 * <p>一句话：它是「配置文件 → 能力开关」的翻译器，让能力表可以在不重启服务的情况下动态调整。</p>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   application.properties 磁盘文件变更
 *     └─ NacosCoreStartUp.registerWatcher().FileWatcher.onChange()  [L183-191]
 *          └─ EnvUtil.loadProperties()  重新加载配置到内存
 *          └─ NotifyCenter.publishEvent(ServerConfigChangeEvent.newEvent())  [L188]
 *               │
 *               ▼
 *   AbilityConfigs.onEvent(ServerConfigChangeEvent)  [L62-81]
 *     ├─ ① 扫描 PREFIX + abilityKey.getName() 对应的环境变量值
 *     │      例如：nacos.core.ability.fuzzyWatch = true
 *     │      能读取则记录新值，读不到保留旧值（不覆盖）
 *     └─ ② refresh(newValues)  [L86-95]
 *          ├─ val=true  → abilityHandlerRegistry.enableCurrentNodeAbility(abilityKey)
 *          └─ val=false → abilityHandlerRegistry.disableCurrentNodeAbility(abilityKey)
 *                          ↓
 *                    AbstractAbilityControlManager.doTurn()
 *                    修改 currentNodeAbilities 存储 + 发布 AbilityUpdateEvent
 * }</pre>
 *
 * <h2>与启动时初始化的配合</h2>
 * <p>{@code ServerAbilityControlManager} 在构造时也使用 {@value #PREFIX} 前缀读取配置，
 * 那是<b>启动时一次性</b>的（配置文件 → 出厂默认覆盖）。而本类是<b>运行时持续</b>的
 * （磁盘文件变更 → 重新扫描 → 开关）。两者共享同一个 {@value #PREFIX} 前缀。</p>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：Spring 组件扫描发现 {@code @Configuration} 注解，自动实例化</li>
 *   <li><b>活跃时机</b>：构造器调用 {@code NotifyCenter.registerSubscriber(this)} 后开始监听</li>
 *   <li><b>触发条件</b>：当 {@code application.properties} 被修改并触发
 *       {@code NacosCoreStartUp} 中文件监听器的 {@code onChange} 回调时</li>
 *   <li><b>销毁</b>：与 Spring 容器同生命周期</li>
 * </ul>
 *
 * @author Daydreamer
 * @description Dynamically load ability from config
 * @date 2022/8/31 12:27
 */
@Configuration
public class AbilityConfigs extends Subscriber<ServerConfigChangeEvent> {
    
    /**
     * 配置键前缀 —— 所有能力开关配置项的公共前缀。
     *
     * <p>完整键格式：{@code nacos.core.ability.} + AbilityKey.name。
     * 例如 {@code nacos.core.ability.fuzzyWatch}。
     * 与 {@code ServerAbilityControlManager.initCurrentNodeAbilities()} 共用此常量。</p>
     */
    public static final String PREFIX = "nacos.core.ability.";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AbilityConfigs.class);
    
    private final Set<AbilityKey> serverAbilityKeys = new ConcurrentHashSet<>();
    
    private AbstractAbilityControlManager abilityHandlerRegistry =
        NacosAbilityManagerHolder.getInstance();
    
    /**
     * 构造器 —— 初始化能力键集合并注册为事件订阅者。
     *
     * <p>两步初始化：</p>
     * <ol>
     *   <li>从 {@code ServerAbilities.getStaticAbilities()} 获取服务端出厂声明的所有
     *       {@link AbilityKey}，存入 {@code serverAbilityKeys} —— 这个集合决定了后续
     *       {@link #onEvent} 扫描哪些配置项</li>
     *   <li>通过 {@link NotifyCenter#registerSubscriber} 将自己注册为
     *       {@link ServerConfigChangeEvent} 的订阅者</li>
     * </ol>
     *
     * <p>调用方：Spring 容器通过 {@code @Configuration} 注解自动扫描实例化。</p>
     */
    public AbilityConfigs() {
        // Step 1: 从出厂声明获取要监控的能力键集合
        serverAbilityKeys.addAll(ServerAbilities.getStaticAbilities().keySet());
        // Step 2: 注册为 ServerConfigChangeEvent 订阅者
        NotifyCenter.registerSubscriber(this);
    }
    
    /**
     * 配置变更回调 —— 当 {@code application.properties} 被修改时触发。
     *
     * <h3>触发链</h3>
     * <pre>{@code
     *   NacosCoreStartUp.registerWatcher()  [L179-198]
     *     └─ FileWatcher.onChange(FileChangeEvent)  [L183-191]
     *          └─ EnvUtil.loadProperties()  重新读文件 → 更新内存
     *          └─ NotifyCenter.publishEvent(ServerConfigChangeEvent.newEvent())
     *               └─ 本方法被回调
     * }</pre>
     *
     * <h3>处理逻辑</h3>
     * <ol>
     *   <li>遍历 {@code serverAbilityKeys} 中所有能力键</li>
     *   <li>对每个键构造完整配置名 {@code PREFIX + abilityKey.getName()}，
     *       调用 {@link EnvUtil#getProperty} 读取最新值</li>
     *   <li>仅处理配置中明确存在的键（非 null）—— 未配置的能力保留当前状态</li>
     *   <li>将变更的值通过 {@link #refresh} 下发到能力控制管理器</li>
     * </ol>
     *
     * @param event 配置变更事件（空事件，不含数据，仅是触发信号）
     */
    @Override
    public void onEvent(ServerConfigChangeEvent event) {
        // Step 1: 扫描 PREFIX + 每个能力名 对应的环境变量
        Map<AbilityKey, Boolean> newValues = new HashMap<>(serverAbilityKeys.size());
        serverAbilityKeys.forEach(abilityKey -> {
            String key = PREFIX + abilityKey.getName();
            try {
                Boolean property = EnvUtil.getProperty(key, Boolean.class);
                if (property != null) {
                    newValues.put(abilityKey, property);
                }
            } catch (Exception e) {
                LOGGER.warn(
                    "Update ability config from env failed, use old val, ability : {} , because : {}",
                    key, e);
            }
        });
        // Step 2: 下发变更
        refresh(newValues);
    }
    
    /**
     * 将扫描到的新配置值下发到能力控制管理器 —— 「扫描 → 执行」的第二步。
     *
     * <p>对每个变更的键：</p>
     * <ul>
     *   <li>值为 true  → {@code enableCurrentNodeAbility}  （开启能力）</li>
     *   <li>值为 false → {@code disableCurrentNodeAbility} （关闭能力）</li>
     * </ul>
     *
     * <p>注意：只处理 {@code newValues} 中存在的键 —— 配置文件中未出现的键不会被触碰，
     * 保持运行时的当前状态不变。</p>
     *
     * <p>调用方：仅 {@link #onEvent} L143</p>
     *
     * @param newValues 从配置中解析出的能力键 → 新值 映射（仅包含配置文件中明确配置的键）
     */
    private void refresh(Map<AbilityKey, Boolean> newValues) {
        newValues.forEach((abilityKey, val) -> {
            if (val) {
                abilityHandlerRegistry.enableCurrentNodeAbility(abilityKey);
            } else {
                abilityHandlerRegistry.disableCurrentNodeAbility(abilityKey);
            }
        });
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return ServerConfigChangeEvent.class;
    }
    
    @JustForTest
    protected Set<AbilityKey> getServerAbilityKeys() {
        return serverAbilityKeys;
    }
    
    @JustForTest
    protected void setAbilityHandlerRegistry(AbstractAbilityControlManager abilityHandlerRegistry) {
        this.abilityHandlerRegistry = abilityHandlerRegistry;
    }
    
}
