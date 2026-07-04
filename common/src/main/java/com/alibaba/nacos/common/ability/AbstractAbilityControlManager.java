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

package com.alibaba.nacos.common.ability;

import com.alibaba.nacos.api.ability.constant.AbilityKey;
import com.alibaba.nacos.api.ability.constant.AbilityMode;
import com.alibaba.nacos.api.ability.constant.AbilityStatus;
import com.alibaba.nacos.api.ability.initializer.AbilityPostProcessor;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.spi.NacosServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nacos 能力控制中心（Capability Control Center）—— 当前节点「能力表」的运行时存储、查询与动态开关。
 *
 * <h2>核心职责</h2>
 * <p>维护一张按 {@link AbilityMode 能力模式} 分组的键值表
 *  {@code Map<AbilityMode, Map<String, Boolean>>}，每个条目表示 「某项能力是否开启」。
 *  提供三种操作：</p>
 * <ul>
 *   <li><b>查询</b> {@link #isCurrentNodeAbilityRunning(AbilityKey)} —— 判断某能力是否启用（返回
 *       {@link AbilityStatus SUPPORTED / NOT_SUPPORTED / UNKNOWN}）</li>
 *   <li><b>开关</b> {@link #enableCurrentNodeAbility(AbilityKey)} /
 *       {@link #disableCurrentNodeAbility(AbilityKey)} —— 运行时动态启用/禁用，并发布
 *       {@link AbilityUpdateEvent} 通知订阅者</li>
 *   <li><b>导出</b> {@link #getCurrentNodeAbilities(AbilityMode)} —— 取完整能力表，供 gRPC 连接建立时的
 *       能力协商（ability negotiation）使用</li>
 * </ul>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   构造器（子类实例化）
 *     └─ initAbilityTable()  [私有方法，L60-98]
 *          ├─ ① initCurrentNodeAbilities()  [抽象方法，子类提供出厂值]
 *          │    ├─ ServerAbilityControlManager  → SERVER + SDK_CLIENT + CLUSTER_CLIENT 三模式
 *          │    └─ ClientAbilityControlManager  → 仅 SDK_CLIENT 模式
 *          ├─ ② 校验：每个 AbilityKey 的 mode 必须与分组一致，否则抛 IllegalStateException
 *          ├─ ③ SPI 后处理：加载 AbilityPostProcessor（扩展点，当前无生产实现）
 *          └─ ④ 存入 currentNodeAbilities：AbilityKey → mapStr → String 键
 *
 *   运行时动态变更
 *     └─ AbilityConfigs.onEvent(ServerConfigChangeEvent)  [core 模块]
 *          └─ enable/disable → doTurn → publishEvent(AbilityUpdateEvent)
 *
 *   消费：gRPC 连接建立
 *     └─ GrpcClient.connectToServer()        [客户端] → getCurrentNodeAbilities → ConnectionSetupRequest
 *     └─ GrpcBiStreamRequestAcceptor.onNext() [服务端] → getCurrentNodeAbilities → SetupAckRequest
 * }</pre>
 *
 * <h2>SPI 发现与优先级竞争</h2>
 * <p>由 {@code com.alibaba.nacos.common.ability.discover.NacosAbilityManagerHolder} 通过 {@code NacosServiceLoader} 发现所有实现，
 * 按 {@link #getPriority()} 排序后取最高优先级者作为全局单例。
 * 服务端实现（{@code ServerAbilityControlManager}，priority=1）的优先级高于客户端实现
 * （{@code ClientAbilityControlManager}，priority=0），因此当两者同处 classpath 时服务端胜出。</p>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：SPI 加载 →
 *       {@code NacosAbilityManagerHolder.initAbilityControlManager()}  L74-87</li>
 *   <li><b>创建时机</b>：首次调用
 *       {@code NacosAbilityManagerHolder.getInstance()} 时懒加载</li>
 *   <li><b>销毁</b>：与 JVM 同生命周期，无显式 shutdown</li>
 * </ul>
 *
 * @author Daydreamer
 * @date 2022/7/12 19:18
 */
public abstract class AbstractAbilityControlManager {
    
    private static final Logger LOGGER =
        LoggerFactory.getLogger(AbstractAbilityControlManager.class);
    
    /**
     * 当前节点能力表 —— 核心数据结构。
     *
     * <p>外层 key 为 {@link AbilityMode 能力模式}（SERVER / SDK_CLIENT / CLUSTER_CLIENT），
     * 内层为 能力名（AbilityKey.name）→ 布尔值的映射。
     * 构造时一次性填充，后续通过 {@link #enableCurrentNodeAbility} / {@link #disableCurrentNodeAbility}
     * 动态修改。</p>
     *
     * <p>使用 ConcurrentHashMap 保证读操作无锁高性能、写操作线程安全。</p>
     */
    protected final Map<AbilityMode, Map<String, Boolean>> currentNodeAbilities =
        new ConcurrentHashMap<>();
    
    /**
     * 保护构造器 —— 仅子类可通过 SPI 实例化。
     *
     * <p>构造时做两件事：</p>
     * <ol>
     *   <li>注册 {@link AbilityUpdateEvent} 到通知中心的发布者通道（队列大小 16384）</li>
     *   <li>调用 {@link #initAbilityTable()} 完成能力表初始化</li>
     * </ol>
     */
    protected AbstractAbilityControlManager() {
        NotifyCenter.registerToPublisher(AbilityUpdateEvent.class, 16384);
        initAbilityTable();
    }
    
    /**
     * 能力表初始化 —— 从子类获取出厂能力值，经校验和后处理后存入
     * {@link #currentNodeAbilities}。
     *
     * <h3>四步流程</h3>
     * <ol>
     *   <li><b>获取出厂值</b>：调用抽象方法
     *       {@link #initCurrentNodeAbilities()} 获取子类声明的能力矩阵
     *       <ul>
     *         <li>服务端：{@code ServerAbilityControlManager} 返回 SERVER + SDK_CLIENT + CLUSTER_CLIENT 三模式</li>
     *         <li>客户端：{@code ClientAbilityControlManager} 仅返回 SDK_CLIENT 模式</li>
     *       </ul></li>
     *   <li><b>开发者校验（fail-fast）</b>：遍历每个 AbilityKey，确保其 {@code getMode()} 与所在
     *       分组的 AbilityMode 一致。不一致则立即抛 IllegalStateException —— 这是开发阶段的防御性检查，
     *       防止开发者将 SERVER 模式的能力误放入 SDK_CLIENT 分组</li>
     *   <li><b>SPI 后处理</b>：加载所有
     *       {@link com.alibaba.nacos.api.ability.initializer.AbilityPostProcessor AbilityPostProcessor}
     *       SPI 实现，对每个模式的能力表执行自定义处理（当前无生产实现，为未来的扩展预留钩子）</li>
     *   <li><b>写入存储</b>：通过 {@link AbilityKey#mapStr} 将 AbilityKey→Boolean 映射转换为
     *       String→Boolean 映射（因为 gRPC wire 上传输的是字符串键），存入 {@code currentNodeAbilities}</li>
     * </ol>
     *
     * <p>调用方：仅构造器 L54</p>
     */
    private void initAbilityTable() {
        LOGGER.info("Ready to get current node abilities...");
        // Step 1: 从子类获取出厂能力矩阵
        Map<AbilityMode, Map<AbilityKey, Boolean>> abilities = initCurrentNodeAbilities();
        // Step 2: 逐模式处理
        for (AbilityMode mode : AbilityMode.values()) {
            Map<AbilityKey, Boolean> abilitiesTable = abilities.get(mode);
            if (abilitiesTable == null) {
                continue;
            }
            // Step 2a: 开发者防御校验 —— 每个 AbilityKey 的 mode 必须与当前分组一致
            // 防止将 SERVER 模式的能力键误放入 SDK_CLIENT 分组
            for (AbilityKey abilityKey : abilitiesTable.keySet()) {
                if (!mode.equals(abilityKey.getMode())) {
                    LOGGER.error(
                        "You should not contain a other mode: {} in a specify mode: {} abilities set, error key: {}, please check again.",
                        abilityKey.getMode(), mode, abilityKey);
                    throw new IllegalStateException(
                        "Except mode: " + mode + " but " + abilityKey + " mode: "
                            + abilityKey.getMode()
                            + ", please check again.");
                }
            }
            // Step 2b: SPI 后处理 —— 加载 AbilityPostProcessor 对能力表做自定义加工
            // 当前无生产实现，为将来预留扩展钩子
            Collection<AbilityPostProcessor> processors =
                NacosServiceLoader.load(AbilityPostProcessor.class);
            for (AbilityPostProcessor processor : processors) {
                processor.process(mode, abilitiesTable);
            }
        }
        // Step 3: 写入 currentNodeAbilities 存储
        // AbilityKey.mapStr 将枚举键转换为 wire 上使用的字符串键
        Set<AbilityMode> abilityModes = abilities.keySet();
        LOGGER.info("Ready to initialize current node abilities, support modes: {}", abilityModes);
        for (AbilityMode abilityMode : abilityModes) {
            this.currentNodeAbilities
                .put(abilityMode,
                    new ConcurrentHashMap<>(AbilityKey.mapStr(abilities.get(abilityMode))));
        }
        LOGGER.info("Initialize current abilities finish...");
    }
    
    /**
     * 启用某项能力 —— 运行时动态开关（ON）。
     *
     * <p>从 {@link #currentNodeAbilities} 中取出该 AbilityKey 对应模式的分表，
     * 从中找到该能力名对应的条目，将其值置为 true，并发布 {@link AbilityUpdateEvent}。
     * 如果该模式的分表不存在则静默忽略。</p>
     *
     * <p>调用方：{@code AbilityConfigs.onEvent(ServerConfigChangeEvent)}（core 模块），
     * 监听到 {@code nacos.core.ability.*} 配置项变为 true 时触发。</p>
     *
     * @param abilityKey 要开启的能力键（必须是合法的 {@link AbilityKey} 枚举值）
     */
    public void enableCurrentNodeAbility(AbilityKey abilityKey) {
        Map<String, Boolean> abilities = this.currentNodeAbilities.get(abilityKey.getMode());
        if (abilities != null) {
            doTurn(abilities, abilityKey, true);
        }
    }
    
    /**
     * 执行能力开关的公共逻辑 —— 修改存储 + 发布事件。
     *
     * <p>两步原子操作（对同一张分表）：</p>
     * <ol>
     *   <li>修改能力表内对应条目的值（true/false）</li>
     *   <li>构建 {@link AbilityUpdateEvent} 并发布到 {@link NotifyCenter}，
     *       事件携带被修改的能力键、新值、以及该模式下的完整能力表快照（只读副本）</li>
     * </ol>
     *
     * @param abilities 某个 AbilityMode 下的分表（直接引用，非副本）
     * @param key       被修改的能力键
     * @param turn      true=开启, false=关闭
     */
    protected void doTurn(Map<String, Boolean> abilities, AbilityKey key, boolean turn) {
        LOGGER.info("Turn current node ability: {}, turn: {}", key, turn);
        abilities.put(key.getName(), turn);
        // notify event
        AbilityUpdateEvent abilityUpdateEvent = new AbilityUpdateEvent();
        abilityUpdateEvent.setTable(Collections.unmodifiableMap(abilities));
        abilityUpdateEvent.setOn(turn);
        abilityUpdateEvent.setAbilityKey(key);
        NotifyCenter.publishEvent(abilityUpdateEvent);
    }
    
    /**
     * 禁用某项能力 —— 运行时动态开关（OFF）。
     *
     * <p>语义与 {@link #enableCurrentNodeAbility} 对称，将值置为 false。
     * 调用方相同：{@code AbilityConfigs.onEvent()} 监听到配置项变为 false 时触发。</p>
     *
     * @param abilityKey 要关闭的能力键
     */
    public void disableCurrentNodeAbility(AbilityKey abilityKey) {
        Map<String, Boolean> abilities = this.currentNodeAbilities.get(abilityKey.getMode());
        if (abilities != null) {
            doTurn(abilities, abilityKey, false);
        }
    }
    
    /**
     * 查询某项能力是否已启用 —— 最常用的读接口。
     *
     * <p>返回三态枚举值：</p>
     * <ul>
     *   <li>{@link AbilityStatus#SUPPORTED} —— 该能力存在且值为 true</li>
     *   <li>{@link AbilityStatus#NOT_SUPPORTED} —— 该能力存在但值为 false</li>
     *   <li>{@link AbilityStatus#UNKNOWN} —— 该能力的模式不存在，或该能力名在分表中不存在</li>
     * </ul>
     *
     * <p>调用方举例：</p>
     * <ul>
     *   <li>{@code DistroClientTransportAgent.checkTargetServerStatusUnhealthy()} ——
     *       检查目标节点是否支持远程连接，决定是否跳过</li>
     *   <li>{@code LockGrpcClient.isAbilitySupportedByServer()} ——
     *       检查服务端是否支持分布式锁，决定是否执行锁操作</li>
     * </ul>
     *
     * @param abilityKey 要查询的能力键
     * @return 三态支持状态（SUPPORTED / NOT_SUPPORTED / UNKNOWN）
     */
    public AbilityStatus isCurrentNodeAbilityRunning(AbilityKey abilityKey) {
        Map<String, Boolean> abilities = currentNodeAbilities.get(abilityKey.getMode());
        if (abilities != null) {
            Boolean support = abilities.get(abilityKey.getName());
            if (support != null) {
                return support ? AbilityStatus.SUPPORTED : AbilityStatus.NOT_SUPPORTED;
            }
        }
        return AbilityStatus.UNKNOWN;
    }
    
    /**
     * 初始化当前节点的能力矩阵 —— 模板方法，由子类提供实现。
     *
     * <p>子类必须返回一个 Map，key 为它要声明的 {@link AbilityMode}，
     * value 为 能力键→默认开关值 的映射。当前两个生产实现：</p>
     * <ul>
     *   <li>{@code ServerAbilityControlManager}：返回 SERVER（6 项能力，全部 true）
     *       + SDK_CLIENT + CLUSTER_CLIENT 三模式，服务端覆盖全场景</li>
     *   <li>{@code ClientAbilityControlManager}：仅返回 SDK_CLIENT 模式（4 项能力），
     *       客户端只声明自己能做什么</li>
     * </ul>
     *
     * <p>调用方：{@link #initAbilityTable()} L63，在构造器中被调用一次。</p>
     *
     * @return 能力矩阵（不可变 HashMap，只读一次）
     */
    protected abstract Map<AbilityMode, Map<AbilityKey, Boolean>> initCurrentNodeAbilities();
    
    /**
     * 获取当前节点在指定模式下的完整能力表 —— gRPC 能力协商的数据源。
     *
     * <p>返回只读副本（{@code Collections.unmodifiableMap}），防止外部直接修改内部状态。
     * 如果该模式不在 {@link #currentNodeAbilities} 中（例如纯 SDK 客户端没有 SERVER 模式），
     * 返回空 Map（非 null）。</p>
     *
     * <p>调用方（gRPC 连接建立时的能力交换）：</p>
     * <ul>
     *   <li><b>客户端侧</b>：{@code GrpcClient.connectToServer()}
     *       → 将 SDK_CLIENT 能力表打包进 {@code ConnectionSetupRequest} 发给服务端</li>
     *   <li><b>服务端侧</b>：{@code GrpcBiStreamRequestAcceptor.onNext()}
     *       → 将 SERVER 能力表打包进 {@code SetupAckRequest} 回给客户端</li>
     * </ul>
     *
     * @param mode 要查询的能力模式（SERVER / SDK_CLIENT / CLUSTER_CLIENT）
     * @return 只读能力表快照，不存在则返回空 Map
     */
    public Map<String, Boolean> getCurrentNodeAbilities(AbilityMode mode) {
        Map<String, Boolean> abilities = currentNodeAbilities.get(mode);
        if (abilities != null) {
            return Collections.unmodifiableMap(abilities);
        }
        return Collections.emptyMap();
    }
    
    /**
     * 返回此能力管理器的优先级 —— 决定全局单例竞争中的胜者。
     *
     * <p>在 {@link NacosAbilityManagerHolder} 中，通过 SPI 发现所有实现后按此值升序排序，
     * 取最高优先级者。当前分配：</p>
     * <ul>
     *   <li>{@code ServerAbilityControlManager} = 1 —— 服务端，更高优先级</li>
     *   <li>{@code ClientAbilityControlManager} = 0 —— 客户端，最低优先级</li>
     * </ul>
     * <p>因此当客户端 SDK 作为依赖引入服务端项目时，服务端实现自动胜出。</p>
     *
     * @return 优先级整数值，越大优先级越高
     */
    public abstract int getPriority();
    
    /**
     * 能力变更事件 —— 当某项能力被启用或禁用时发布，通知关注者更新自身行为。
     *
     * <p>由 {@link #doTurn} 在每个 AbilityMode 的分表内修改后发布。
     * 携带三个关键信息：</p>
     * <ul>
     *   <li>{@link #getAbilityKey()} —— 被修改的是哪个能力</li>
     *   <li>{@link #isOn()} —— 新值是开还是关</li>
     *   <li>{@link #getAbilityTable()} —— 该模式下修改后的完整能力表（只读快照）</li>
     * </ul>
     *
     * <p>当前无已知的生产订阅者（仅测试中验证），但事件机制已就绪，
     * 可通过 {@link com.alibaba.nacos.common.notify.listener.Subscriber Subscriber} 订阅。</p>
     */
    public static class AbilityUpdateEvent extends Event {
        
        private static final long serialVersionUID = -1232411212311111L;
        
        private AbilityKey abilityKey;
        
        private boolean isOn;
        
        private Map<String, Boolean> table;
        
        private AbilityUpdateEvent() {
        }
        
        public Map<String, Boolean> getAbilityTable() {
            return table;
        }
        
        public void setTable(Map<String, Boolean> abilityTable) {
            this.table = abilityTable;
        }
        
        public AbilityKey getAbilityKey() {
            return abilityKey;
        }
        
        public void setAbilityKey(AbilityKey abilityKey) {
            this.abilityKey = abilityKey;
        }
        
        public boolean isOn() {
            return isOn;
        }
        
        public void setOn(boolean on) {
            isOn = on;
        }
    }
}
