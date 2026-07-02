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

package com.alibaba.nacos.client.naming.remote.gprc;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.remote.request.NamingFuzzyWatchChangeNotifyRequest;
import com.alibaba.nacos.api.naming.remote.request.NamingFuzzyWatchSyncRequest;
import com.alibaba.nacos.api.naming.remote.response.NamingFuzzyWatchChangeNotifyResponse;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.client.naming.cache.NamingFuzzyWatchContext;
import com.alibaba.nacos.client.naming.cache.NamingFuzzyWatchServiceListHolder;
import com.alibaba.nacos.client.naming.event.NamingFuzzyWatchNotifyEvent;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.remote.client.Connection;
import com.alibaba.nacos.common.remote.client.ServerRequestHandler;
import com.alibaba.nacos.common.utils.FuzzyGroupKeyPattern;

import java.util.Collection;

import static com.alibaba.nacos.api.common.Constants.FUZZY_WATCH_RESOURCE_CHANGED;

/**
 * 模糊监听（Fuzzy Watch）通知处理器 —— 接收并处理服务端通过 gRPC 双向流推送的
 * 模糊匹配服务变更通知。
 *
 * <p>相当于 Nacos 3.0+ 模糊监听功能的客户端入口。与常规的
 * {@link NamingPushRequestHandler}（处理精确订阅的实例变更推送）不同，
 * 本处理器专门处理基于通配符模式的服务级别变更（哪些服务出现/消失），
 * 而非实例级别的变更。</p>
 *
 * <h2>处理的两种通知类型</h2>
 * <ol>
 *   <li><b>NamingFuzzyWatchSyncRequest（同步通知）</b>：
 *     <ul>
 *       <li>INIT_NOTIFY —— 首次建立模糊订阅时，服务端回传目前已匹配的全部服务列表</li>
 *       <li>DIFF_SYNC_NOTIFY —— 增量同步，服务端推送自上次同步以来的新增/删除服务</li>
 *       <li>FINISH_INIT_NOTIFY —— 初始化完成标记，后续只接收增量变更</li>
 *     </ul>
 *   </li>
 *   <li><b>NamingFuzzyWatchChangeNotifyRequest（变更通知）</b>：
 *     服务端检测到有服务匹配通配符发生了变化（ADD_SERVICE / INSTANCE_CHANGED / DELETE_SERVICE）
 *     → 推送给所有匹配该通配符的模糊订阅客户端
 *   </li>
 * </ol>
 *
 * <h2>数据流转</h2>
 * <pre>{@code
 *   Nacos Server
 *     │ gRPC 双向流推送
 *     ▼
 *   NamingFuzzyWatchNotifyRequestHandler.requestReply()
 *     │ 两条处理路径:
 *     │
 *     ├─ NamingFuzzyWatchSyncRequest（同步）
 *     │   ├─ 从 NamingFuzzyWatchServiceListHolder 查找匹配的 FuzzyWatchContext
 *     │   ├─ 去重检查（addReceivedServiceKey，防止 change event 先于 init event 到达）
 *     │   ├─ 发布 NamingFuzzyWatchNotifyEvent → NamingFuzzyWatchServiceListHolder.onEvent()
 *     │   │   → 通知业务层 FuzzyWatchEventWatcher
 *     │   └─ FINISH_INIT_NOTIFY → markInitializationComplete()
 *     │
 *     └─ NamingFuzzyWatchChangeNotifyRequest（增量变更）
 *         ├─ 解析 serviceKey → namespace/group/serviceName
 *         ├─ FuzzyGroupKeyPattern.filterMatchedPatterns() → 找到所有匹配的通配符
 *         ├─ ADD_SERVICE / INSTANCE_CHANGED → addReceivedServiceKey() + 发布事件
 *         └─ DELETE_SERVICE → removeReceivedServiceKey() + 发布事件
 * }</pre>
 *
 * <h2>关键协作者</h2>
 * <ul>
 *   <li><b>NamingFuzzyWatchServiceListHolder</b> —— 管理所有模糊订阅上下文（FuzzyWatchContext），
 *       以通配符 pattern 为 key 索引；同时也是 NamingFuzzyWatchNotifyEvent 的最终消费者</li>
 *   <li><b>NamingFuzzyWatchContext</b> —— 单个模糊订阅的上下文，维护已收到服务列表 + 监听器列表，
 *       提供去重能力（addReceivedServiceKey/removeReceivedServiceKey）</li>
 *   <li><b>FuzzyGroupKeyPattern</b> —— 通配符匹配引擎，判断具体 serviceKey 是否匹配某个 pattern</li>
 *   <li><b>NotifyCenter</b> —— 事件总线，发布 NamingFuzzyWatchNotifyEvent 异步通知</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <ul>
 *   <li><b>创建者</b>：NamingGrpcClientProxy.start() L232-233 —— gRPC 建连时注册为
 *       {@code ServerRequestHandler}</li>
 *   <li><b>初始化</b>：构造器中注册 NamingFuzzyWatchNotifyEvent 的 Publisher（环形缓冲区 1000）</li>
 *   <li><b>关闭者</b>：随 RpcClient 关闭自动注销（gRPC 连接断开时）</li>
 * </ul>
 *
 * <h2>与 NamingPushRequestHandler 的区别</h2>
 * <table>
 *   <tr><th>维度</th><th>NamingPushRequestHandler</th><th>本类</th></tr>
 *   <tr><td>粒度</td><td>实例级（哪些 IP:port 变化）</td><td>服务级（哪些 service 匹配通配符）</td></tr>
 *   <tr><td>触发条件</td><td>subscribe 某个具体服务</td><td>fuzzyWatch 某个通配符模式</td></tr>
 *   <tr><td>最终消费者</td><td>ServiceInfoHolder → InstancesChangeNotifier</td>
 *       <td>NamingFuzzyWatchServiceListHolder → FuzzyWatchEventWatcher</td></tr>
 * </table>
 *
 * @see NamingPushRequestHandler
 * @see NamingFuzzyWatchServiceListHolder
 * @since 3.0.0
 * @author shiyiyue
 */
public class NamingFuzzyWatchNotifyRequestHandler implements ServerRequestHandler {
    
    /**
     * 模糊监听服务列表持有者 —— 维护所有通配符 pattern → FuzzyWatchContext 的映射关系。
     * <p>由 NamingGrpcClientProxy.start() 传入，通常是同一个实例被多个组件共享。</p>
     */
    NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder;
    
    /**
     * 构造处理器并注册 SlowEvent Publisher。
     *
     * <p>调用方：NamingGrpcClientProxy.start() L232-233。</p>
     *
     * <p>NotifyCenter.registerToPublisher 创建环形缓冲区队列（大小 1000），
     * 确保高并发下的异步解耦。</p>
     *
     * @param namingFuzzyWatchServiceListHolder 模糊监听上下文管理器
     */
    public NamingFuzzyWatchNotifyRequestHandler(
        NamingFuzzyWatchServiceListHolder namingFuzzyWatchServiceListHolder) {
        this.namingFuzzyWatchServiceListHolder = namingFuzzyWatchServiceListHolder;
        // 预注册 SlowEvent Publisher，避免第一次发布事件时因 Publisher 尚未初始化而丢失事件
        NotifyCenter.registerToPublisher(NamingFuzzyWatchNotifyEvent.class, 1000);
    }
    
    /**
     * gRPC Server Push 回调入口 —— 处理服务端推送的模糊监听通知。
     *
     * <p>调用方：gRPC 双向流的 ServerRequestHandler 回调机制（由 RpcClient 分发）。</p>
     *
     * <h3>处理路径 A：同步通知（NamingFuzzyWatchSyncRequest）</h3>
     * <ol>
     *   <li>根据 groupKeyPattern 查找对应的 NamingFuzzyWatchContext</li>
     *   <li>对每个 serviceKey 做去重检查（addReceivedServiceKey），
     *       防止"change event 先于 init event 到达"的乱序问题</li>
     *   <li>去重成功后发布 NamingFuzzyWatchNotifyEvent 通知业务层</li>
     *   <li>FINISH_INIT_NOTIFY → 标记初始化完成，后续只接收增量</li>
     * </ol>
     *
     * <h3>处理路径 B：变更通知（NamingFuzzyWatchChangeNotifyRequest）</h3>
     * <ol>
     *   <li>解析 serviceKey → namespace / groupName / serviceName</li>
     *   <li>用 FuzzyGroupKeyPattern.filterMatchedPatterns() 找出所有匹配的通配符</li>
     *   <li>ADD_SERVICE / INSTANCE_CHANGED → 注册新 serviceKey + 发布事件</li>
     *   <li>DELETE_SERVICE → 移除 serviceKey + 发布事件</li>
     * </ol>
     *
     * @param request    来自服务端的推送（SyncRequest 或 ChangeNotifyRequest）
     * @param connection 当前 gRPC 连接（未直接使用，由框架传入）
     * @return NamingFuzzyWatchChangeNotifyResponse（ack 确认）
     */
    @Override
    public Response requestReply(Request request, Connection connection) {
        
        // ========== 路径 A: 同步通知（初始同步 / 增量同步） ==========
        if (request instanceof NamingFuzzyWatchSyncRequest) {
            NamingFuzzyWatchSyncRequest watchNotifySyncRequest =
                (NamingFuzzyWatchSyncRequest) request;
            // Step A1: 根据通配符 pattern 查找对应的 FuzzyWatchContext
            NamingFuzzyWatchContext namingFuzzyWatchContext =
                namingFuzzyWatchServiceListHolder.getFuzzyMatchContextMap()
                    .get(watchNotifySyncRequest.getGroupKeyPattern());
            if (namingFuzzyWatchContext != null) {
                Collection<NamingFuzzyWatchSyncRequest.Context> serviceKeys =
                    watchNotifySyncRequest.getContexts();
                // Step A2: 判断同步类型
                if (watchNotifySyncRequest.getSyncType().equals(Constants.FUZZY_WATCH_INIT_NOTIFY)
                    || watchNotifySyncRequest.getSyncType()
                        .equals(Constants.FUZZY_WATCH_DIFF_SYNC_NOTIFY)) {
                    // 初始同步 or 增量差分同步 → 逐个 serviceKey 去重后发布事件
                    for (NamingFuzzyWatchSyncRequest.Context serviceKey : serviceKeys) {
                        // 去重：may have a 'change event' sent to client before 'init event'
                        if (namingFuzzyWatchContext
                            .addReceivedServiceKey(serviceKey.getServiceKey())) {
                            NotifyCenter.publishEvent(NamingFuzzyWatchNotifyEvent.build(
                                namingFuzzyWatchServiceListHolder.getNotifierEventScope(),
                                watchNotifySyncRequest.getGroupKeyPattern(),
                                serviceKey.getServiceKey(),
                                serviceKey.getChangedType(),
                                watchNotifySyncRequest.getSyncType()));
                        }
                    }
                } else if (watchNotifySyncRequest.getSyncType()
                    .equals(Constants.FINISH_FUZZY_WATCH_INIT_NOTIFY)) {
                    // Step A3: 初始化完成标记 → 后续只接收增量变更
                    namingFuzzyWatchContext.markInitializationComplete();
                }
            }
            
            return new NamingFuzzyWatchChangeNotifyResponse();
            
        // ========== 路径 B: 变更通知（增量 ADD/DELETE） ==========
        } else if (request instanceof NamingFuzzyWatchChangeNotifyRequest) {
            NamingFuzzyWatchChangeNotifyRequest notifyChangeRequest =
                (NamingFuzzyWatchChangeNotifyRequest) request;
            // Step B1: 解析 serviceKey → namespace / groupName / serviceName
            String[] serviceKeyItems =
                NamingUtils.parseServiceKey(notifyChangeRequest.getServiceKey());
            String namespace = serviceKeyItems[0];
            String groupName = serviceKeyItems[1];
            String serviceName = serviceKeyItems[2];
            
            // Step B2: 找出所有匹配该 serviceKey 的通配符 pattern
            Collection<String> matchedPattern = FuzzyGroupKeyPattern.filterMatchedPatterns(
                namingFuzzyWatchServiceListHolder.getFuzzyMatchContextMap().keySet(),
                serviceName, groupName,
                namespace);
            String serviceChangeType = notifyChangeRequest.getChangedType();
            
            switch (serviceChangeType) {
                // Step B3: ADD_SERVICE / INSTANCE_CHANGED → 注册新 serviceKey + 发布事件
                case Constants.ServiceChangedType.ADD_SERVICE:
                case Constants.ServiceChangedType.INSTANCE_CHANGED:
                    for (String pattern : matchedPattern) {
                        NamingFuzzyWatchContext namingFuzzyWatchContext =
                            namingFuzzyWatchServiceListHolder.getFuzzyMatchContextMap()
                                .get(pattern);
                        if (namingFuzzyWatchContext != null
                            && namingFuzzyWatchContext.addReceivedServiceKey(
                                ((NamingFuzzyWatchChangeNotifyRequest) request)
                                    .getServiceKey())) {
                            // 发布本地服务新增/变更事件
                            NotifyCenter.publishEvent(NamingFuzzyWatchNotifyEvent.build(
                                namingFuzzyWatchServiceListHolder.getNotifierEventScope(),
                                pattern,
                                notifyChangeRequest.getServiceKey(),
                                Constants.ServiceChangedType.ADD_SERVICE,
                                FUZZY_WATCH_RESOURCE_CHANGED));
                        }
                    }
                    break;
                // Step B4: DELETE_SERVICE → 移除 serviceKey + 发布事件
                case Constants.ServiceChangedType.DELETE_SERVICE:
                    for (String pattern : matchedPattern) {
                        NamingFuzzyWatchContext namingFuzzyWatchContext =
                            namingFuzzyWatchServiceListHolder.getFuzzyMatchContextMap()
                                .get(pattern);
                        if (namingFuzzyWatchContext != null
                            && namingFuzzyWatchContext.removeReceivedServiceKey(
                                notifyChangeRequest.getServiceKey())) {
                            NotifyCenter.publishEvent(NamingFuzzyWatchNotifyEvent.build(
                                namingFuzzyWatchServiceListHolder.getNotifierEventScope(),
                                pattern,
                                notifyChangeRequest.getServiceKey(),
                                Constants.ServiceChangedType.DELETE_SERVICE,
                                FUZZY_WATCH_RESOURCE_CHANGED));
                        }
                    }
                    break;
                default:
                    break;
            }
            return new NamingFuzzyWatchChangeNotifyResponse();
        }
        return null;
    }
}
