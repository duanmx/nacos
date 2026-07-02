/*
 * Copyright 1999-2025 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.redo.service;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.task.AbstractExecuteTask;
import org.slf4j.Logger;

/**
 * 抽象重做任务 —— 定时执行的"重放未完成操作"任务基类。
 *
 * <h2>核心职责</h2>
 * <p>由 AbstractRedoService 的定时线程池周期性执行：
 * <ol>
 *   <li>检查 gRPC 连接状态 —— 未连接则跳过（等待下次调度）</li>
 *   <li>调用 redoData() —— 子类实现具体的重做逻辑（遍历 findRedoData 结果并重新 RPC）</li>
 *   <li>异常安全 —— catch 所有异常保证定时任务不因单次失败而停止</li>
 * </ol>
 * </p>
 *
 * <h2>典型子类</h2>
 * <ul>
 *   <li>RedoScheduledTask（Naming）—— 遍历 Instance/BatchInstance/Subscriber RedoData</li>
 *   <li>ConfigRedoScheduledTask（Config）—— 遍历 ConfigRedoData</li>
 *   <li>AiRedoScheduledTask（AI）—— 遍历 AgentEndpoint/McpServerEndpoint RedoData</li>
 * </ul>
 *
 * @param <S> RedoService 类型
 * @author xiweng.yy
 */
public abstract class AbstractRedoTask<S extends AbstractRedoService> extends AbstractExecuteTask {
    
    private final Logger logger;
    
    /**
     * 关联的 RedoService —— 提供 findRedoData() 等查询方法。
     */
    private final S redoService;
    
    public AbstractRedoTask(Logger logger, S redoService) {
        this.logger = logger;
        this.redoService = redoService;
    }
    
    @Override
    public void run() {
        if (!redoService.isConnected()) {
            logger.warn("Grpc Connection is disconnect, skip current redo task");
            return;
        }
        try {
            redoData();
        } catch (Exception e) {
            logger.warn("Redo task run with unexpected exception: ", e);
        }
    }
    
    /**
     * Do actual redo task.
     *
     * @throws NacosException if redo task failed.
     */
    protected abstract void redoData() throws NacosException;
    
    protected S getRedoService() {
        return redoService;
    }
}
