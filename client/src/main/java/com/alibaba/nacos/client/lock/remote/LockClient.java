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

package com.alibaba.nacos.client.lock.remote;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.lock.model.LockInstance;
import com.alibaba.nacos.common.lifecycle.Closeable;

/**
 * Nacos 分布式锁客户端接口 —— 定义锁客户端与 Nacos 服务端通信的基础契约。
 *
 * <h2>职责</h2>
 * <p>抽象锁操作的两个基本动作：加锁（acquire）和释放（release）。
 * {@link AbstractLockClient} 提供了鉴权 Header 注入的共用逻辑，
 * {@link LockGrpcClient} 基于 gRPC 实现具体的网络通信。</p>
 *
 * <h2>与 JUC Lock 的关系</h2>
 * <p>此接口是底层 RPC 契约（"能不能把锁请求发出去"），
 * {@link NacosLock} 是上层 JUC 适配层（"按 JUC 语义使用锁"）。
 * 两者配合实现：JUC API → NacosLock（重入计数 + 看门狗）→ LockGrpcClient（gRPC 通信）→ 服务端。</p>
 *
 * @author 985492783@qq.com
 * @date 2023/6/28 17:19
 */
public interface LockClient extends Closeable {
    
    /**
     * 向 Nacos 服务端发送加锁请求。
     *
     * @param instance 锁实例信息（key、owner、过期时间等）
     * @return true 表示加锁成功，false 表示锁被其他客户端持有
     * @throws NacosException 网络异常或服务端不支持锁功能时抛出
     */
    Boolean lock(LockInstance instance) throws NacosException;
    
    /**
     * 向 Nacos 服务端发送释放锁请求。
     *
     * @param instance 锁实例信息
     * @return true 表示释放成功
     * @throws NacosException 网络异常时抛出
     */
    Boolean unLock(LockInstance instance) throws NacosException;
    
}
