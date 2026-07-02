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

package com.alibaba.nacos.client.lock.core;

import com.alibaba.nacos.api.lock.common.LockConstants;
import com.alibaba.nacos.api.lock.model.LockInstance;

/**
 * Nacos 锁实体 —— {@link LockInstance} 的子类，固定锁类型为
 * {@link LockConstants#NACOS_LOCK_TYPE}。
 *
 * <p>与 {@link NacosLock}（JUC Lock 适配器）不同，NLock 是纯数据模型，
 * 封装锁的 key 和过期时间，用于直接传递给 {@link LockGrpcClient} 进行底层 RPC 操作。</p>
 *
 * <p>创建方式：通过 {@link NLockFactory#getLock(String)} 或直接构造。</p>
 *
 * @author 985492783@qq.com
 * @date 2023/8/24 19:52
 */
public class NLock extends LockInstance {
    
    private static final long serialVersionUID = -346054842454875524L;
    
    /**
     * 构造一个 Nacos 类型的锁实体。
     *
     * @param key 锁标识（业务唯一键）
     * @param expireTimestamp 过期时间戳，-1 表示永不过期
     */
    public NLock(String key, Long expireTimestamp) {
        super(key, expireTimestamp, LockConstants.NACOS_LOCK_TYPE);
    }
}
