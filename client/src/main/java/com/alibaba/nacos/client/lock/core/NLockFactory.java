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

/**
 * {@link NLock} 的静态工厂类 —— 提供便捷的锁实体创建方法。
 *
 * <p>封装了 NLock 的两种构造方式：无过期时间（永不过期）和指定过期时间戳。</p>
 *
 * @author 985492783@qq.com
 * @date 2023/8/27 15:23
 */
public class NLockFactory {
    
    /**
     * 创建一个永不过期的 Nacos 锁实体。
     *
     * @param key 锁标识（业务唯一键）
     * @return NLock 实例，过期时间为 -1（永不过期）
     */
    public static NLock getLock(String key) {
        return new NLock(key, -1L);
    }
    
    /**
     * 创建一个指定过期时间的 Nacos 锁实体。
     *
     * @param key 锁标识（业务唯一键）
     * @param expireTimestamp 过期时间戳（毫秒）
     * @return NLock 实例
     */
    public static NLock getLock(String key, Long expireTimestamp) {
        return new NLock(key, expireTimestamp);
    }
}
