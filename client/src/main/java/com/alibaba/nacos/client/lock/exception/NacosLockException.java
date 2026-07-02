/*
 * Copyright 1999-2026 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.client.lock.exception;

/**
 * Nacos 分布式锁操作异常 —— 在加锁、释放锁或续约过程中发生错误时抛出。
 *
 * <h2>设计考量</h2>
 * <p>继承 {@link RuntimeException} 而非受检异常，原因：JUC
 * {@link java.util.concurrent.locks.Lock} 接口的方法签名
 * （如 {@code lock()}、{@code unlock()}）不声明任何受检异常。
 * 为了在 {@link NacosLock} 中直接抛出而不破坏 JUC 接口契约，
 * 必须使用运行时异常包装底层错误。</p>
 *
 * <p>典型触发场景：</p>
 * <ul>
 *   <li>{@link NacosLock#lock()} 被中断时包装 InterruptedException</li>
 *   <li>{@link com.alibaba.nacos.client.lock.remote.grpc.LockGrpcClient}
 *       的 gRPC 调用失败时包装 NacosException</li>
 * </ul>
 *
 * @author DHX
 * @date 2026/05/31
 */
public class NacosLockException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;
    
    public NacosLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
