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

package com.alibaba.nacos.client.lock;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.lock.LockService;
import com.alibaba.nacos.api.lock.common.LockConstants;
import com.alibaba.nacos.api.lock.model.LockInstance;
import com.alibaba.nacos.client.address.AbstractServerListManager;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.lock.remote.grpc.LockGrpcClient;
import com.alibaba.nacos.client.naming.core.NamingServerListManager;
import com.alibaba.nacos.client.naming.remote.http.NamingHttpClientManager;
import com.alibaba.nacos.client.security.SecurityProxy;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.nacos.client.constant.Constants.Security.SECURITY_INFO_REFRESH_INTERVAL_MILLS;

/**
 * Nacos 分布式锁服务 —— 客户端分布式锁功能的主入口。
 *
 * <h2>核心职责</h2>
 * <p>作为 {@link LockService} 的实现，NacosLockService 负责：
 * <ul>
 *   <li>创建并管理 {@link LockGrpcClient}（gRPC 锁通信客户端）</li>
 *   <li>创建并管理 {@link SecurityProxy}（鉴权代理，定时刷新登录态）</li>
 *   <li>创建并管理 {@link NacosLockWatchdog}（锁续约看门狗）</li>
 *   <li>提供 JUC 风格的 {@link NacosLock}（可重入/不可重入）实例</li>
 *   <li>提供底层 {@link LockInstance} 模型的直接锁操作（remoteTryLock/remoteReleaseLock）</li>
 * </ul>
 *
 * <h2>架构层次</h2>
 * <pre>{@code
 *   用户代码
 *     ├── NacosLock (JUC Lock 接口)        ← 高层：重入计数 + 看门狗自动续约
 *     │     └── LockGrpcClient             ← 低层：gRPC 通信
 *     └── LockInstance (直接 RPC)          ← 低层：直接操作锁
 *           └── LockGrpcClient
 *
 *   NacosLockService                       ← 本类：工厂 + 生命周期管理
 *     ├── LockGrpcClient                   ← gRPC 客户端
 *     ├── SecurityProxy                    ← 鉴权代理（定时刷新 accessToken）
 *     ├── NacosLockWatchdog               ← 看门狗（定时续约）
 *     └── NamingServerListManager          ← 服务端地址管理
 * }</pre>
 *
 * <h2>初始化过程</h2>
 * <ol>
 *   <li>从 Properties 构建 NacosClientProperties</li>
 *   <li>创建 NamingServerListManager（获取 Nacos 服务端地址）</li>
 *   <li>创建 SecurityProxy 并执行首次 login</li>
 *   <li>创建 LockGrpcClient（建立 gRPC 连接）</li>
 *   <li>创建 NacosLockWatchdog（锁续约调度器）</li>
 *   <li>生成 clientId（UUID，标识当前客户端实例）</li>
 *   <li>启动定时任务定期刷新登录态</li>
 * </ol>
 *
 * <h2>关闭顺序</h2>
 * <p>先关闭 LockGrpcClient（断开 gRPC 连接，完成所有 pending future），
 * 再关闭 Watchdog（取消续约任务），最后关闭 ServerListManager 和 executorService。</p>
 *
 * @author 985492783@qq.com
 * @date 2023/8/24 19:51
 */
public class NacosLockService implements LockService {
    
    private final LockGrpcClient lockGrpcClient;
    
    private final SecurityProxy securityProxy;
    
    private final NacosLockWatchdog watchdog;
    
    private final String clientId;
    
    private final AbstractServerListManager serverListManager;
    
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    private ScheduledExecutorService executorService;
    
    /**
     * 构造 NacosLockService，按顺序初始化所有依赖组件。
     *
     * @param properties 包含服务端地址、认证信息等配置
     * @throws NacosException 初始化失败时抛出
     */
    public NacosLockService(Properties properties) throws NacosException {
        NacosClientProperties nacosClientProperties =
            NacosClientProperties.PROTOTYPE.derive(properties);
        this.serverListManager = new NamingServerListManager(properties);
        serverListManager.start();
        this.securityProxy = new SecurityProxy(serverListManager,
            NamingHttpClientManager.getInstance().getNacosRestTemplate());
        initSecurityProxy(nacosClientProperties);
        this.lockGrpcClient =
            new LockGrpcClient(nacosClientProperties, serverListManager, securityProxy);
        this.watchdog = new NacosLockWatchdog();
        this.clientId = UUID.randomUUID().toString();
    }
    
    /**
     * 检查服务是否已关闭，已关闭则抛异常。
     */
    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("NacosLockService has been shut down");
        }
    }
    
    /**
     * 初始化鉴权代理：执行首次登录，然后启动定时任务定期刷新登录态。
     *
     * <p>定时刷新间隔由 {@code SECURITY_INFO_REFRESH_INTERVAL_MILLS} 控制。
     * 使用守护线程（daemon），不会阻止 JVM 退出。</p>
     */
    private void initSecurityProxy(NacosClientProperties properties) {
        this.executorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r);
            t.setName("com.alibaba.nacos.client.lock.security");
            t.setDaemon(true);
            return t;
        });
        final Properties nacosClientPropertiesView = properties.asProperties();
        this.securityProxy.login(nacosClientPropertiesView);
        this.executorService.scheduleWithFixedDelay(
            () -> securityProxy.login(nacosClientPropertiesView), 0,
            SECURITY_INFO_REFRESH_INTERVAL_MILLS, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public Boolean lock(LockInstance instance) throws NacosException {
        checkNotClosed();
        return instance.lock(this);
    }
    
    @Override
    public Boolean unLock(LockInstance instance) throws NacosException {
        checkNotClosed();
        return instance.unLock(this);
    }
    
    @Override
    public Boolean remoteTryLock(LockInstance instance) throws NacosException {
        checkNotClosed();
        return lockGrpcClient.lock(instance);
    }
    
    @Override
    public Boolean remoteReleaseLock(LockInstance instance) throws NacosException {
        checkNotClosed();
        return lockGrpcClient.unLock(instance);
    }
    
    @Override
    public Boolean renew(LockInstance instance) throws NacosException {
        checkNotClosed();
        return lockGrpcClient.renew(instance);
    }
    
    /**
     * 获取一个 JUC 风格的可重入分布式锁。
     *
     * <p>返回的 {@link NacosLock} 实现了 {@link java.util.concurrent.locks.Lock} 接口，
     * 内部使用 {@link ThreadLocal} 跟踪重入计数，并自动注册到 {@link NacosLockWatchdog}
     * 进行定期续约。</p>
     *
     * @param key 锁标识（业务唯一键）
     * @return NacosLock 实例（实现 JUC Lock 接口）
     */
    public NacosLock getReentrantLock(String key) {
        checkNotClosed();
        return new NacosLock(key, LockConstants.REENTRANT_LOCK_TYPE, lockGrpcClient, watchdog,
            clientId);
    }
    
    /**
     * 获取一个 JUC 风格的不可重入分布式锁。
     *
     * <p>同一线程重复调用 lock() 会抛出 {@link IllegalMonitorStateException}。
     * 使用场景：需要严格保证同一线程不能重复获取同一把锁的业务。</p>
     *
     * @param key 锁标识（业务唯一键）
     * @return NacosLock 实例（实现 JUC Lock 接口）
     */
    public NacosLock getNonReentrantLock(String key) {
        checkNotClosed();
        return new NacosLock(key, LockConstants.NON_REENTRANT_LOCK_TYPE, lockGrpcClient, watchdog,
            clientId);
    }
    
    @Override
    public void shutdown() throws NacosException {
        if (closed.compareAndSet(false, true)) {
            lockGrpcClient.shutdown();
            watchdog.shutdown();
            serverListManager.shutdown();
            if (null != executorService) {
                executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executorService.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
