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

package com.alibaba.nacos.client.demo;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.FuzzyWatchChangeEvent;
import com.alibaba.nacos.api.naming.listener.FuzzyWatchEventWatcher;
import com.alibaba.nacos.api.naming.pojo.ListView;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 4.14 服务模糊订阅。
 *
 * <p>Nacos 3.0+ 新功能。用通配符订阅多个服务，服务列表变化时通知。
 *
 * <p>调用链路：
 * <pre>
 *   NamingService.fuzzyWatch(serviceNamePattern, groupNamePattern, watcher)
 *     → namingFuzzyWatchServiceListHolder.registerFuzzyWatcher(pattern, watcher)
 *       → initFuzzyWatchContextIfNeed(pattern)  ← 创建或复用 FuzzyWatchContext
 *       → context.addWatcher(watcher)
 *     → notifyFuzzyWatchSync()  ← 通知工作线程向服务端发起模糊订阅请求
 *       → grpcClientProxy.fuzzyWatchRequest()  ← gRPC 发送
 * </pre>
 *
 * <p>与 subscribe 的区别：
 * subscribe 是订阅某个服务的实例变化；
 * fuzzyWatch 是订阅符合 pattern 的服务列表变化（哪些服务存在/不存在）。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#fuzzyWatch}
 *        {@link com.alibaba.nacos.client.naming.cache.NamingFuzzyWatchServiceListHolder#registerFuzzyWatcher}
 */
public class Demo414FuzzyWatch {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.14 服务模糊订阅 ---");

        // 方式1：fuzzyWatch —— 模糊监听服务列表变化
        // 注意：FuzzyWatchEventWatcher 有两个抽象方法（onEvent + getExecutor），不能用 lambda
        FuzzyWatchEventWatcher watcher = new FuzzyWatchEventWatcher() {
            @Override
            public void onEvent(FuzzyWatchChangeEvent event) {
                System.out.println("  [fuzzyWatch 收到] changeType=" + event.getChangeType()
                    + " service=" + event.getServiceName()
                    + " syncType=" + event.getSyncType());
            }

            @Override
            public java.util.concurrent.Executor getExecutor() {
                return null; // null 表示用 Nacos 内部线程
            }
        };

        try {
            naming.fuzzyWatch("demo-*", "DEFAULT_GROUP", watcher);
            System.out.println("  fuzzyWatch(serviceNamePattern='demo-*', groupNamePattern='DEFAULT_GROUP', watcher)");
            System.out.println("  （监听所有名字以 demo- 开头的服务）");

            // 触发变更：注册一个新服务
            naming.registerInstance("demo-new-service", "192.168.1.1", 8080);
            sleep(2000);

            // 取消模糊订阅
            naming.cancelFuzzyWatch("demo-*", "DEFAULT_GROUP", watcher);
            System.out.println("  cancelFuzzyWatch → 已取消");
        } catch (NacosException e) {
            System.out.println("  fuzzyWatch 需要服务端支持（Nacos 3.0+）：" + e.getMessage());
        }

        // 方式2：fuzzyWatchWithServiceKeys —— 模糊监听并返回完整服务列表
        try {
            FuzzyWatchEventWatcher watcher2 = new FuzzyWatchEventWatcher() {
                @Override
                public void onEvent(FuzzyWatchChangeEvent event) {
                    // 变更通知
                }

                @Override
                public java.util.concurrent.Executor getExecutor() {
                    return null;
                }
            };
            Future<ListView<String>> future = naming.fuzzyWatchWithServiceKeys(
                "demo-*", "DEFAULT_GROUP", watcher2);
            System.out.println("  fuzzyWatchWithServiceKeys → 返回 Future<ListView<String>>");
        } catch (NacosException e) {
            System.out.println("  fuzzyWatchWithServiceKeys 需要服务端支持：" + e.getMessage());
        }

        naming.shutDown();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
