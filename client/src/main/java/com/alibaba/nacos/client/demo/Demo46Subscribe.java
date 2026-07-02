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
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;

import java.util.Arrays;
import java.util.Properties;

/**
 * 4.6 监听服务。
 *
 * <p>调用链路（核心！）：
 * <pre>
 *   NamingService.subscribe(serviceName, groupName, listener)
 *     → clientProxy.subscribe() → grpcClientProxy.subscribe()
 *       → 向服务端发起 gRPC 订阅请求
 *     → serviceInfoHolder.processServiceInfo(result)
 *       → 实例有变化时 → NotifyCenter.publishEvent(InstancesChangeEvent)
 *         → InstancesChangeNotifier.onEvent(event)  ← NotifyCenter 回调
 *           → SelectorManager.getSelectorWrappers(serviceName)
 *             → wrapper.notifyListener(event) → listener.onEvent(namingEvent)
 * </pre>
 *
 * <p>用户传入的 EventListener 最终被包装在 NamingSelectorWrapper 中，
 * 由 SelectorManager 按 serviceName 路由。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#subscribe}
 */
public class Demo46Subscribe {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        properties.setProperty("username", "nacos");
        properties.setProperty("password", "nacos");

        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.6 监听服务 ---");

        // 先注册一个实例，确保服务存在
        naming.registerInstance("demo-service", "192.168.1.1", 8080);
        sleep(3000);

        // 方式1：最简监听
        EventListener listener = event -> {
            if (event instanceof NamingEvent) {
                NamingEvent namingEvent = (NamingEvent) event;
                System.out.println("  [收到通知] service=" + namingEvent.getServiceName()
                    + " instances=" + namingEvent.getInstances().size());
            }
        };
        naming.subscribe("demo-service", listener);
        System.out.println("  方式1：subscribe(serviceName, listener)");

//        // 方式2：指定分组
//        EventListener listener2 = event -> {
//            NamingEvent e = (NamingEvent) event;
//            System.out.println("  [listener2 收到] service=" + e.getServiceName());
//        };
//        naming.subscribe("demo-service", "DEFAULT_GROUP", listener2);
//        System.out.println("  方式2：subscribe(serviceName, groupName, listener)");
//
//        // 方式3：指定集群
//        EventListener listener3 = event -> {
//            NamingEvent e = (NamingEvent) event;
//            System.out.println("  [listener3 收到] clusters=" + e.getClusters());
//        };
//        naming.subscribe("demo-service", "DEFAULT_GROUP", Arrays.asList("BJ"), listener3);
//        System.out.println("  方式3：subscribe(serviceName, groupName, clusters, listener)");
//
//        // 触发一次变更看看
//        naming.registerInstance("demo-service", "192.168.1.10", 8080, "BJ");
//        sleep(1000);

        // 清理
        naming.unsubscribe("demo-service", listener);
//        naming.unsubscribe("demo-service", "DEFAULT_GROUP", listener2);
//        naming.unsubscribe("demo-service", "DEFAULT_GROUP", Arrays.asList("BJ"), listener3);

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
