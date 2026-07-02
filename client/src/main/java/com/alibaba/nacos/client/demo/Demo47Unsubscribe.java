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
 * 4.7 取消监听服务。
 *
 * <p>调用链路：
 * <pre>
 *   NamingService.unsubscribe(serviceName, groupName, listener)
 *     → changeNotifier.deregisterListener(groupName, serviceName, wrapper)
 *       → SelectorManager.removeSelectorWrapper(subId, wrapper)
 *     → clientProxy.unsubscribe()
 *       → grpcClientProxy.unsubscribe() → 向服务端发送取消订阅请求
 * </pre>
 *
 * <p>注意：unsubscribe 的 listener 必须和 subscribe 时传入的是同一个对象。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#unsubscribe}
 */
public class Demo47Unsubscribe {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.7 取消监听服务 ---");

        // 先注册实例 + 订阅，然后演示取消
        naming.registerInstance("demo-service", "192.168.1.1", 8080, "BJ");
        sleep(500);

        // 订阅三种方式
        EventListener listener1 = event -> {
            NamingEvent e = (NamingEvent) event;
            System.out.println("  [listener1 收到] " + e.getServiceName());
        };
        EventListener listener2 = event -> {
            NamingEvent e = (NamingEvent) event;
            System.out.println("  [listener2 收到] " + e.getServiceName());
        };
        EventListener listener3 = event -> {
            NamingEvent e = (NamingEvent) event;
            System.out.println("  [listener3 收到] clusters=" + e.getClusters());
        };

        naming.subscribe("demo-service", listener1);
        naming.subscribe("demo-service", "DEFAULT_GROUP", listener2);
        naming.subscribe("demo-service", "DEFAULT_GROUP", Arrays.asList("BJ"), listener3);
        System.out.println("  已订阅 3 个 listener");

        // 触发一次通知
        naming.registerInstance("demo-service", "192.168.1.10", 8080, "BJ");
        sleep(1000);

        // 取消监听
        naming.unsubscribe("demo-service", listener1);
        System.out.println("  取消方式1：unsubscribe(serviceName, listener)");

        naming.unsubscribe("demo-service", "DEFAULT_GROUP", listener2);
        System.out.println("  取消方式2：unsubscribe(serviceName, groupName, listener)");

        naming.unsubscribe("demo-service", "DEFAULT_GROUP", Arrays.asList("BJ"), listener3);
        System.out.println("  取消方式3：unsubscribe(serviceName, groupName, clusters, listener)");

        // 再次触发变更，不应再收到通知
        naming.registerInstance("demo-service", "192.168.1.20", 8080, "BJ");
        sleep(1000);
        System.out.println("  （取消后再次变更，不应有通知输出）");

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
