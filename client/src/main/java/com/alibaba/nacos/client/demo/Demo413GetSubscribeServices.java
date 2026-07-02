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
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;

import java.util.List;
import java.util.Properties;

/**
 * 4.13 获取当前客户端所监听的服务列表。
 *
 * <p>调用链路：
 * <pre>
 *   NamingService.getSubscribeServices()
 *     → changeNotifier.getSubscribeServices()
 *       → SelectorManager.getSubscriptions()
 *         → selectorMap.keySet()  ← 返回所有已注册了 listener 的 serviceKey
 * </pre>
 *
 * <p>注意：这是客户端本地记录的已订阅列表，不是查询服务端。
 * 只有调用了 subscribe() 才会出现在列表中。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.event.InstancesChangeNotifier#getSubscribeServices}
 */
public class Demo413GetSubscribeServices {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.13 获取当前客户端所监听的服务列表 ---");

        // 先注册实例 + 订阅两个服务
        naming.registerInstance("demo-service-a", "192.168.1.1", 8080);
        naming.registerInstance("demo-service-b", "192.168.1.2", 8080);
        sleep(500);

        EventListener listener1 = event -> {};
        EventListener listener2 = event -> {};
        naming.subscribe("demo-service-a", listener1);
        naming.subscribe("demo-service-b", listener2);

        // 查询已订阅服务列表
        List<ServiceInfo> subscribed = naming.getSubscribeServices();
        System.out.println("  getSubscribeServices() → 已订阅 " + subscribed.size() + " 个服务：");
        for (ServiceInfo si : subscribed) {
            System.out.println("    - " + si.getGroupName() + "@@" + si.getName()
                + " (group=" + si.getGroupName() + ")");
        }

        // 清理
        naming.unsubscribe("demo-service-a", listener1);
        naming.unsubscribe("demo-service-b", listener2);

        // 再次查询
        List<ServiceInfo> afterUnsub = naming.getSubscribeServices();
        System.out.println("  取消订阅后 → 已订阅 " + afterUnsub.size() + " 个服务");

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
