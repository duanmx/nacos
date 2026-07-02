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
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.selector.NamingContext;
import com.alibaba.nacos.api.naming.selector.NamingResult;
import com.alibaba.nacos.api.naming.selector.NamingSelector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 4.11 取消带选择器的监听服务。
 *
 * <p>注意：unsubscribe 的 selector 和 listener 必须和 subscribe 时传入的是同一组对象。
 * 因为 SelectorManager.removeSelectorWrapper 用 equals 匹配 wrapper，
 * wrapper 的 equals 基于 selector + listener。
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#unsubscribe}
 *        （带 NamingSelector 参数的重载）
 */
public class Demo411UnsubscribeWithSelector {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.11 取消带选择器的监听服务 ---");

        // 先注册实例
        naming.registerInstance("demo-service", "192.168.1.1", 8080, "BJ");
        naming.registerInstance("demo-service", "192.168.1.2", 9090, "BJ");
        sleep(500);

        // 自定义选择器
        NamingSelector portSelector = new NamingSelector() {
            @Override
            public NamingResult select(NamingContext context) {
                List<Instance> filtered = new ArrayList<>();
                for (Instance inst : context.getInstances()) {
                    if (inst.getPort() == 8080) {
                        filtered.add(inst);
                    }
                }
                return () -> filtered;
            }
        };

        EventListener listener = event -> {
            NamingEvent e = (NamingEvent) event;
            System.out.println("  [selector 监听收到] service=" + e.getServiceName()
                + " instances=" + e.getInstances().size());
        };

        // 订阅
        naming.subscribe("demo-service", "DEFAULT_GROUP", portSelector, listener);
        System.out.println("  subscribe(serviceName, groupName, selector, listener)");

        // 触发变更
        naming.registerInstance("demo-service", "192.168.1.20", 8080, "BJ");
        sleep(1000);

        // 取消带选择器的监听
        naming.unsubscribe("demo-service", "DEFAULT_GROUP", portSelector, listener);
        System.out.println("  unsubscribe(serviceName, groupName, selector, listener)");

        // 再次触发变更，不应再收到通知
        naming.registerInstance("demo-service", "192.168.1.30", 8080, "BJ");
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
