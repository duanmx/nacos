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
 * 4.10 带选择器的监听服务。
 *
 * <p>NamingSelector 是实例过滤器，在事件通知前对实例列表做过滤。
 * 只收到过滤后的实例，不是全部实例。
 *
 * <p>调用链路（与普通 subscribe 的区别）：
 * <pre>
 *   NamingService.subscribe(serviceName, groupName, selector, listener)
 *     → 创建 NamingSelectorWrapper(selector, listener)
 *     → changeNotifier.registerListener(groupName, serviceName, wrapper)
 *       → SelectorManager.addSelectorWrapper(subId, wrapper)
 *     → clientProxy.subscribe()  ← 和普通 subscribe 一样
 *
 *   事件回调时：
 *     → wrapper.notifyListener(event)
 *       → doSelect(event.getHosts())   ← 先用 selector 过滤实例
 *       → listener.onEvent(filteredEvent) ← 再通知用户
 * </pre>
 *
 * <p>源码：{@link com.alibaba.nacos.client.naming.NacosNamingService#subscribe}
 *        （带 NamingSelector 参数的重载）
 */
public class Demo410SubscribeWithSelector {

    private static final String SERVER_ADDR = "127.0.0.1:8848";

    public static void main(String[] args) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty("serverAddr", SERVER_ADDR);
        properties.setProperty("namespace", "public");
        NamingService naming = NamingFactory.createNamingService(properties);

        System.out.println("--- 4.10 带选择器的监听服务 ---");

        // 先注册实例
        naming.registerInstance("demo-service", "192.168.1.1", 8080, "BJ");
        naming.registerInstance("demo-service", "192.168.1.2", 9090, "BJ");
        sleep(500);

        // 自定义选择器：只保留 port=8080 的实例
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
                + " instances=" + e.getInstances().size() + "（已过滤，只含 8080 端口）");
        };

        naming.subscribe("demo-service", "DEFAULT_GROUP", portSelector, listener);
        System.out.println("  subscribe(serviceName, groupName, selector, listener)");

        // 触发变更
        naming.registerInstance("demo-service", "192.168.1.20", 8080, "BJ");
        sleep(1000);

        // 清理
        naming.unsubscribe("demo-service", "DEFAULT_GROUP", portSelector, listener);

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
