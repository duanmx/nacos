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

package com.alibaba.nacos.common.notify.demo;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Demo8: SelectorManager 路由 —— 一个 Subscriber 如何分发给多个 Listener？
 *
 * <p>问题：一个 NamingService 可以同时订阅多个服务：
 * <pre>
 *   ns.subscribe("order-service",   listener1);
 *   ns.subscribe("order-service",   listener2);  // 同一个服务可以注册多个 listener
 *   ns.subscribe("payment-service", listener3);
 * </pre>
 *
 * <p>但 NotifyCenter 里只注册了一个 Subscriber（InstancesChangeNotifier）。
 * 它收到事件后，怎么知道该通知哪个 listener？
 *
 * <p>答案：用 SelectorManager 做路由。
 * <pre>
 *   NotifyCenter
 *        │
 *        └── InstancesChangeNotifier（1 个 Subscriber）
 *              │
 *              ├── onEvent(event)
 *              │     │
 *              │     ├── subId = "order-service"
 *              │     ├── selectorManager.getSelectorWrappers(subId) → [wrapper1, wrapper2]
 *              │     └── 逐个调用 wrapper.notifyListener(event)
 *              │           ├── wrapper1 → listener1.onEvent()
 *              │           └── wrapper2 → listener2.onEvent()
 *              │
 *              └── selectorManager 内部结构：
 *                    Map<String, Set<Wrapper>>
 *                      "order-service"   → {wrapper1, wrapper2}
 *                      "payment-service" → {wrapper3}
 * </pre>
 *
 * <p>运行后你会看到：
 * <pre>
 *   [order-service] listener1 收到事件: [instance-1, instance-2]
 *   [order-service] listener2 收到事件: [instance-1, instance-2]
 *   （注意：listener3 没收到，因为它订阅的是 payment-service）
 *
 *   [payment-service] listener3 收到事件: [instance-3]
 *   （注意：listener1、listener2 没收到）
 * </pre>
 *
 * @author nacos
 */
public class Demo8SelectorManager {

    public static void main(String[] args) throws InterruptedException {
        // ==========================================
        // 第一步：定义事件（模拟 InstancesChangeEvent）
        // ==========================================

        // 事件只带 serviceName + 实例列表，模拟 NamingService 收到的实例变更
        // ==========================================
        // 第二步：定义用户 Listener（模拟 EventListener）
        // ==========================================

        // ==========================================
        // 第三步：定义 Wrapper（模拟 NamingSelectorWrapper）
        // ==========================================
        // Wrapper 包了两样东西：
        //   1. Listener —— 用户真正的回调
        //   2. 可选的过滤器 —— 这里简化了，不实现过滤逻辑

        // ==========================================
        // 第四步：定义 SelectorManager（核心路由表）
        // ==========================================
        // 本质就是 Map<String, Set<Wrapper>>，key 是服务名

        // ==========================================
        // 第五步：定义 Notifier（模拟 InstancesChangeNotifier）
        // ==========================================
        // 它是注册到 NotifyCenter 的唯一 Subscriber
        // 收到事件后，用 SelectorManager 路由到具体的 Listener

        // --- 创建 Notifier ---
        String scope = UUID.randomUUID().toString();
        SimpleNotifier notifier = new SimpleNotifier(scope);

        // --- 注册到 NotifyCenter ---
        NotifyCenter.registerToPublisher(ServiceInstanceEvent.class, 16384);
        NotifyCenter.registerSubscriber(notifier);

        // ==========================================
        // 第六步：模拟用户注册多个监听器
        // ==========================================

        // listener1 监听 order-service
        SimpleListener listener1 = new SimpleListener("listener1");
        SimpleWrapper wrapper1 = new SimpleWrapper(listener1);
        notifier.registerListener("order-service", wrapper1);

        // listener2 也监听 order-service（同一个服务可以有多个 listener）
        SimpleListener listener2 = new SimpleListener("listener2");
        SimpleWrapper wrapper2 = new SimpleWrapper(listener2);
        notifier.registerListener("order-service", wrapper2);

        // listener3 监听 payment-service
        SimpleListener listener3 = new SimpleListener("listener3");
        SimpleWrapper wrapper3 = new SimpleWrapper(listener3);
        notifier.registerListener("payment-service", wrapper3);

        // ==========================================
        // 第七步：发布事件，看路由效果
        // ==========================================

        // 等一下让 Publisher 线程启动
        Thread.sleep(500);

        System.out.println("\n=== 发布 order-service 实例变更 ===");
        NotifyCenter.publishEvent(new ServiceInstanceEvent(
            scope, "order-service", Arrays.asList("instance-1", "instance-2")));

        Thread.sleep(500);

        System.out.println("\n=== 发布 payment-service 实例变更 ===");
        NotifyCenter.publishEvent(new ServiceInstanceEvent(
            scope, "payment-service", Arrays.asList("instance-3")));

        Thread.sleep(500);

        System.out.println("\n=== 发布 unknown-service 实例变更（没有订阅者）===");
        NotifyCenter.publishEvent(new ServiceInstanceEvent(
            scope, "unknown-service", Arrays.asList("instance-x")));

        Thread.sleep(500);

        System.out.println("\n=== 注销 listener2 后再发 order-service 事件 ===");
        notifier.removeListener("order-service", wrapper2);
        NotifyCenter.publishEvent(new ServiceInstanceEvent(
            scope, "order-service", Arrays.asList("instance-1", "instance-2", "instance-4")));

        Thread.sleep(500);

        NotifyCenter.shutdown();
    }

    // ==========================================
    // 事件定义
    // ==========================================

    static class ServiceInstanceEvent extends Event {

        private final String eventScope;
        private final String serviceName;
        private final List<String> instances;

        public ServiceInstanceEvent(String eventScope, String serviceName, List<String> instances) {
            this.eventScope = eventScope;
            this.serviceName = serviceName;
            this.instances = instances;
        }

        public String getServiceName() {
            return serviceName;
        }

        public List<String> getInstances() {
            return instances;
        }

        @Override
        public String scope() {
            return eventScope;
        }
    }

    // ==========================================
    // 用户 Listener
    // ==========================================

    static class SimpleListener {

        private final String name;

        public SimpleListener(String name) {
            this.name = name;
        }

        public void onEvent(ServiceInstanceEvent event) {
            System.out.println("  [" + event.getServiceName() + "] " + name
                + " 收到事件: " + event.getInstances());
        }
    }

    // ==========================================
    // Wrapper（包了 Listener + 可选的 Selector）
    // ==========================================

    static class SimpleWrapper {

        private final SimpleListener listener;

        public SimpleWrapper(SimpleListener listener) {
            this.listener = listener;
        }

        public void notifyListener(ServiceInstanceEvent event) {
            listener.onEvent(event);
        }
    }

    // ==========================================
    // SelectorManager（路由表）
    // ==========================================

    static class SimpleSelectorManager {

        // key = serviceName, value = 该服务下所有注册的 wrapper
        private final Map<String, Set<SimpleWrapper>> selectorMap = new ConcurrentHashMap<>();

        public void addWrapper(String serviceName, SimpleWrapper wrapper) {
            selectorMap.compute(serviceName, (k, v) -> {
                if (v == null) {
                    v = new HashSet<>();
                }
                v.add(wrapper);
                return v;
            });
        }

        public void removeWrapper(String serviceName, SimpleWrapper wrapper) {
            selectorMap.computeIfPresent(serviceName, (k, v) -> {
                v.remove(wrapper);
                return v.isEmpty() ? null : v;
            });
        }

        public Set<SimpleWrapper> getWrappers(String serviceName) {
            return selectorMap.getOrDefault(serviceName, new HashSet<>());
        }

        public boolean isSubscribed(String serviceName) {
            return !getWrappers(serviceName).isEmpty();
        }

        public Set<String> getSubscriptions() {
            return selectorMap.keySet();
        }
    }

    // ==========================================
    // Notifier（注册到 NotifyCenter 的唯一 Subscriber）
    // ==========================================

    static class SimpleNotifier extends Subscriber<ServiceInstanceEvent> {

        private final String eventScope;
        // 核心：路由表
        private final SimpleSelectorManager selectorManager = new SimpleSelectorManager();

        public SimpleNotifier(String eventScope) {
            this.eventScope = eventScope;
        }

        // 注册监听器（外部调用）
        public void registerListener(String serviceName, SimpleWrapper wrapper) {
            selectorManager.addWrapper(serviceName, wrapper);
            System.out.println("  注册监听: " + serviceName + " → "
                + wrapper.listener.name);
        }

        // 注销监听器
        public void removeListener(String serviceName, SimpleWrapper wrapper) {
            selectorManager.removeWrapper(serviceName, wrapper);
            System.out.println("  注销监听: " + serviceName + " → "
                + wrapper.listener.name);
        }

        // 收到事件后的路由逻辑
        @Override
        public void onEvent(ServiceInstanceEvent event) {
            String serviceName = event.getServiceName();
            Set<SimpleWrapper> wrappers = selectorManager.getWrappers(serviceName);
            if (wrappers.isEmpty()) {
                System.out.println("  [" + serviceName + "] 没有订阅者，事件被丢弃");
                return;
            }
            for (SimpleWrapper wrapper : wrappers) {
                wrapper.notifyListener(event);
            }
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return ServiceInstanceEvent.class;
        }

        @Override
        public boolean scopeMatches(ServiceInstanceEvent event) {
            return this.eventScope.equals(event.scope());
        }
    }
}
