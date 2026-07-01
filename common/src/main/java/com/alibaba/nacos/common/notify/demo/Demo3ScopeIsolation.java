/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

import java.util.UUID;

/**
 * Demo3: scope 作用域隔离 —— 为什么同一个 JVM 里的多个实例不会串台？
 *
 * <p>场景：同一个 JVM 里创建了两个 NamingService（模拟）
 * <pre>
 *   NamingService ns1 = new NamingService("127.0.0.1:8848");  // scope = "uuid-1"
 *   NamingService ns2 = new NamingService("10.0.0.1:8848");   // scope = "uuid-2"
 *
 *   ns1.subscribe("order-service", listener1);  // listener1 关心 scope="uuid-1"
 *   ns2.subscribe("order-service", listener2);  // listener2 关心 scope="uuid-2"
 *
 *   // ns1 发现实例变了，发布事件（带 scope="uuid-1"）
 *   // → listener1 收到 ✓（scope 匹配）
 *   // → listener2 收到 ✗（scope 不匹配，被跳过）
 * </pre>
 *
 * <p>运行后你会看到：
 * <pre>
 *   [ns1的Publisher] listener1 收到事件！scope=uuid-aaaa
 *   （注意：listener2 没收到，因为 scope 不匹配）
 *
 *   [ns2的Publisher] listener2 收到事件！scope=uuid-bbbb
 *   （注意：listener1 没收到，因为 scope 不匹配）
 * </pre>
 *
 * @author nacos
 */
public class Demo3ScopeIsolation {

    // ==================== 事件 ====================

    /**
     * 实例变更事件，带 scope 标识
     * 这就是 Nacos 中 InstancesChangeEvent 的简化版
     */
    static class InstanceChangeEvent extends Event {

        private final String serviceName;
        private final String scope;  // 关键：每个 NamingService 实例有自己的 UUID

        public InstanceChangeEvent(String serviceName, String scope) {
            this.serviceName = serviceName;
            this.scope = scope;
        }

        public String getServiceName() {
            return serviceName;
        }

        @Override
        public String scope() {
            // 覆写 Event.scope()，返回自己的 scope
            // DefaultPublisher.receiveEvent 中会用 subscriber.scopeMatches(event) 比较
            return scope;
        }

        @Override
        public String toString() {
            return "InstanceChangeEvent{serviceName='" + serviceName + "', scope=" + scope.substring(0, 8) + "}";
        }
    }

    // ==================== 订阅者 ====================

    /**
     * 实例变更通知器，只处理自己 scope 的事件
     * 这就是 Nacos 中 InstancesChangeNotifier 的简化版
     */
    static class InstanceChangeNotifier extends Subscriber<InstanceChangeEvent> {

        private final String name;     // 给个名字方便看日志
        private final String scope;    // 自己的 scope

        public InstanceChangeNotifier(String name, String scope) {
            this.name = name;
            this.scope = scope;
        }

        @Override
        public void onEvent(InstanceChangeEvent event) {
            System.out.println("  [" + name + "] 收到事件！服务="
                + event.getServiceName()
                + " scope=" + event.scope().substring(0, 8));
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return InstanceChangeEvent.class;
        }

        @Override
        public boolean scopeMatches(InstanceChangeEvent event) {
            // 关键！只有 scope 匹配的事件才处理
            boolean match = this.scope.equals(event.scope());
            if (!match) {
                System.out.println("  [" + name + "] 跳过事件（scope不匹配）"
                    + " 我的scope=" + this.scope.substring(0, 8)
                    + " 事件scope=" + event.scope().substring(0, 8));
            }
            return match;
        }
    }

    // ==================== 运行 ====================

    public static void main(String[] args) throws InterruptedException {

        // 模拟创建两个 NamingService，各自有独立的 UUID scope
        String scope1 = UUID.randomUUID().toString();
        String scope2 = UUID.randomUUID().toString();

        System.out.println("模拟创建两个 NamingService：");
        System.out.println("  ns1 scope = " + scope1.substring(0, 8) + "...");
        System.out.println("  ns2 scope = " + scope2.substring(0, 8) + "...");
        System.out.println();

        // 注册发布器（两个 ns 用同一个事件类型，所以共用一个 Publisher）
        NotifyCenter.registerToPublisher(InstanceChangeEvent.class, 1024);

        // 注册订阅者
        InstanceChangeNotifier notifier1 = new InstanceChangeNotifier("ns1监听器", scope1);
        InstanceChangeNotifier notifier2 = new InstanceChangeNotifier("ns2监听器", scope2);
        NotifyCenter.registerSubscriber(notifier1);
        NotifyCenter.registerSubscriber(notifier2);

        // ns1 发现实例变了，发布事件
        System.out.println("=== ns1 发布事件（scope=" + scope1.substring(0, 8) + "） ===");
        NotifyCenter.publishEvent(new InstanceChangeEvent("order-service", scope1));

        Thread.sleep(500);

        System.out.println();
        System.out.println("=== ns2 发布事件（scope=" + scope2.substring(0, 8) + "） ===");
        NotifyCenter.publishEvent(new InstanceChangeEvent("order-service", scope2));

        Thread.sleep(500);

        System.out.println();
        System.out.println("=== 分析 ===");
        System.out.println("ns1 发的事件 → notifier1 的 scopeMatches() 返回 true  → 收到回调");
        System.out.println("            → notifier2 的 scopeMatches() 返回 false → 被跳过");
        System.out.println("ns2 发的事件 → notifier2 的 scopeMatches() 返回 true  → 收到回调");
        System.out.println("            → notifier1 的 scopeMatches() 返回 false → 被跳过");
        System.out.println();
        System.out.println("如果没有 scope 机制：");
        System.out.println("  ns1 的实例变更会触发 ns2 的监听器 → 误通知！");
        System.out.println("  在同一个 JVM 中运行多个 NamingService 时就会出问题。");

        NotifyCenter.shutdown();
    }
}
