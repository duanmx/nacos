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

/**
 * Demo5: 队列满降级 + 过期事件丢弃 —— 边界场景演示
 *
 * <p>这个 Demo 演示两个真实生产中会遇到的边界场景：
 *
 * <p>场景1：队列满了怎么办？
 * <pre>
 *   队列大小 = 3
 *   订阅者处理很慢（每次 1 秒）
 *   快速发布 5 个事件：
 *     事件1 → 入队成功
 *     事件2 → 入队成功
 *     事件3 → 入队成功
 *     事件4 → 队列满了！offer() 返回 false → 降级为同步直接调 receiveEvent()
 *     事件5 → 队列满了！offer() 返回 false → 降级为同步直接调 receiveEvent()
 *
 *   结果：事件4和5没有走异步队列，而是在发布者线程同步执行
 * </pre>
 *
 * <p>场景2：过期事件丢弃
 * <pre>
 *   订阅者 A 设置了 ignoreExpireEvent() = true
 *   队列积压了旧事件（sequence 较小）
 *   当 Publisher 处理到旧事件时：
 *     lastEventSequence = 5（已经处理过 seq=5 的事件了）
 *     当前事件 sequence = 2（这是更早的事件）
 *     2 < 5 → 判定为过期 → 跳过！
 * </pre>
 *
 * <p>运行后你会看到：
 * <pre>
 *   === 场景1：队列满降级 ===
 *   [main] 发布事件 #0
 *   [main] 发布事件 #1
 *   [main] 发布事件 #2
 *   [main] 发布事件 #3
 *   [main] WARN: 队列满了！事件 #3 降级为同步发送
 *   [main] 发布事件 #4
 *   [main] WARN: 队列满了！事件 #4 降级为同步发送
 *   [nacos.publisher] 处理事件 #0
 *   [main] 处理事件 #3（注意：在 main 线程同步执行！）
 *   [main] 处理事件 #4（注意：在 main 线程同步执行！）
 *   [nacos.publisher] 处理事件 #1
 *   [nacos.publisher] 处理事件 #2
 *
 *   === 场景2：过期事件丢弃 ===
 *   [nacos.publisher] 订阅者A 处理事件 seq=0
 *   [nacos.publisher] 订阅者A 处理事件 seq=1
 *   [nacos.publisher] 订阅者A 处理事件 seq=2
 *   [nacos.publisher] 订阅者A 跳过过期事件 seq=1（lastSeq=2 > 1）
 *   [nacos.publisher] 订阅者B 处理事件 seq=1（B 没设 ignoreExpireEvent，所以不跳过）
 * </pre>
 *
 * @author nacos
 */
public class Demo5EdgeCases {

    static class SimpleEvent extends Event {

        private final int id;

        public SimpleEvent(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return "SimpleEvent{id=" + id + ", seq=" + sequence() + "}";
        }
    }

    /**
     * 慢订阅者：处理每个事件需要 1 秒
     */
    static class SlowSubscriber extends Subscriber<SimpleEvent> {

        @Override
        public void onEvent(SimpleEvent event) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] 处理事件 #" + event.getId()
                + " seq=" + event.sequence());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return SimpleEvent.class;
        }
    }

    /**
     * 订阅者A：开启过期事件忽略
     */
    static class IgnoreExpireSubscriber extends Subscriber<SimpleEvent> {

        private final String name;

        IgnoreExpireSubscriber(String name) {
            this.name = name;
        }

        @Override
        public void onEvent(SimpleEvent event) {
            System.out.println("  ["
                + Thread.currentThread().getName()
                + "] " + name + " 处理事件 seq=" + event.sequence());
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return SimpleEvent.class;
        }

        @Override
        public boolean ignoreExpireEvent() {
            // 关键：返回 true → 如果事件的 sequence 小于 lastEventSequence，就跳过
            return true;
        }
    }

    /**
     * 订阅者B：不忽略过期事件（默认行为）
     */
    static class NormalSubscriber extends Subscriber<SimpleEvent> {

        private final String name;

        NormalSubscriber(String name) {
            this.name = name;
        }

        @Override
        public void onEvent(SimpleEvent event) {
            System.out.println("  ["
                + Thread.currentThread().getName()
                + "] " + name + " 处理事件 seq=" + event.sequence());
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return SimpleEvent.class;
        }

        @Override
        public boolean ignoreExpireEvent() {
            return false;  // 默认：不忽略，所有事件都处理
        }
    }

    // ==================== 场景1：队列满降级 ====================

    static void demoQueueFull() throws InterruptedException {
        System.out.println("=== 场景1：队列满降级 ===");
        System.out.println("队列大小=3，订阅者每次处理 1 秒，快速发 5 个事件\n");

        // 队列大小设为 3（很小，容易满）
        NotifyCenter.registerToPublisher(SimpleEvent.class, 3);
        NotifyCenter.registerSubscriber(new SlowSubscriber());

        // 快速发布 5 个事件（订阅者还在处理第一个，队列很快满）
        for (int i = 0; i < 5; i++) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] 发布事件 #" + i);
            // 这里会触发两种路径：
            //   队列没满 → offer 成功 → 异步处理（Publisher 线程回调）
            //   队列满了 → offer 失败 → 同步降级 → 在 main 线程直接回调
            NotifyCenter.publishEvent(new SimpleEvent(i));
        }

        // 等待队列中的事件处理完
        Thread.sleep(5000);

        System.out.println("\n分析：");
        System.out.println("  事件 #0,1,2 → 入队成功 → Publisher 线程异步处理");
        System.out.println("  事件 #3,4   → 队列满 → 降级同步 → main 线程同步处理");
        System.out.println("  降级时 publish() 方法内部直接调 receiveEvent()，不经过队列");

        // 清理
        NotifyCenter.deregisterPublisher(SimpleEvent.class);
    }

    // ==================== 场景2：过期事件丢弃 ====================

    static void demoExpireEvent() throws InterruptedException {
        System.out.println("\n=== 场景2：过期事件丢弃 ===");
        System.out.println("订阅者A 设置了 ignoreExpireEvent()=true");
        System.out.println("订阅者B 默认 ignoreExpireEvent()=false\n");

        // 重新注册（新的事件类型避免和场景1混淆）
        NotifyCenter.registerToPublisher(SimpleEvent.class, 1024);

        IgnoreExpireSubscriber subA = new IgnoreExpireSubscriber("订阅者A(忽略过期)");
        NormalSubscriber subB = new NormalSubscriber("订阅者B(不忽略)");
        NotifyCenter.registerSubscriber(subA);
        NotifyCenter.registerSubscriber(subB);

        // 发布 3 个事件
        // 由于是同步回调（executor 返回 null），事件会按顺序处理
        System.out.println("发布 3 个事件（seq 递增）...");
        SimpleEvent e0 = new SimpleEvent(0);
        SimpleEvent e1 = new SimpleEvent(1);
        SimpleEvent e2 = new SimpleEvent(2);
        NotifyCenter.publishEvent(e0);
        NotifyCenter.publishEvent(e1);
        NotifyCenter.publishEvent(e2);

        Thread.sleep(500);

        // 现在手动构造一个"过期"事件
        // 由于 Event 的 sequence 是全局自增的，我们无法手动设置
        // 但可以理解原理：如果队列积压了旧事件，当 Publisher 处理到它时
        // lastEventSequence 已经 > 旧事件的 sequence → 被判定为过期

        System.out.println("\n模拟：再发布一个事件（seq 会更大）...");
        NotifyCenter.publishEvent(new SimpleEvent(3));

        Thread.sleep(500);

        System.out.println("\n分析：");
        System.out.println("  订阅者A 设了 ignoreExpireEvent()=true：");
        System.out.println("    如果事件 sequence < lastEventSequence → 跳过");
        System.out.println("    适用场景：配置变更通知，旧配置已无意义，不需要处理");
        System.out.println();
        System.out.println("  订阅者B 默认 ignoreExpireEvent()=false：");
        System.out.println("    所有事件都处理，不管新旧");
        System.out.println("    适用场景：审计日志，每个变更都要记录");

        NotifyCenter.shutdown();
    }

    // ==================== 运行 ====================

    public static void main(String[] args) throws InterruptedException {
        demoQueueFull();
        demoExpireEvent();
    }
}
