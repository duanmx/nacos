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
import com.alibaba.nacos.common.notify.SlowEvent;
import com.alibaba.nacos.common.notify.listener.Subscriber;

/**
 * Demo2: 普通事件 vs 慢事件 —— 为什么要分两种？
 *
 * <p>这个 Demo 对比两种事件的区别，让你直观感受"独立队列"和"共享队列"的差异
 *
 * <p>运行后你会看到：
 * <pre>
 *   === 普通事件 ===
 *   [main] 发布 OrderEvent #1
 *   [main] 发布 OrderEvent #2
 *   [main] 发布 OrderEvent #3
 *   [nacos.publisher-OrderEvent] 处理 OrderEvent #1
 *   [nacos.publisher-OrderEvent] 处理 OrderEvent #2
 *   [nacos.publisher-OrderEvent] 处理 OrderEvent #3
 *
 *   === 慢事件 ===
 *   [main] 发布 IpChangeEvent
 *   [main] 发布 RaftEvent
 *   [main] 发布 DerbyLoadEvent
 *   [nacos.publisher-SlowEvent] 处理 IpChangeEvent
 *   [nacos.publisher-SlowEvent] 处理 RaftEvent
 *   [nacos.publisher-SlowEvent] 处理 DerbyLoadEvent
 * </pre>
 *
 * <p>关键观察：
 * 1. 普通事件：线程名是 "nacos.publisher-OrderEvent"（专属线程）
 * 2. 慢事件：线程名是 "nacos.publisher-SlowEvent"（共享线程）
 * 3. 三种慢事件用同一个线程处理，因为它们都继承 SlowEvent
 *
 * @author nacos
 */
public class Demo2NormalVsSlowEvent {

    // ==================== 普通事件 ====================

    /**
     * 普通事件：继承 Event
     * 特点：每种事件类型有自己的 Publisher 和队列，互不影响
     */
    static class OrderEvent extends Event {

        private final String orderNo;

        public OrderEvent(String orderNo) {
            this.orderNo = orderNo;
        }

        public String getOrderNo() {
            return orderNo;
        }
    }

    // ==================== 慢事件 ====================

    /**
     * 慢事件1：IP 变更
     * 特点：继承 SlowEvent，所有慢事件共用一个 Publisher 和队列
     */
    static class IpChangeEvent extends SlowEvent {

        private final String newIp;

        public IpChangeEvent(String newIp) {
            this.newIp = newIp;
        }

        public String getNewIp() {
            return newIp;
        }
    }

    /**
     * 慢事件2：Raft 选举
     * 和 IpChangeEvent 完全不同的事件，但共用同一个 Publisher
     */
    static class RaftEvent extends SlowEvent {

        private final String leader;

        public RaftEvent(String leader) {
            this.leader = leader;
        }

        public String getLeader() {
            return leader;
        }
    }

    /**
     * 慢事件3：Derby 数据库加载
     */
    static class DerbyLoadEvent extends SlowEvent {

        private final String status;

        public DerbyLoadEvent(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }

    // ==================== 订阅者 ====================

    static class OrderSubscriber extends Subscriber<OrderEvent> {

        @Override
        public void onEvent(OrderEvent event) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] 处理 " + event.getClass().getSimpleName()
                + " 订单号=" + event.getOrderNo());
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return OrderEvent.class;
        }
    }

    static class IpChangeSubscriber extends Subscriber<IpChangeEvent> {

        @Override
        public void onEvent(IpChangeEvent event) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] 处理 " + event.getClass().getSimpleName()
                + " 新IP=" + event.getNewIp());
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return IpChangeEvent.class;
        }
    }

    static class RaftSubscriber extends Subscriber<RaftEvent> {

        @Override
        public void onEvent(RaftEvent event) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] 处理 " + event.getClass().getSimpleName()
                + " Leader=" + event.getLeader());
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return RaftEvent.class;
        }
    }

    static class DerbySubscriber extends Subscriber<DerbyLoadEvent> {

        @Override
        public void onEvent(DerbyLoadEvent event) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] 处理 " + event.getClass().getSimpleName()
                + " 状态=" + event.getStatus());
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return DerbyLoadEvent.class;
        }
    }

    // ==================== 运行 ====================

    public static void main(String[] args) throws InterruptedException {

        // ---- 普通事件：需要手动注册发布器 ----
        NotifyCenter.registerToPublisher(OrderEvent.class, 1024);
        NotifyCenter.registerSubscriber(new OrderSubscriber());

        // ---- 慢事件：不需要注册发布器！共享发布器在 NotifyCenter 静态块已自动创建 ----
        NotifyCenter.registerSubscriber(new IpChangeSubscriber());
        NotifyCenter.registerSubscriber(new RaftSubscriber());
        NotifyCenter.registerSubscriber(new DerbySubscriber());

        // ---- 发布普通事件 ----
        System.out.println("=== 普通事件（独立队列+独立线程） ===");
        NotifyCenter.publishEvent(new OrderEvent("ORD-001"));
        NotifyCenter.publishEvent(new OrderEvent("ORD-002"));
        NotifyCenter.publishEvent(new OrderEvent("ORD-003"));

        Thread.sleep(500);

        // ---- 发布慢事件 ----
        System.out.println("\n=== 慢事件（共享队列+共享线程） ===");
        NotifyCenter.publishEvent(new IpChangeEvent("10.0.0.2"));
        NotifyCenter.publishEvent(new RaftEvent("node-1"));
        NotifyCenter.publishEvent(new DerbyLoadEvent("loaded"));

        Thread.sleep(500);

        // ---- 对比说明 ----
        System.out.println("\n=== 对比 ===");
        System.out.println("普通事件 OrderEvent   → 有自己独立的 Publisher 和队列（16384）");
        System.out.println("慢事件 IpChangeEvent   → ┐");
        System.out.println("慢事件 RaftEvent      → ├→ 共用一个 DefaultSharePublisher 和队列（1024）");
        System.out.println("慢事件 DerbyLoadEvent  → ┘");
        System.out.println();
        System.out.println("为什么慢事件要共享？");
        System.out.println("  因为这些事件很少发生（IP变更、Raft选举、DB加载），");
        System.out.println("  每种都开一个线程太浪费了，共用一个就行。");

        NotifyCenter.shutdown();
    }
}
