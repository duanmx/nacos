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
 * Demo1: 最简发布订阅 —— 用最少的代码跑通整个流程
 *
 * <p>运行后你会看到：
 * <pre>
 *   [main] 我要发布事件了...
 *   [nacos.publisher-...Event] 收到订单事件！订单号=ORDER_001，金额=99.9
 *   [main] 事件发布完毕，主线程继续干别的...
 * </pre>
 *
 * <p>关键观察：
 * 1. 回调线程名是 "nacos.publisher-xxx"，不是 main —— 说明是 Publisher 线程异步执行的
 * 2. "发布完毕" 可能在 "收到订单事件" 之前或之后 —— 因为是异步的，看线程调度
 *
 * @author nacos
 */
public class Demo1BasicPubSub {

    // ==================== 第一步：定义事件 ====================

    /**
     * 一个最简单的事件：订单创建事件
     * 只需要继承 Event，加自己的字段就行
     */
    static class OrderCreatedEvent extends Event {

        private final String orderId;
        private final double amount;

        public OrderCreatedEvent(String orderId, double amount) {
            this.orderId = orderId;
            this.amount = amount;
        }

        public String getOrderId() {
            return orderId;
        }

        public double getAmount() {
            return amount;
        }

        @Override
        public String toString() {
            return "OrderCreatedEvent{orderId='" + orderId + "', amount=" + amount + "}";
        }
    }

    // ==================== 第二步：定义订阅者 ====================

    /**
     * 一个最简单的订阅者：收到订单事件后打印日志
     */
    static class OrderLogSubscriber extends Subscriber<OrderCreatedEvent> {

        @Override
        public void onEvent(OrderCreatedEvent event) {
            // 收到事件后做什么？—— 这里就是你的业务逻辑
            System.out.println("["
                + Thread.currentThread().getName()
                + "] 收到订单事件！订单号=" + event.getOrderId()
                + "，金额=" + event.getAmount());
        }

        @Override
        public Class<? extends Event> subscribeType() {
            // 告诉 NotifyCenter：我订阅的是 OrderCreatedEvent
            return OrderCreatedEvent.class;
        }
    }

    // ==================== 第三步：组装运行 ====================

    public static void main(String[] args) throws InterruptedException {

        // 1. 注册发布器（告诉 NotifyCenter：OrderCreatedEvent 需要一个独立的 Publisher）
        //    队列大小 1024（演示用，生产环境用 16384）
        NotifyCenter.registerToPublisher(OrderCreatedEvent.class, 1024);

        // 2. 注册订阅者（告诉 NotifyCenter：有人关心 OrderCreatedEvent）
        //    注意：注册订阅者时，如果发布器还没创建，会自动创建一个（懒加载）
        NotifyCenter.registerSubscriber(new OrderLogSubscriber());

        // 3. 发布事件！
        System.out.println("["
            + Thread.currentThread().getName()
            + "] 我要发布事件了...");

        OrderCreatedEvent event = new OrderCreatedEvent("ORDER_001", 99.9);
        NotifyCenter.publishEvent(event);
        // ↑ 这一行执行完，事件已经放入队列了
        //   Publisher 线程会异步从队列取出并调用 onEvent()

        // 4. 等一下，让 Publisher 线程有时间处理（否则 main 退出后守护线程就没了）
        Thread.sleep(1000);

        System.out.println("["
            + Thread.currentThread().getName()
            + "] 主线程结束");

        // 5. 清理
        NotifyCenter.shutdown();
    }
}
