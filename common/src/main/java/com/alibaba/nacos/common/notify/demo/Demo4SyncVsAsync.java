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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Demo4: 同步回调 vs 异步回调 —— executor() 方法的作用
 *
 * <p>问题场景：
 * <pre>
 *   订阅者 A 的 onEvent() 需要 2 秒（比如发邮件）
 *   订阅者 B 的 onEvent() 很快（比如打日志）
 *
 *   如果都用同步（executor 返回 null）：
 *     事件1 → A 处理 2 秒 → B 处理 0 秒 → 事件2 → A 处理 2 秒 → B 处理 0 秒
 *     总耗时 4+ 秒，B 被 A 拖累
 *
 *   如果 A 用异步（executor 返回线程池）：
 *     事件1 → A 丢给线程池（立即返回）→ B 立刻处理
 *     事件2 → A 丢给线程池（立即返回）→ B 立刻处理
 *     总耗时 < 1 秒，A B 互不干扰
 * </pre>
 *
 * <p>运行后你会看到：
 * <pre>
 *   === 场景1：同步回调（executor 返回 null） ===
 *   [nacos.publisher] 同步A 开始处理...（耗时2秒）
 *   [nacos.publisher] 同步A 处理完毕
 *   [nacos.publisher] 同步B 开始处理...（耗时0秒）
 *   [nacos.publisher] 同步B 处理完毕
 *   [nacos.publisher] 同步A 开始处理...（耗时2秒）
 *   [nacos.publisher] 同步A 处理完毕
 *   [nacos.publisher] 同步B 开始处理...（耗时0秒）
 *   [nacos.publisher] 同步B 处理完毕
 *   总耗时: 40xx ms
 *
 *   === 场景2：异步回调（A 用自己的线程池） ===
 *   [nacos.publisher] 同步B 开始处理...（耗时0秒）
 *   [nacos.publisher] 同步B 处理完毕
 *   [nacos.publisher] 同步B 开始处理...（耗时0秒）
 *   [nacos.publisher] 同步B 处理完毕
 *   [async-pool-1] 异步A 开始处理...（耗时2秒）
 *   [async-pool-2] 异步A 开始处理...（耗时2秒）
 *   [async-pool-1] 异步A 处理完毕
 *   [async-pool-2] 异步A 处理完毕
 *   总耗时: 20xx ms
 * </pre>
 *
 * <p>关键观察：
 * 1. 同步模式：所有回调在 Publisher 线程执行，慢的会阻塞快的
 * 2. 异步模式：慢回调丢给自己的线程池，不阻塞 Publisher 线程
 * 3. 异步模式下 B 的回调先完成，因为不用等 A
 *
 * @author nacos
 */
public class Demo4SyncVsAsync {

    static class TaskEvent extends Event {

        private final int taskId;

        public TaskEvent(int taskId) {
            this.taskId = taskId;
        }

        public int getTaskId() {
            return taskId;
        }
    }

    /**
     * 慢订阅者：每次处理需要 2 秒
     * 不覆写 executor()，默认同步执行（阻塞 Publisher 线程）
     */
    static class SlowSyncSubscriber extends Subscriber<TaskEvent> {

        private final String name;

        SlowSyncSubscriber(String name) {
            this.name = name;
        }

        @Override
        public void onEvent(TaskEvent event) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] " + name + " 开始处理 task=" + event.getTaskId() + "（耗时2秒）");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("["
                + Thread.currentThread().getName()
                + "] " + name + " 处理完毕");
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return TaskEvent.class;
        }

        @Override
        public Executor executor() {
            // 返回 null → 在 Publisher 线程同步执行（默认行为）
            return null;
        }
    }

    /**
     * 慢订阅者：每次处理也需要 2 秒
     * 但覆写 executor()，返回自己的线程池 → 异步执行
     */
    static class SlowAsyncSubscriber extends Subscriber<TaskEvent> {

        private final String name;
        private final Executor asyncExecutor;

        SlowAsyncSubscriber(String name) {
            this.name = name;
            // 自己的线程池，2 个线程
            this.asyncExecutor = Executors.newFixedThreadPool(2, r -> {
                Thread t = new Thread(r, "async-pool");
                t.setDaemon(true);
                return t;
            });
        }

        @Override
        public void onEvent(TaskEvent event) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] " + name + " 开始处理 task=" + event.getTaskId() + "（耗时2秒）");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("["
                + Thread.currentThread().getName()
                + "] " + name + " 处理完毕");
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return TaskEvent.class;
        }

        @Override
        public Executor executor() {
            // 返回自己的线程池 → Publisher 会把任务丢到这里执行，不阻塞自己
            return asyncExecutor;
        }
    }

    /**
     * 快订阅者：处理很快
     */
    static class FastSubscriber extends Subscriber<TaskEvent> {

        private final String name;

        FastSubscriber(String name) {
            this.name = name;
        }

        @Override
        public void onEvent(TaskEvent event) {
            System.out.println("["
                + Thread.currentThread().getName()
                + "] " + name + " 开始处理 task=" + event.getTaskId() + "（耗时0秒）");
            System.out.println("["
                + Thread.currentThread().getName()
                + "] " + name + " 处理完毕");
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return TaskEvent.class;
        }
    }

    // ==================== 运行 ====================

    public static void main(String[] args) throws InterruptedException {

        // ---- 场景1：全部同步 ----
        System.out.println("=== 场景1：同步回调（慢订阅者 executor 返回 null） ===");
        NotifyCenter.registerToPublisher(TaskEvent.class, 1024);

        SlowSyncSubscriber slowSync = new SlowSyncSubscriber("同步A");
        FastSubscriber fastSync = new FastSubscriber("同步B");
        NotifyCenter.registerSubscriber(slowSync);
        NotifyCenter.registerSubscriber(fastSync);

        long start = System.currentTimeMillis();
        NotifyCenter.publishEvent(new TaskEvent(1));
        NotifyCenter.publishEvent(new TaskEvent(2));

        // 等所有事件处理完（2 个事件 × 2 秒慢处理 = 4 秒）
        Thread.sleep(5000);
        System.out.println("总耗时: " + (System.currentTimeMillis() - start) + " ms\n");

        // 清理场景1的订阅者
        NotifyCenter.deregisterSubscriber(slowSync);
        NotifyCenter.deregisterSubscriber(fastSync);

        // ---- 场景2：慢订阅者异步 ----
        System.out.println("=== 场景2：异步回调（慢订阅者 executor 返回线程池） ===");

        SlowAsyncSubscriber slowAsync = new SlowAsyncSubscriber("异步A");
        FastSubscriber fastAsync = new FastSubscriber("同步B");
        NotifyCenter.registerSubscriber(slowAsync);
        NotifyCenter.registerSubscriber(fastAsync);

        start = System.currentTimeMillis();
        NotifyCenter.publishEvent(new TaskEvent(1));
        NotifyCenter.publishEvent(new TaskEvent(2));

        // 慢处理异步执行，快处理立即完成
        // 两个慢处理并行（线程池 2 个线程），约 2 秒完成
        Thread.sleep(3000);
        System.out.println("总耗时: " + (System.currentTimeMillis() - start) + " ms\n");

        System.out.println("=== 分析 ===");
        System.out.println("同步模式：");
        System.out.println("  慢 A 在 Publisher 线程执行，阻塞了队列");
        System.out.println("  快 B 必须等 A 处理完才能收到事件");
        System.out.println("  2 个事件 × (2秒A + 0秒B) = 4 秒");
        System.out.println();
        System.out.println("异步模式：");
        System.out.println("  慢 A 被丢到自己的线程池，Publisher 线程立即返回");
        System.out.println("  快 B 不用等 A，立即处理");
        System.out.println("  2 个事件的 A 并行执行 = 2 秒");

        NotifyCenter.shutdown();
    }
}
