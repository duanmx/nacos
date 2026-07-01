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
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;

import java.util.Arrays;
import java.util.List;

/**
 * Demo7: SmartSubscriber —— 一个订阅者监听多种事件
 *
 * <p>场景：一个"用户操作记录器"需要同时监听 3 种事件
 * <pre>
 *   1. 用户注册事件（普通事件）
 *   2. 用户登录事件（普通事件）
 *   3. 系统关闭事件（慢事件，继承 SlowEvent）
 * </pre>
 *
 * <p>如果用普通 Subscriber，需要写 3 个类。
 * 用 SmartSubscriber，一个类搞定。
 *
 * <p>运行后你会看到：
 * <pre>
 *   [nacos.publisher-...RegisterEvent] 收到事件：用户[张三]注册了
 *   [nacos.publisher-...LoginEvent]    收到事件：用户[张三]登录了，IP=192.168.1.1
 *   [nacos.publisher-SlowEvent]         收到事件：系统正在关闭，原因=维护
 *   [nacos.publisher-...LoginEvent]    收到事件：用户[李四]登录了，IP=10.0.0.1
 *
 *   === 对比 ===
 *   SmartSubscriber：1 个类，3 种事件，1 个 onEvent 方法里 instanceof 分流
 *   普通 Subscriber：需要 3 个类，各自 1 个 onEvent，类型安全
 * </pre>
 *
 * @author nacos
 */
public class Demo7SmartSubscriber {

    // ==================== 定义 3 种事件 ====================

    /** 普通事件1：用户注册 */
    static class UserRegisterEvent extends Event {

        private final String username;

        public UserRegisterEvent(String username) {
            this.username = username;
        }

        public String getUsername() {
            return username;
        }
    }

    /** 普通事件2：用户登录 */
    static class UserLoginEvent extends Event {

        private final String username;
        private final String ip;

        public UserLoginEvent(String username, String ip) {
            this.username = username;
            this.ip = ip;
        }

        public String getUsername() {
            return username;
        }

        public String getIp() {
            return ip;
        }
    }

    /** 慢事件：系统关闭（继承 SlowEvent，走共享发布器） */
    static class SystemShutdownEvent extends com.alibaba.nacos.common.notify.SlowEvent {

        private final String reason;

        public SystemShutdownEvent(String reason) {
            this.reason = reason;
        }

        public String getReason() {
            return reason;
        }
    }

    // ==================== SmartSubscriber：一个类监听 3 种事件 ====================

    /**
     * 对比普通 Subscriber 只能 subscribeType() 返回一种事件类型
     * SmartSubscriber 覆写 subscribeTypes() 返回一个 List，可以包含多种
     */
    static class UserActionLogger extends SmartSubscriber {

        @Override
        public List<Class<? extends Event>> subscribeTypes() {
            // 关键：返回你想监听的所有事件类型
            return Arrays.asList(
                UserRegisterEvent.class,
                UserLoginEvent.class,
                SystemShutdownEvent.class   // 慢事件也能混在一起监听！
            );
        }

        @Override
        public void onEvent(Event event) {
            // 注意：参数是 Event 基类，不是泛型
            // 必须用 instanceof 判断具体类型，然后强转

            if (event instanceof UserRegisterEvent) {
                UserRegisterEvent e = (UserRegisterEvent) event;
                System.out.println("["
                    + Thread.currentThread().getName()
                    + "] 收到事件：用户[" + e.getUsername() + "]注册了");

            } else if (event instanceof UserLoginEvent) {
                UserLoginEvent e = (UserLoginEvent) event;
                System.out.println("["
                    + Thread.currentThread().getName()
                    + "] 收到事件：用户[" + e.getUsername() + "]登录了，IP=" + e.getIp());

            } else if (event instanceof SystemShutdownEvent) {
                SystemShutdownEvent e = (SystemShutdownEvent) event;
                System.out.println("["
                    + Thread.currentThread().getName()
                    + "] 收到事件：系统正在关闭，原因=" + e.getReason());

            } else {
                System.out.println("["
                    + Thread.currentThread().getName()
                    + "] 收到未知事件：" + event.getClass().getSimpleName());
            }
        }
    }

    // ==================== 对比：普通 Subscriber 需要写 3 个类 ====================

    static class UserRegisterLogger extends com.alibaba.nacos.common.notify.listener.Subscriber<UserRegisterEvent> {
        @Override
        public void onEvent(UserRegisterEvent event) {
            // 类型安全：参数已经是 UserRegisterEvent，不需要 instanceof
            System.out.println("["
                + Thread.currentThread().getName()
                + "] [普通Subscriber] 用户[" + event.getUsername() + "]注册了");
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return UserRegisterEvent.class;
        }
    }

    // ==================== 运行 ====================

    public static void main(String[] args) throws InterruptedException {

        // 1. 注册发布器（普通事件需要，慢事件不需要）
        NotifyCenter.registerToPublisher(UserRegisterEvent.class, 1024);
        NotifyCenter.registerToPublisher(UserLoginEvent.class, 1024);

        // 2. 注册 SmartSubscriber（一次注册，自动监听 3 种事件）
        UserActionLogger logger = new UserActionLogger();
        NotifyCenter.registerSubscriber(logger);
        System.out.println("SmartSubscriber 注册完成，监听 3 种事件\n");

        // 3. 发布事件
        System.out.println("=== 发布事件 ===");
        NotifyCenter.publishEvent(new UserRegisterEvent("张三"));
        Thread.sleep(200);

        NotifyCenter.publishEvent(new UserLoginEvent("张三", "192.168.1.1"));
        Thread.sleep(200);

        // 慢事件走共享发布器（不同的线程名）
        NotifyCenter.publishEvent(new SystemShutdownEvent("维护"));
        Thread.sleep(200);

        // 再发一个登录事件
        NotifyCenter.publishEvent(new UserLoginEvent("李四", "10.0.0.1"));
        Thread.sleep(200);

        // 4. 对比
        System.out.println("\n=== 注册普通 Subscriber 做对比 ===");
        UserRegisterLogger regLogger = new UserRegisterLogger();
        NotifyCenter.registerSubscriber(regLogger);
        NotifyCenter.publishEvent(new UserRegisterEvent("王五"));
        Thread.sleep(200);

        System.out.println("\n=== 分析 ===");
        System.out.println("SmartSubscriber：");
        System.out.println("  1 个类，subscribeTypes() 返回 3 种事件");
        System.out.println("  onEvent(Event) 收到基类，需要 instanceof 判断");
        System.out.println("  好处：一个类管所有相关事件，逻辑集中");
        System.out.println("  坏处：类型不安全，漏写 instanceof 分支不会编译报错");
        System.out.println();
        System.out.println("普通 Subscriber：");
        System.out.println("  每种事件一个类，subscribeType() 返回 1 种");
        System.out.println("  onEvent(具体类型) 参数类型安全，直接用");
        System.out.println("  好处：类型安全，编译器帮你检查");
        System.out.println("  坏处：事件多了类也多，注册也多");
        System.out.println();
        System.out.println("关键区别在 onEvent 的参数：");
        System.out.println("  SmartSubscriber:  onEvent(Event event)         → 需要 instanceof");
        System.out.println("  Subscriber<T>:    onEvent(UserRegisterEvent e)  → 直接用，不用转型");

        NotifyCenter.shutdown();
    }
}
