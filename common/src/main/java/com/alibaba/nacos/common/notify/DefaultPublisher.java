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

package com.alibaba.nacos.common.notify;

import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.alibaba.nacos.common.notify.NotifyCenter.ringBufferSize;

/**
 * The default event publisher implementation.
 *
 * <p>Internally, use {@link ArrayBlockingQueue <Event/>} as a message staging queue.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
public class DefaultPublisher extends Thread implements EventPublisher {
    
    protected static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);
    
    private volatile boolean initialized = false;
    
    private volatile boolean shutdown = false;
    
    private Class<? extends Event> eventType;
    
    protected final ConcurrentHashSet<Subscriber> subscribers = new ConcurrentHashSet<>();
    
    private int queueMaxSize = -1;
    
    private BlockingQueue<Event> queue;
    
    protected volatile Long lastEventSequence = -1L;
    
    private static final AtomicReferenceFieldUpdater<DefaultPublisher, Long> UPDATER =
        AtomicReferenceFieldUpdater
            .newUpdater(DefaultPublisher.class, Long.class, "lastEventSequence");
    
    @Override
    public void init(Class<? extends Event> type, int bufferSize) {
        setDaemon(true);
        setName("nacos.publisher-" + type.getName());
        this.eventType = type;
        this.queueMaxSize = bufferSize;
        if (this.queueMaxSize == -1) {
            this.queueMaxSize = ringBufferSize;
        }
        this.queue = new ArrayBlockingQueue<>(this.queueMaxSize);
        start();
    }
    
    public ConcurrentHashSet<Subscriber> getSubscribers() {
        return subscribers;
    }
    
    @Override
    public synchronized void start() {
        if (!initialized) {
            // start just called once
            super.start();
            initialized = true;
        }
    }
    
    @Override
    public long currentEventSize() {
        return queue.size();
    }
    
    @Override
    public void run() {
        openEventHandler();
    }
    
    void openEventHandler() {
        try {
            
            // ============================================================
            // 第一阶段：等待第一个订阅者注册（最多等 60 秒）
            // ============================================================
            // 为什么需要等待？
            //   场景：registerToPublisher() 先执行，registerSubscriber() 后执行
            //   如果 Publisher 线程启动后直接 queue.take()，此时还没有订阅者
            //   事件会被 receiveEvent() 判定 "no subscriber" 并丢弃（见 L178-180）
            //   所以先等订阅者来了再开始消费队列
            //
            // 为什么最多等 60 秒？
            //   防止订阅者永远不注册导致线程死等
            //   60 秒后不管有没有订阅者都进入主循环
            //   如果这时有事件进来但没订阅者，receiveEvent 会打 warn 日志并丢弃
            
            int waitTimes = 60;
            while (!shutdown && !hasSubscriber() && waitTimes > 0) {
                ThreadUtils.sleep(1000L);  // 每秒检查一次
                waitTimes--;
            }
            
            // ============================================================
            // 第二阶段：主循环 —— 不断从队列取事件并处理
            // ============================================================
            while (!shutdown) {
                // queue.take() 是阻塞方法：
                //   队列有事件 → 取出并返回
                //   队列为空   → 线程阻塞等待，直到有事件入队
                //   被 interrupt() → 抛 InterruptedException，跳出循环
                final Event event = queue.take();
                
                // 派发事件给所有匹配的订阅者
                // 内部遍历 subscribers：
                //   1. scopeMatches() 过滤作用域不匹配的
                //   2. ignoreExpireEvent() 过滤过期事件
                //   3. notifySubscriber() 执行回调
                receiveEvent(event);
                
                // 更新 lastEventSequence（记录已处理的最大事件序列号）
                // 用 CAS（AtomicReferenceFieldUpdater）保证线程安全
                // 用途：用于过期事件判断
                //   如果后续收到 sequence 更小的事件，说明是旧事件
                //   订阅者可以通过 ignoreExpireEvent()=true 跳过它
                UPDATER.compareAndSet(this, lastEventSequence,
                    Math.max(lastEventSequence, event.sequence()));
            }
        } catch (InterruptedException e) {
            // shutdown() 中调用了 this.interrupt()
            // queue.take() 被中断后抛 InterruptedException，退出循环
            // 这是正常的关闭路径，不需要打印堆栈（issue #13752）
        } catch (Throwable ex) {
            // 兜底：receiveEvent 或 notifySubscriber 中出现未预期异常
            // 注意：异常会导致线程退出，后续事件将无法处理
            LOGGER.error("Event listener exception : ", ex);
        }
    }
    
    private boolean hasSubscriber() {
        return CollectionUtils.isNotEmpty(subscribers);
    }
    
    @Override
    public void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }
    
    @Override
    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }
    
    @Override
    public boolean publish(Event event) {
        checkIsStart();
        boolean success = this.queue.offer(event);
        if (!success) {
            LOGGER.warn(
                "Unable to plug in due to interruption, synchronize sending time, event : {}",
                event);
            receiveEvent(event);
            return true;
        }
        return true;
    }
    
    void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
        this.queue.clear();
        // Interrupt the thread to stop processing events: queue.take().
        this.interrupt();
    }
    
    public boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Receive and notifySubscriber to process the event.
     *
     * @param event {@link Event}.
     */
    void receiveEvent(Event event) {
        // 获取当前事件的序列号（Event 构造时全局自增的值）
        // 用于后续过期事件判断
        final long currentEventSequence = event.sequence();
        
        // 没有任何订阅者 → 事件丢失，打 warn 日志后直接返回
        // 这通常发生在：注册了发布器但还没来得及注册订阅者，事件就来了
        if (!hasSubscriber()) {
            LOGGER.warn("[NotifyCenter] the {} is lost, because there is no subscriber.", event);
            return;
        }
        
        // 遍历所有订阅者，逐个通知
        // 注意：subscribers 是 ConcurrentHashSet，遍历时可能有并发修改
        //       但不锁，因为注册/注销订阅者频率低，偶尔遍历不到不影响正确性
        for (Subscriber subscriber : subscribers) {
            
            // 过滤1：作用域匹配
            // Subscriber 默认 scopeMatches() 返回 true（全部接收）
            // InstancesChangeNotifier 覆写为按 UUID 比较，实现多 NamingService 实例隔离
            // 不匹配 → 跳过这个订阅者，不影响其他订阅者
            if (!subscriber.scopeMatches(event)) {
                continue;
            }
            
            // 过滤2：过期事件检测
            // lastEventSequence 是已处理的最大序列号（主循环中 CAS 更新）
            // currentEventSequence 是当前事件的序列号
            // 如果当前事件比已处理的还旧 → 说明是队列积压的旧事件
            //
            // 只有订阅者主动设置了 ignoreExpireEvent()=true 才会跳过
            // 默认 ignoreExpireEvent()=false → 所有事件都处理，不管新旧
            //
            // 适用场景：配置变更通知，旧配置已无意义，不需要处理
            // SmartSubscriber 固定返回 false，不做过期跳过
            if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
                LOGGER.debug(
                    "[NotifyCenter] the {} is unacceptable to this subscriber, because had expire",
                    event.getClass());
                continue;
            }
            
            // 执行回调：调用 subscriber.onEvent(event)
            // 内部判断 subscriber.executor()：
            //   返回 null  → 在当前 Publisher 线程同步执行（默认）
            //   返回线程池 → 丢给线程池异步执行
            // 注意：同步模式下如果 onEvent 抛异常，会被 notifySubscriber 内部 try-catch 吞掉
            //       不影响后续订阅者和后续事件
            notifySubscriber(subscriber, event);
        }
    }
    
    @Override
    public void notifySubscriber(final Subscriber subscriber, final Event event) {
        
        LOGGER.debug("[NotifyCenter] the {} will received by {}", event, subscriber);
        
        // 把回调包装成 Runnable，不立即执行
        final Runnable job = () -> subscriber.onEvent(event);
        
        // 订阅者可以覆写 executor() 返回自定义线程池
        // 默认返回 null（在 Publisher 线程同步执行）
        final Executor executor = subscriber.executor();
        
        if (executor != null) {
            // 异步模式：丢给订阅者自己的线程池执行
            // Publisher 线程不阻塞，立即处理下一个事件
            // 适用于：回调耗时长的场景（发邮件、IO 操作等）
            // 风险：onEvent 抛异常由订阅者的线程池处理，Publisher 线程不感知
            executor.execute(job);
        } else {
            // 同步模式：在 Publisher 线程直接调用
            // job.run() 不是新开线程，就是普通方法调用
            // 风险：onEvent 慢会阻塞整个队列，后续事件排队等待
            // 好处：try-catch 兜底，异常不会让 Publisher 线程崩溃
            try {
                job.run();
            } catch (Throwable e) {
                // 吞掉异常，只打日志
                // 目的：一个订阅者出错不影响后续订阅者和后续事件
                LOGGER.error("Event callback exception: ", e);
            }
        }
    }
}
