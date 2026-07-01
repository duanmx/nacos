# Nacos 事件通知机制深度源码解析

> 所有源码基于 `common/src/main/java/com/alibaba/nacos/common/notify/` 包，共 9 个文件。
> 本文不做任何猜测，每一行描述都基于实际源码。

---

## 一、什么是发布-订阅机制

### 1.1 生活中的例子

你关注了一个微信公众号。公众号发文章时，你不需要主动去刷，文章会自动推送到你手机上。

- **公众号** = 发布者（Publisher），负责产出内容
- **你** = 订阅者（Subscriber），被动接收内容
- **微信平台** = 事件总线（EventBus），负责把内容从公众号传递到每个订阅者

### 1.2 软件中的发布-订阅

发布-订阅（Publish-Subscribe，简称 Pub/Sub）是一种**消息通信模式**：

```
发布者 ──→ 事件总线 ──→ 订阅者A
                        ├──→ 订阅者B
                        └──→ 订阅者C
```

核心特点：
1. **解耦**：发布者不认识订阅者，只管把事件丢给事件总线
2. **一对多**：一个事件可以通知多个订阅者
3. **异步**：发布者丢完事件就返回，不等订阅者处理完

### 1.3 Nacos 为什么要用发布-订阅

Nacos 是一个分布式系统，内部有大量"一件事发生，多个组件需要响应"的场景：

| 事件 | 谁发布 | 谁订阅 |
|------|--------|--------|
| 实例列表变了 | ServiceInfoHolder | 用户的 EventListener |
| 集群成员变了 | MemberLookup | MemberChangeListener |
| 配置数据变了 | ConfigChangeNotifier | 客户端推送通道 |
| Raft 选主了 | JRaft | 各个需要感知 Leader 的组件 |

如果不用发布-订阅，每个场景都要写 `if (xxx变了) { 通知A(); 通知B(); 通知C(); }`，耦合度极高。有了 NotifyCenter，发布者只需要一行 `NotifyCenter.publishEvent(event)`，订阅者各管各的。

---

## 二、notify 包文件总览

```
common/src/main/java/com/alibaba/nacos/common/notify/
│
├── NotifyCenter.java            ← 事件总线入口（全局唯一，所有操作都通过它）
│
├── Event.java                   ← 事件基类（所有事件的父类）
├── SlowEvent.java               ← 慢事件基类（所有慢事件的父类，共享一个发布器）
│
├── listener/
│   ├── Subscriber.java           ← 订阅者基类（订阅一种事件）
│   └── SmartSubscriber.java      ← 智能订阅者（可订阅多种事件）
│
├── EventPublisher.java           ← 发布器接口（定义发布、通知等方法）
├── EventPublisherFactory.java   ← 发布器工厂（函数式接口，用 Lambda 创建发布器）
│
├── DefaultPublisher.java         ← 默认发布器（Thread + ArrayBlockingQueue）
├── DefaultSharePublisher.java    ← 共享发布器（多种慢事件共用一个实例）
├── ShardedEventPublisher.java   ← 分片发布器接口（带事件类型的增删订阅者）
│
└── demo/                        ← 教学用 Demo（7 个可运行示例）
```

按职责分为四层：

| 层 | 文件 | 作用 |
|----|------|------|
| 入口层 | NotifyCenter | 全局唯一事件总线，所有操作的统一入口 |
| 事件层 | Event、SlowEvent | 定义"发生了什么" |
| 订阅层 | Subscriber、SmartSubscriber | 定义"谁来处理" |
| 发布层 | EventPublisher、DefaultPublisher、DefaultSharePublisher、ShardedEventPublisher、EventPublisherFactory | 定义"怎么传递" |

---

## 三、类关系图与角色说明

### 3.1 类关系图

```
    ┌──────────────────────────────────────────────────────────────┐
    │                      NotifyCenter                             │
    │                   （静态单例，全局入口）                          │
    │                                                              │
    │  publisherMap (ConcurrentHashMap)                            │
    │  ┌────────────────────────────────────────────────────┐      │
    │  │ "InstancesChangeEvent" → DefaultPublisher (线程A)   │      │
    │  │ "LocalDataChangeEvent"  → DefaultPublisher (线程B)  │      │
    │  │ "MembersChangeEvent"    → DefaultPublisher (线程C)   │      │
    │  └────────────────────────────────────────────────────┘      │
    │                                                              │
    │  sharePublisher (DefaultSharePublisher，单例)                  │
    │  ┌────────────────────────────────────────────────────┐      │
    │  │ 线程 + 队列(1024)                                   │      │
    │  │ subMappings:                                        │      │
    │  │   RaftEvent        → [Subscriber1, Subscriber2]     │      │
    │  │   IPChangeEvent     → [Subscriber3]                 │      │
    │  │   DerbyLoadEvent    → [Subscriber4]                 │      │
    │  └────────────────────────────────────────────────────┘      │
    └──────────────────────────────────────────────────────────────┘
         ↑ publishEvent()                    ↑ registerSubscriber()
         │                                    │
   发布者（业务代码）                      订阅者（业务代码）
```

### 3.2 每个类的角色

| 类 | 角色 | 一句话说明 |
|----|------|-----------|
| `NotifyCenter` | 事件总线 | 全局唯一入口，管理所有发布器和订阅者 |
| `Event` | 事件 | "发生了什么"的载体，带全局自增序列号 |
| `SlowEvent` | 慢事件 | 低频事件的基类，所有慢事件共享一个发布器 |
| `Subscriber` | 订阅者 | 接收一种事件并处理，可自定义回调方式 |
| `SmartSubscriber` | 智能订阅者 | 一次订阅多种事件，用 instanceof 分流 |
| `EventPublisher` | 发布器接口 | 定义发布/通知/增删订阅者的契约 |
| `EventPublisherFactory` | 发布器工厂 | 用 Lambda 创建发布器实例 |
| `DefaultPublisher` | 默认发布器 | Thread + ArrayBlockingQueue，一种事件一个实例 |
| `DefaultSharePublisher` | 共享发布器 | 继承 DefaultPublisher，多种慢事件共用一个实例 |
| `ShardedEventPublisher` | 分片接口 | 带事件类型的增删订阅者方法 |

### 3.3 继承与实现关系

```
EventPublisher（接口）
    │
    ├── DefaultPublisher（继承 Thread + 实现接口）
    │       │
    │       └── DefaultSharePublisher（继承 DefaultPublisher + 实现 ShardedEventPublisher）
    │
    └── ShardedEventPublisher（接口，继承 EventPublisher）

Event（抽象类）
    │
    └── SlowEvent（继承 Event）

Subscriber<T>（抽象类）
    │
    └── SmartSubscriber（继承 Subscriber<Event>）
```

---

## 四、核心设计思路

### 4.1 普通事件 vs 慢事件 —— 为什么要分两种？

Nacos 把事件分成两类，用完全不同的发布器策略处理：

| 对比项 | 普通事件（继承 Event） | 慢事件（继承 SlowEvent） |
|--------|----------------------|------------------------|
| 频率 | 高频（实例变更、配置变更） | 低频（IP 变更、Raft 选举、DB 加载） |
| 发布器 | 每种事件类型一个 `DefaultPublisher` | 所有类型共享一个 `DefaultSharePublisher` |
| 队列 | 独立队列（默认 16384） | 共享队列（默认 1024） |
| 线程 | 独立线程 | 共享线程 |
| sequence | 全局自增 | 恒为 0 |
| 过期检测 | 支持 | 不支持（永远 0） |
| 隔离性 | 事件 A 积压不影响事件 B | 所有慢事件互相影响 |

**为什么这样设计？**

普通事件是高频的，比如实例列表变更、配置数据变更，可能每秒几十个。如果 A 事件的订阅者处理慢导致队列积压，不能影响 B 事件的正常处理。所以每种普通事件需要独立的队列和线程。

慢事件是低频的，比如 IP 变更可能几小时才发生一次。给每种慢事件开一个线程太浪费内存。所有慢事件共用一个队列和线程就行，反正不会经常撞上。

**SlowEvent 的 sequence 为什么恒为 0？**

```java
public abstract class SlowEvent extends Event {
    @Override
    public long sequence() {
        return 0;  // 恒为 0
    }
}
```

因为慢事件共享一个队列，多种事件类型混在一个队列里排队。Event 基类的 sequence 是全局自增的，不同类型的事件 sequence 不可比（RaftEvent 的 seq=5 和 IPChangeEvent 的 seq=3 没有先后关系）。所以 SlowEvent 直接返回 0，让过期检测逻辑失效，所有慢事件都处理。

**代码中如何判断是慢事件？**

```java
// NotifyCenter 中到处可见这个判断：
if (ClassUtils.isAssignableFrom(SlowEvent.class, eventType)) {
    // 是 SlowEvent 的子类 → 走共享发布器
    return INSTANCE.sharePublisher.publish(event);
}
// 否则 → 走独立发布器
EventPublisher publisher = INSTANCE.publisherMap.get(topic);
```

### 4.2 Subscriber vs SmartSubscriber —— 什么时候用哪个？

`Subscriber<T>` 订阅一种事件，泛型 T 保证了类型安全：

```java
class MyListener extends Subscriber<InstancesChangeEvent> {
    public void onEvent(InstancesChangeEvent event) {
        // 参数已经是具体类型，直接用
    }
    public Class<? extends Event> subscribeType() {
        return InstancesChangeEvent.class;
    }
}
```

`SmartSubscriber` 订阅多种事件，需要在 `onEvent` 里用 `instanceof` 分流：

```java
class MyMultiListener extends SmartSubscriber {
    public List<Class<? extends Event>> subscribeTypes() {
        return Arrays.asList(EventA.class, EventB.class, EventC.class);
    }
    public void onEvent(Event event) {
        if (event instanceof EventA) { ... }
        else if (event instanceof EventB) { ... }
        else if (event instanceof EventC) { ... }
    }
}
```

| 对比 | Subscriber | SmartSubscriber |
|------|------------|-----------------|
| 订阅事件数 | 1 种 | N 种 |
| 类型安全 | 是（泛型） | 否（需要 instanceof） |
| 过期忽略 | 可自定义 | 固定 false |
| 选型原则 | 只监听 1 种事件 → 用这个 | 监听多种相关事件 → 用这个 |

Nacos 实际数据：21 个 Subscriber vs 16 个 SmartSubscriber，基本对半开。

### 4.3 scope 隔离机制 —— 多实例不串台

同一个 JVM 中可能创建多个 NamingService 实例（比如连不同 Nacos 服务端），每个实例有自己的 UUID scope。事件发布时带上 scope，订阅者收到后用 `scopeMatches()` 判断是否属于自己。

```java
// NacosNamingService.init() 生成唯一 scope
this.notifierEventScope = UUID.randomUUID().toString();

// 事件构造时带上 scope
new InstancesChangeEvent(notifierEventScope, serviceName, ...);

// 订阅者过滤
public boolean scopeMatches(InstancesChangeEvent event) {
    return this.eventScope.equals(event.scope());
}
```

如果没有 scope：ns1 的实例变更会触发 ns2 的 listener，造成误通知。

**注意**：只有普通事件走 `scopeMatches` 过滤（在 `DefaultPublisher.receiveEvent` 中）。慢事件不走 scope 过滤（在 `DefaultSharePublisher.receiveEvent` 中用 `subMappings` 精准匹配，不需要 scope）。

### 4.4 SPI 可扩展 —— 发布器可替换

```java
// NotifyCenter 静态块 L79-86：
final Collection<EventPublisher> publishers = NacosServiceLoader.load(EventPublisher.class);
if (iterator.hasNext()) {
    clazz = iterator.next().getClass();  // SPI 有自定义实现 → 用它
} else {
    clazz = DefaultPublisher.class;       // 没有 → 用默认实现
}
```

在 `META-INF/services/com.alibaba.nacos.common.notify.EventPublisher` 注册自定义类即可替换默认发布器。

### 4.5 无参构造 + init 初始化模式

所有通过反射创建的类（`DefaultPublisher`、`NacosJsonAdapter` 等）都使用这个模式：

```java
// 第1步：反射无参构造 → 创建空壳对象（字段都是默认值）
EventPublisher publisher = clazz.newInstance();

// 第2步：init() 填充字段 + 启动线程
publisher.init(eventType, queueSize);
```

为什么不用带参数的构造方法？因为 `clazz.newInstance()` 只支持无参构造，SPI/反射场景的标准做法。

---

## 五、如何使用（案例）

### 5.1 最简用法：三步曲

```java
// 1. 注册发布器（普通事件需要，慢事件不需要）
NotifyCenter.registerToPublisher(MyEvent.class, 16384);

// 2. 注册订阅者
NotifyCenter.registerSubscriber(new Subscriber<MyEvent>() {
    @Override
    public void onEvent(MyEvent event) {
        System.out.println("收到事件：" + event);
    }

    @Override
    public Class<? extends Event> subscribeType() {
        return MyEvent.class;
    }
});

// 3. 发布事件
NotifyCenter.publishEvent(new MyEvent());
```

### 5.2 慢事件用法：不需要注册发布器

```java
// 定义慢事件
class MySlowEvent extends SlowEvent {
    String data;
}

// 注册订阅者（不需要 registerToPublisher！）
NotifyCenter.registerSubscriber(new Subscriber<MySlowEvent>() {
    @Override
    public void onEvent(MySlowEvent event) { ... }

    @Override
    public Class<? extends Event> subscribeType() {
        return MySlowEvent.class;
    }
});

// 发布
NotifyCenter.publishEvent(new MySlowEvent());
// → 自动走 sharePublisher
```

### 5.3 scope 隔离用法

```java
// 实例1
String scope1 = UUID.randomUUID().toString();
NotifyCenter.registerToPublisher(OrderEvent.class, 1024);
NotifyCenter.registerSubscriber(new MyNotifier("listener1", scope1));

// 实例2（同一个 JVM）
String scope2 = UUID.randomUUID().toString();
NotifyCenter.registerSubscriber(new MyNotifier("listener2", scope2));

// 实例1 发布事件
NotifyCenter.publishEvent(new OrderEvent("data", scope1));
// → listener1 收到（scope 匹配）
// → listener2 跳过（scope 不匹配）
```

### 5.4 SmartSubscriber 多事件订阅

```java
class MyMultiListener extends SmartSubscriber {
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        return Arrays.asList(OrderEvent.class, PaymentEvent.class);
    }

    @Override
    public void onEvent(Event event) {
        if (event instanceof OrderEvent) {
            handleOrder((OrderEvent) event);
        } else if (event instanceof PaymentEvent) {
            handlePayment((PaymentEvent) event);
        }
    }
}

// 一次注册，监听两种事件
NotifyCenter.registerSubscriber(new MyMultiListener());
```

### 5.5 异步回调用法

```java
class AsyncSubscriber extends Subscriber<MyEvent> {

    private final Executor executor = Executors.newFixedThreadPool(4);

    @Override
    public void onEvent(MyEvent event) {
        // 耗时操作...
    }

    @Override
    public Class<? extends Event> subscribeType() {
        return MyEvent.class;
    }

    @Override
    public Executor executor() {
        return executor;  // 返回自定义线程池 → 异步执行
    }
}
```

### 5.6 可运行 Demo

`demo/` 目录下有 7 个可运行的完整示例：

| Demo | 说明 |
|------|------|
| Demo1BasicPubSub | 最简发布订阅（热身） |
| Demo2NormalVsSlowEvent | 普通事件 vs 慢事件对比 |
| Demo3ScopeIsolation | scope 多实例隔离 |
| Demo4SyncVsAsync | 同步回调 vs 异步回调 |
| Demo5EdgeCases | 队列满降级 + 过期事件丢弃 |
| Demo6LambdaSyntax | Lambda 语法糖拆解 |
| Demo7SmartSubscriber | SmartSubscriber 多事件监听 |

---

## 六、核心类源码分析

### 6.1 Event — 事件基类

> 源码：`Event.java`（64 行）

```java
public abstract class Event implements Serializable {

    private static final AtomicLong SEQUENCE = new AtomicLong(0);

    // 每个事件构造时分配一个全局自增序列号
    private final long sequence = SEQUENCE.getAndIncrement();

    public long sequence() {
        return sequence;
    }

    // 事件作用域，默认 null（全局可见）
    // 子类可覆写为具体 UUID，实现多实例隔离
    public String scope() {
        return null;
    }

    // 是否为插件事件，true 时无发布器也不警告（静默丢弃）
    public boolean isPluginEvent() {
        return false;
    }
}
```

三个方法的用途：
- `sequence()`：全局自增序列号，用于过期事件检测。队列积压时旧事件的 sequence 比已处理的小，可被跳过。
- `scope()`：事件作用域标识。默认 null 表示全局可见，覆写后可实现多实例隔离。
- `isPluginEvent()`：插件事件标记。如果事件没有对应的发布器，普通事件会打 warn 日志，插件事件静默丢弃。

### 6.2 SlowEvent — 慢事件基类

> 源码：`SlowEvent.java`（32 行）

```java
public abstract class SlowEvent extends Event {

    @Override
    public long sequence() {
        return 0;  // 恒为 0，不做过期判断
    }
}
```

为什么返回 0？因为慢事件共享一个队列，多种事件类型混排在一起，不同类型的 sequence 不可比。直接返回 0 让过期检测逻辑失效，所有慢事件都处理。

### 6.3 Subscriber — 订阅者基类

> 源码：`listener/Subscriber.java`（74 行）

```java
public abstract class Subscriber<T extends Event> {

    // 【必须实现】收到事件后的回调逻辑
    public abstract void onEvent(T event);

    // 【必须实现】声明订阅哪种事件
    public abstract Class<? extends Event> subscribeType();

    // 回调执行器，null 表示在 Publisher 线程同步执行
    public Executor executor() {
        return null;
    }

    // 是否忽略过期事件，默认 false
    public boolean ignoreExpireEvent() {
        return false;
    }

    // 作用域匹配，默认 true（匹配所有作用域）
    public boolean scopeMatches(T event) {
        return true;
    }
}
```

五个方法中，两个必须实现（`onEvent` + `subscribeType`），三个可选覆写：

| 方法 | 默认行为 | 覆写影响 |
|------|----------|----------|
| `executor()` | 返回 null → 同步执行 | 返回线程池 → 异步执行，不阻塞 Publisher 线程 |
| `ignoreExpireEvent()` | 返回 false | 返回 true → sequence 小于已处理的旧事件被跳过 |
| `scopeMatches()` | 返回 true | 覆写 → 按 scope 过滤，不匹配的事件被跳过 |

### 6.4 SmartSubscriber — 智能订阅者

> 源码：`listener/SmartSubscriber.java`（48 行）

```java
public abstract class SmartSubscriber extends Subscriber<Event> {

    // 返回要订阅的所有事件类型
    public abstract List<Class<? extends Event>> subscribeTypes();

    @Override
    public final Class<? extends Event> subscribeType() {
        return null;  // 不再使用单一类型
    }

    @Override
    public final boolean ignoreExpireEvent() {
        return false;  // 固定不支持过期忽略
    }
}
```

`subscribeType()` 和 `ignoreExpireEvent()` 被标记为 `final`，不允许覆写。因为 SmartSubscriber 监听多种事件，单一事件类型的过期判断没有意义。

### 6.5 EventPublisher — 发布器接口

> 源码：`EventPublisher.java`（76 行）

```java
public interface EventPublisher extends Closeable {

    void init(Class<? extends Event> type, int bufferSize);  // 初始化
    long currentEventSize();                                  // 队列中积压的事件数
    void addSubscriber(Subscriber subscriber);                // 添加订阅者
    void removeSubscriber(Subscriber subscriber);             // 移除订阅者
    boolean publish(Event event);                             // 发布事件
    void notifySubscriber(Subscriber subscriber, Event event); // 通知单个订阅者
}
```

### 6.6 EventPublisherFactory — 发布器工厂

> 源码：`EventPublisherFactory.java`（39 行）

```java
public interface EventPublisherFactory
    extends BiFunction<Class<? extends Event>, Integer, EventPublisher> {

    EventPublisher apply(Class<? extends Event> eventType, Integer maxQueueSize);
}
```

继承 `BiFunction`，是函数式接口。在 `NotifyCenter` 静态块中用 Lambda 创建：

```java
DEFAULT_PUBLISHER_FACTORY = (cls, buffer) -> {
    EventPublisher publisher = clazz.newInstance();  // 反射创建
    publisher.init(cls, buffer);                     // 初始化
    return publisher;
};
```

### 6.7 DefaultPublisher — 默认发布器（核心）

> 源码：`DefaultPublisher.java`（原 222 行，已加注释约 250 行）

这是最核心的类。**继承 Thread**，是一个独立线程 + 阻塞队列的组合。

#### 类结构

```java
public class DefaultPublisher extends Thread implements EventPublisher {

    private volatile boolean initialized = false;       // 是否已启动
    private volatile boolean shutdown = false;          // 是否已关闭
    private Class<? extends Event> eventType;            // 绑定的事件类型
    protected final ConcurrentHashSet<Subscriber> subscribers;  // 订阅者集合
    private int queueMaxSize = -1;                       // 队列大小
    private BlockingQueue<Event> queue;                  // 事件暂存队列
    protected volatile Long lastEventSequence = -1L;    // 最后处理的事件序列号

    // CAS 更新 lastEventSequence
    private static final AtomicReferenceFieldUpdater<DefaultPublisher, Long> UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(DefaultPublisher.class, Long.class, "lastEventSequence");
}
```

#### init() — 初始化并启动线程

```java
public void init(Class<? extends Event> type, int bufferSize) {
    setDaemon(true);                                    // 守护线程，不阻止 JVM 退出
    setName("nacos.publisher-" + type.getName());       // 线程名
    this.eventType = type;
    this.queueMaxSize = bufferSize;
    this.queue = new ArrayBlockingQueue<>(this.queueMaxSize);
    start();                                            // 启动线程
}
```

为什么设为守护线程？因为 Publisher 线程的主循环是死循环 `while(!shutdown) { queue.take(); }`，如果不设守护线程，JVM 永远退不出来。

#### run() — 线程主循环

```java
public void run() {
    openEventHandler();
}

void openEventHandler() {
    try {
        // 第一阶段：等待第一个订阅者注册（最多等 60 秒）
        // 防止发布器启动后还没注册订阅者，事件就被丢弃
        int waitTimes = 60;
        while (!shutdown && !hasSubscriber() && waitTimes > 0) {
            ThreadUtils.sleep(1000L);
            waitTimes--;
        }

        // 第二阶段：主循环——不断从队列取事件并处理
        while (!shutdown) {
            final Event event = queue.take();           // 阻塞直到有事件
            receiveEvent(event);                        // 派发给所有订阅者
            UPDATER.compareAndSet(this, lastEventSequence,
                Math.max(lastEventSequence, event.sequence()));  // CAS 更新最大序列号
        }
    } catch (InterruptedException e) {
        // shutdown() 中调用了 interrupt()，正常退出
    } catch (Throwable ex) {
        LOGGER.error("Event listener exception : ", ex);
    }
}
```

#### publish() — 发布事件

```java
public boolean publish(Event event) {
    checkIsStart();
    boolean success = this.queue.offer(event);  // 非阻塞放入队列
    if (!success) {
        // 队列满了！降级为同步直接派发
        LOGGER.warn("Unable to plug in due to interruption, synchronize sending time, event : {}", event);
        receiveEvent(event);  // 同步调用，不经过队列
        return true;
    }
    return true;
}
```

`offer()` 是非阻塞的，队列满了立即返回 false。此时降级为同步直接调用 `receiveEvent()`，确保事件不丢失（但会阻塞发布者线程）。

#### receiveEvent() — 派发事件给订阅者

```java
void receiveEvent(Event event) {
    final long currentEventSequence = event.sequence();

    if (!hasSubscriber()) {
        LOGGER.warn("[NotifyCenter] the {} is lost, because there is no subscriber.", event);
        return;
    }

    for (Subscriber subscriber : subscribers) {
        // 过滤1：作用域不匹配 → 跳过
        if (!subscriber.scopeMatches(event)) {
            continue;
        }

        // 过滤2：过期事件且订阅者设置了忽略过期 → 跳过
        if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
            continue;
        }

        // 通过两道过滤 → 执行回调
        notifySubscriber(subscriber, event);
    }
}
```

#### notifySubscriber() — 执行回调

```java
public void notifySubscriber(final Subscriber subscriber, final Event event) {
    final Runnable job = () -> subscriber.onEvent(event);
    final Executor executor = subscriber.executor();

    if (executor != null) {
        executor.execute(job);      // 异步：丢给订阅者的线程池
    } else {
        try {
            job.run();              // 同步：在 Publisher 线程直接执行
        } catch (Throwable e) {
            LOGGER.error("Event callback exception: ", e);  // 吞异常，不影响后续
        }
    }
}
```

### 6.8 DefaultSharePublisher — 共享发布器

> 源码：`DefaultSharePublisher.java`（112 行）

继承 `DefaultPublisher`，实现 `ShardedEventPublisher`。多种慢事件共用一个实例。

#### 核心结构

```java
public class DefaultSharePublisher extends DefaultPublisher implements ShardedEventPublisher {

    // 额外的映射表：事件类型 → 订阅者集合
    private final Map<Class<? extends SlowEvent>, Set<Subscriber>> subMappings =
        new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();
}
```

#### receiveEvent() — 覆盖父类，按事件类型精准派发

```java
@Override
public void receiveEvent(Event event) {
    final long currentEventSequence = event.sequence();

    // 用事件的实际 Class 查 subMappings，O(1) 查找
    final Class<? extends SlowEvent> slowEventType =
        (Class<? extends SlowEvent>) event.getClass();

    Set<Subscriber> subscribers = subMappings.get(slowEventType);
    if (null == subscribers) {
        return;  // 没有订阅者
    }

    // 遍历该事件类型对应的订阅者
    for (Subscriber subscriber : subscribers) {
        if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
            continue;
        }
        notifySubscriber(subscriber, event);
    }
}
```

与父类 `DefaultPublisher.receiveEvent()` 的区别：
- 父类：遍历所有 subscribers，用 `scopeMatches` 过滤
- 子类：先用 `event.getClass()` 查 `subMappings` Map 精准找到订阅者集合，**不做 scopeMatches 过滤**

### 6.9 NotifyCenter — 事件总线入口

> 源码：`NotifyCenter.java`（392 行）

#### 静态初始化块

```java
static {
    // 1. 读取队列大小配置
    ringBufferSize = Integer.getInteger("nacos.core.notify.ring-buffer-size", 16384);
    shareBufferSize = Integer.getInteger("nacos.core.notify.share-buffer-size", 1024);

    // 2. SPI 加载自定义发布器（没有则用 DefaultPublisher）
    final Collection<EventPublisher> publishers = NacosServiceLoader.load(EventPublisher.class);
    Iterator<EventPublisher> iterator = publishers.iterator();
    if (iterator.hasNext()) {
        clazz = iterator.next().getClass();
    } else {
        clazz = DefaultPublisher.class;
    }

    // 3. 创建默认工厂（Lambda）
    DEFAULT_PUBLISHER_FACTORY = (cls, buffer) -> {
        EventPublisher publisher = clazz.newInstance();
        publisher.init(cls, buffer);
        return publisher;
    };

    // 4. 创建共享发布器（单例，所有 SlowEvent 共用）
    INSTANCE.sharePublisher = new DefaultSharePublisher();
    INSTANCE.sharePublisher.init(SlowEvent.class, shareBufferSize);

    // 5. 注册 JVM 关闭钩子
    ThreadUtils.addShutdownHook(NotifyCenter::shutdown);
}
```

#### publishEvent() — 发布事件

```java
private static boolean publishEvent(
    final Class<? extends Event> eventType, final Event event) {

    // 慢事件 → 共享发布器
    if (ClassUtils.isAssignableFrom(SlowEvent.class, eventType)) {
        return INSTANCE.sharePublisher.publish(event);
    }

    // 普通事件 → 查 publisherMap
    final String topic = ClassUtils.getCanonicalName(eventType);
    EventPublisher publisher = INSTANCE.publisherMap.get(topic);

    if (publisher != null) {
        return publisher.publish(event);
    }

    // 插件事件无发布器 → 静默丢弃
    if (event.isPluginEvent()) {
        return true;
    }

    // 普通事件无发布器 → 警告
    LOGGER.warn("There are no [{}] publishers for this event, please register", topic);
    return false;
}
```

#### registerSubscriber() — 注册订阅者

```java
private static void registerSubscriber(
    final Subscriber consumer, final EventPublisherFactory factory) {

    // SmartSubscriber：遍历所有事件类型，逐个注册
    if (consumer instanceof SmartSubscriber) {
        for (Class<? extends Event> subscribeType : ((SmartSubscriber) consumer).subscribeTypes()) {
            if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
                INSTANCE.sharePublisher.addSubscriber(consumer, subscribeType);
            } else {
                addSubscriber(consumer, subscribeType, factory);
            }
        }
        return;
    }

    // 普通 Subscriber：注册一种事件
    final Class<? extends Event> subscribeType = consumer.subscribeType();
    if (ClassUtils.isAssignableFrom(SlowEvent.class, subscribeType)) {
        INSTANCE.sharePublisher.addSubscriber(consumer, subscribeType);
        return;
    }

    addSubscriber(consumer, subscribeType, factory);
}
```

注册时如果发布器不存在，会自动创建（懒加载）：

```java
private static void addSubscriber(...) {
    final String topic = ClassUtils.getCanonicalName(subscribeType);

    synchronized (NotifyCenter.class) {
        // computeIfAbsent：不存在才创建
        MapUtil.computeIfAbsent(INSTANCE.publisherMap, topic, factory, subscribeType, ringBufferSize);
    }

    EventPublisher publisher = INSTANCE.publisherMap.get(topic);
    if (publisher instanceof ShardedEventPublisher) {
        ((ShardedEventPublisher) publisher).addSubscriber(consumer, subscribeType);
    } else {
        publisher.addSubscriber(consumer);
    }
}
```

`synchronized` + `computeIfAbsent` 是因为 `ConcurrentHashMap.computeIfAbsent` 在 Java 8 有已知 bug（JDK-8062841），需要外部加锁。

---

## 七、异常场景分析

### 7.1 事件何时会丢失

| 场景 | 丢失？ | 原因 | 源码位置 |
|------|--------|------|----------|
| 没有注册发布器就发事件 | **是** | `publishEvent` 找不到 publisher，打 warn 后返回 false | `NotifyCenter L312` |
| 没有订阅者就发事件 | **是** | `receiveEvent` 发现 `!hasSubscriber()`，打 warn 后 return | `DefaultPublisher L178-180` |
| Publisher 线程启动后 60 秒内没注册订阅者 | **是** | 超时后进入主循环，事件入队后被 receiveEvent 判定无订阅者丢弃 | `DefaultPublisher L106-109` |
| 队列满 | **否** | 降级为同步直接调 `receiveEvent`，事件不丢失 | `DefaultPublisher L142-147` |
| 回调抛异常 | **否** | `notifySubscriber` 内 try-catch 吞掉，不影响事件已处理的状态 | `DefaultPublisher L214-217` |
| 订阅者设置了 ignoreExpireEvent | **可能** | sequence 小于 lastEventSequence 的旧事件被跳过 | `DefaultPublisher L190-195` |
| JVM 异常退出 | **可能** | 队列中未处理的事件丢失（守护线程被强制终止） | - |

### 7.2 队列满了会怎样

```java
// DefaultPublisher.publish()
boolean success = this.queue.offer(event);  // 非阻塞
if (!success) {
    // 队列满了 → 降级为同步直接派发
    receiveEvent(event);
    return true;
}
```

**队列满时的完整链路：**

```
正常情况：
  publish() → queue.offer() 成功 → Publisher 线程 take() → receiveEvent() → notifySubscriber()
  发布者线程：不阻塞，立即返回
  Publisher 线程：异步处理

队列满时：
  publish() → queue.offer() 失败 → receiveEvent() 同步调用 → notifySubscriber()
  发布者线程：被阻塞，直到所有订阅者回调执行完
  Publisher 线程：不参与（没经过队列）
```

**影响分析：**
1. 发布者线程被阻塞（可能是主线程或 gRPC 处理线程）
2. 如果订阅者 `executor()` 返回 null（同步执行），发布者线程要等回调完成
3. 如果订阅者 `executor()` 返回线程池，发布者线程只等 `executor.execute()` 返回（很快）

**解决方案：**
- 增大队列：`-Dnacos.core.notify.ring-buffer-size=65536`
- 订阅者覆写 `executor()` 返回独立线程池
- 优化回调逻辑减少耗时

### 7.3 回调异常会怎样

```java
// DefaultPublisher.notifySubscriber() 同步模式：
try {
    job.run();  // subscriber.onEvent(event)
} catch (Throwable e) {
    LOGGER.error("Event callback exception: ", e);  // 吞异常
}
```

异常被 try-catch 吞掉，只打日志。**一个订阅者出错不影响其他订阅者和后续事件**。

但注意：异步模式下（`executor != null`），异常由订阅者的线程池处理，Publisher 线程不感知。如果线程池没有配置异常处理器，异常可能导致线程池线程终止。

### 7.4 过期事件处理

当队列积压时，旧事件的 sequence 比已处理的小：

```java
if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
    // 跳过过期事件
    continue;
}
```

适用场景：配置变更通知，旧配置已无意义。不适用场景：审计日志，每个变更都要记录。

---

## 八、线程安全分析

### 8.1 NotifyCenter 层面

| 操作 | 并发控制 | 安全性 | 说明 |
|------|----------|--------|------|
| `registerToPublisher` | `synchronized(NotifyCenter.class)` | 安全 | 防止并发创建重复 Publisher |
| `addSubscriber` | `synchronized(NotifyCenter.class)` | 安全 | 同上 |
| `publishEvent` | 无锁 | 安全 | 只读 `publisherMap`（ConcurrentHashMap） |
| `registerSubscriber` | 同步块内创建 Publisher | 安全 | 创建后 addSubscriber 无锁 |
| `shutdown` | `AtomicBoolean.compareAndSet` | 安全 | 保证只关闭一次 |

### 8.2 DefaultPublisher 层面

| 字段 | 并发控制 | 安全性 | 说明 |
|------|----------|--------|------|
| `initialized` | `volatile` | 安全 | 可见性保证，`start()` 用 `synchronized` 保证原子 |
| `shutdown` | `volatile` | 安全 | 可见性保证 |
| `subscribers` | `ConcurrentHashSet` | 安全 | 并发集合，内部用 ConcurrentHashMap |
| `queue` | `ArrayBlockingQueue` | 安全 | 内部用 ReentrantLock |
| `lastEventSequence` | `AtomicReferenceFieldUpdater` (CAS) | 安全 | 无锁原子更新 |
| `start()` | `synchronized` | 安全 | 保证 `super.start()` 只调一次 |

### 8.3 潜在风险点

1. **Publisher 线程回调慢阻塞**：`subscriber.executor()` 返回 null 且 `onEvent()` 耗时长，整个队列被阻塞，后续事件排队等待
2. **队列满时同步降级阻塞发布者**：`publish()` 同步调用 `receiveEvent()`，发布者线程被阻塞
3. **ConcurrentHashSet 遍历弱一致性**：遍历 subscribers 时如果有并发注册/注销，可能看不到最新的订阅者，但不会报错
4. **Publisher 线程异常退出**：如果 `receiveEvent` 或 `notifySubscriber` 抛出未被捕获的异常（Throwable），线程退出，后续事件堆积无人处理。不过 `openEventHandler` 有 `catch (Throwable)` 兜底，但会导致线程结束

---

## 九、思考与改进建议

### 9.1 这个方案的优点

1. **简单可靠**：一个线程 + 一个队列，没有复杂的锁竞争
2. **隔离性好**：普通事件之间互不影响
3. **资源可控**：慢事件共享线程，不浪费
4. **SPI 可扩展**：可以替换默认发布器实现
5. **scope 隔离**：多实例场景下不会串台

### 9.2 这个方案的缺点

**1. 事件可能丢失**

```
没有订阅者时事件直接丢弃，只打 warn 日志。
虽然等了 60 秒，但如果 60 秒后订阅者才注册，期间的事件就丢了。
```

改进建议：引入持久化队列或死信队列，丢弃前先存起来。

**2. 队列满时降级同步阻塞发布者**

```
队列满 → publish() 同步调 receiveEvent() → 阻塞发布者线程
如果发布者是 gRPC 处理线程，会导致 gRPC 线程被阻塞，影响其他请求处理。
```

改进建议：队列满时应该阻塞等待（用 `put()` 而不是 `offer()` + 降级），或者直接丢弃并告警。

**3. 单线程处理瓶颈**

```
每个事件类型只有一个 Publisher 线程处理。
如果订阅者回调慢（executor 返回 null），所有事件排队等待。
即使有多个订阅者可以并行处理，也是串行执行的。
```

改进建议：引入线程池模型（类似 Disruptor 的 WorkerPool），多个线程并行消费同一个队列。

**4. 无背压机制**

```
发布者不知道消费者处理不过来，只管往队列里塞。
队列满了才降级，没有提前预警。
```

改进建议：引入水位线机制，队列使用超过 80% 时打 warn 日志，提醒扩容。

**5. 异常处理过于简单**

```
回调异常被 catch(Throwable) 吞掉只打日志，没有重试机制。
如果某个事件处理失败，没有补偿机制。
```

改进建议：引入重试队列或死信队列，处理失败的事件可以重试。

**6. 无事件追踪**

```
事件发布后无法追踪是否被成功处理。
没有 Metrics 暴露队列积压情况。
```

改进建议：添加 Metrics（队列大小、处理延迟、丢弃数量），接入监控系统。

**7. 守护线程的隐患**

```
Publisher 是守护线程，JVM 退出时会被强制终止。
如果队列中还有未处理的事件，会丢失。
```

改进建议：shutdown 时应该等待队列排空再退出（graceful shutdown）。

### 9.3 更好的替代方案

| 方案 | 优点 | 缺点 | 适用场景 |
|------|------|------|----------|
| **Disruptor** | 无锁环形队列，吞吐量高 10 倍 | 依赖外部库，API 复杂 | 超高频场景 |
| **Spring Event** | 集成 Spring 生态，注解驱动 | 依赖 Spring，客户端 SDK 不能用 | Spring 服务端 |
| **Reactor / RxJava** | 响应式编程，背压支持 | 学习成本高，依赖大 | 流式处理场景 |
| **Netty EventLoop** | 复用 Netty 线程模型 | 需要 Netty 依赖 | 网络框架场景 |

**对于 Nacos 客户端 SDK 的场景**，当前方案其实已经是合理的选择：

1. 客户端 SDK 不能引入 Spring、Netty 等重依赖
2. 事件频率不高，单线程 + 阻塞队列足够
3. 代码简单，Java 8 兼容，无外部依赖

如果要改进，建议优先做：
1. **添加 Metrics**（队列积压、丢弃数量、处理延迟）
2. **graceful shutdown**（等待队列排空）
3. **背压预警**（水位线告警）

而不是替换整个框架。

---

## 十、关键源码索引

| 类 | 文件路径 | 行数 |
|----|----------|------|
| Event | `common/.../notify/Event.java` | 64 |
| SlowEvent | `common/.../notify/SlowEvent.java` | 32 |
| Subscriber | `common/.../notify/listener/Subscriber.java` | 74 |
| SmartSubscriber | `common/.../notify/listener/SmartSubscriber.java` | 48 |
| EventPublisher | `common/.../notify/EventPublisher.java` | 76 |
| EventPublisherFactory | `common/.../notify/EventPublisherFactory.java` | 39 |
| DefaultPublisher | `common/.../notify/DefaultPublisher.java` | ~250 |
| DefaultSharePublisher | `common/.../notify/DefaultSharePublisher.java` | 112 |
| ShardedEventPublisher | `common/.../notify/ShardedEventPublisher.java` | 46 |
| NotifyCenter | `common/.../notify/NotifyCenter.java` | 392 |

### 完整 Demo 索引

| Demo | 文件 | 说明 |
|------|------|------|
| Demo1 | `demo/Demo1BasicPubSub.java` | 最简发布订阅 |
| Demo2 | `demo/Demo2NormalVsSlowEvent.java` | 普通事件 vs 慢事件 |
| Demo3 | `demo/Demo3ScopeIsolation.java` | scope 多实例隔离 |
| Demo4 | `demo/Demo4SyncVsAsync.java` | 同步回调 vs 异步回调 |
| Demo5 | `demo/Demo5EdgeCases.java` | 队列满降级 + 过期丢弃 |
| Demo6 | `demo/Demo6LambdaSyntax.java` | Lambda 语法糖拆解 |
| Demo7 | `demo/Demo7SmartSubscriber.java` | SmartSubscriber 多事件监听 |
