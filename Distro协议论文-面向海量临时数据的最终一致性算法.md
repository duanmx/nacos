# Distro：一种面向海量临时数据的可理解最终一致性协议

> **副标题**：基于 Nacos 服务端源码（`naming` 与 `core` 模块）的形式化解读
>
> **写作约定**：本文仿照 Ongaro 与 Ousterhout 的 Raft 论文（*In Search of an Understandable Consensus Algorithm*）的论证风格——以文字推理为主干，辅以规范图与示意图，不粘贴实现源码。所有论断都以「方法名 + 源码位置链接」的形式给出可核验的证据，读者可自行对照。
>
> 版本 `3.2.1-SNAPSHOT`｜主分支 `develop`｜实现作者 `xiweng.yy`

---

## 摘要

Distro 是 Nacos 自研的、用于同步**临时服务实例数据**的最终一致性（AP）协议。与 Raft 这类基于多数派投票的强一致（CP）协议不同，它主动放弃了「线性一致」这一昂贵目标，转而针对服务发现的真实负载——数据量大、变更频繁（心跳）、可由客户端重建——做取舍。

Distro 遵循与 Raft 相同的方法论：**把一个难以整体推理的问题，分解为若干个各自朴素、可独立验证的子问题**。它的三个子问题是——责任分片（每份数据有唯一权威节点，从源头消除写冲突）、写扩散（权威节点异步复制全量数据，使各节点持有只读副本）、反熵校验（权威节点周期性以轻量指纹探测并修复副本漂移）。

本文论证：在「成员视图最终一致」且「故障最终恢复」两个前提下，三者的组合收敛到最终一致；并给出 Distro 与 Raft 在一致性模型、角色结构、复制粒度、故障恢复等维度的对比，最后讨论其固有局限。

---

## 目录

1. [引言](#1-引言)
2. [背景与动机](#2-背景与动机)
3. [设计总览](#3-设计总览)
4. [责任分片](#4-责任分片)
5. [写扩散](#5-写扩散)
6. [反熵校验](#6-反熵校验)
7. [新节点引导](#7-新节点引导)
8. [成员变更](#8-成员变更)
9. [安全性](#9-安全性)
10. [实现概要](#10-实现概要)
11. [参数与取舍](#11-参数与取舍)
12. [与 Raft 的对比](#12-与-raft-的对比)
13. [局限](#13-局限)
14. [结论](#14-结论)
15. [附录：源码索引](#15-附录源码索引)

---

## 1. 引言

一致性算法让一组机器即便部分故障也能像整体一样工作。Raft 为此选择了「强领导者 + 多数派日志复制」，目标是线性一致性。但线性一致并不免费：每一次写都要等待多数派确认，写延迟随集群规模上升，且少数派分区期间不可写。

服务发现是一个对强一致并不敏感的场景。一个中大型集群可能注册数十万实例，每个实例每隔数秒续约一次心跳，写入量极大；临时实例的数据（地址、端口、健康状态）即便短暂丢失，也能在下一次心跳或重连时由客户端自动重建；消费方也能容忍读到略微陈旧的实例列表——多注册或少注册几秒钟一个实例，通常只影响个别请求的负载均衡，而非系统正确性。在这种负载下，用 Raft 的多数派写来承载海量心跳，会让 Leader 迅速成为吞吐瓶颈。

Distro 因此而生，它以**可用性与低延迟**为首要目标。与 Raft 相同的是，它同样把「可理解性」放在设计的中心：不引入向量时钟、不做复杂的并发写冲突合并，而是把一致性拆成三个朴素的子问题，每一个都能被单独推理。本文正是沿着这条分解线索展开。

需要说明，Nacos 还有一套基于 JRaft 的 CP 协议，用于**持久实例**等关键数据。两套协议的分界线在源码中清晰存在（见第 4 节）。本文只讨论 Distro。

---

## 2. 背景与动机

### 2.1 两类数据，两套协议

Nacos 将服务实例分为两类，分别走两套协议：持久实例走 JRaft（CP，强一致），临时实例走 Distro（AP，最终一致）。前者是关键数据，宁可牺牲可用性也要强一致；后者量大、变更频繁，用最终一致换高可用与低延迟。

这条边界不是文档上的口号，而是硬编码在扩散入口的判定里。方法 [`isInvalidClient`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L137-L141) 用一行注释 “Only ephemeral data sync by Distro, persist client should sync by raft” 与对 `isEphemeral()` 的判断，明确把非临时数据挡在 Distro 之外。换言之，「谁走 Distro」这个问题在代码层面就已经回答清楚。

### 2.2 为什么临时数据适合 AP

临时数据的三个特征共同决定了 AP 是划算的取舍。其一，量大——心跳是持续、高频的写；其二，可重建——客户端断连后服务端会清除其临时数据（[`clientDisconnected`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/manager/impl/EphemeralIpPortClientManager.java#L91-L104)），重连后重新注册即可恢复；其三，弱一致可接受——短暂读到陈旧列表不破坏正确性。当数据本身就「便宜且可重建」时，为它付出强一致的代价并不理智。

---

## 3. 设计总览

### 3.1 三种机制及其分工

Distro 由三种相互补充的机制构成，可以用一句话概括它们的分工：**写扩散负责快，反熵校验负责准，快照拉取负责让新节点追平**。

- **写扩散**是正常路径，增量而实时：权威节点一旦发生本地变更，就把数据异步推送给其余所有节点。
- **反熵校验**是兜底路径，周期而补偿：写扩散可能因丢包或节点重启而失败，权威节点便周期性地用轻量指纹探测各副本，一旦发现漂移就补发全量。
- **快照拉取**是引导路径，一次性而全量：新节点加入时先从任一节点拉取全量数据，追平后才对外服务。

下图给出三者的关系（仅示意，非源码）：

```
                    ┌──── 权威节点 A ────┐
   客户端写入 ─────► │ 本地变更 → 事件     │
                    │ 判定「我负责」→扩散 │
                    └─────────┬──────────┘
                              │ ① 写扩散（异步、全量副本）
                 ┌────────────┼────────────┐
                 ▼            ▼             ▼
              节点 B        节点 C        节点 D     （持只读副本，可对外读/推送）

   A 周期任务 ── ② 反熵校验（发 revision 指纹）──► B/C/D 比对；不一致则请 A 补发全量
   新节点 E  ── ③ 快照拉取（一次性灌入全量）───► E 追平后置就绪
```

### 3.2 协议的浓缩规范

仿照 Raft 论文用一张「Figure 2」浓缩整个算法，这里也给出 Distro 的规范式概述。它由「每个节点持有的状态」与「三种机制的触发条件和动作」两部分构成：

**节点状态**
- `healthyList`：全局排序一致的存活节点列表，用于责任判定。
- 每个 Client 的数据及其 `revision`（内容指纹，一个整数）。
- `isFinishInitial`：本节点是否已完成初始全量加载。

**机制一·写扩散**（事件触发）
- 触发：本地 Client 发生变更或断连事件。
- 前置：本节点是该 Client 的权威节点，且数据为临时。
- 动作：将该 Client 的全量数据异步发往其余所有节点；失败进入延迟重试。

**机制二·反熵校验**（周期触发，默认 5 秒）
- 前置：本节点已完成初始加载。
- 动作：对本节点负责的每个 Client，向其余节点发送 `⟨clientId, revision⟩` 指纹；对端比对本地副本指纹，不一致则通知权威节点定向补发全量。

**机制三·快照拉取**（启动时一次）
- 动作：从任一其他节点获取全量快照并逐条落地；成功后置 `isFinishInitial`，节点方可对外服务并参与校验。

### 3.3 框架层与业务层

Distro 在工程上分为两层：位于 `core` 模块的**框架层**（`com.alibaba.nacos.core.distributed.distro`）提供与业务无关的协议骨架——任务调度、三条流水线、失败重试、超时控制；位于 `naming` 模块的**业务层**（`...ephemeral.distro.v2`）实现框架定义的接口，把 Client 数据接入协议。业务组件在启动时由 [`DistroClientComponentRegistry.doRegister`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientComponentRegistry.java#L66-L79) 注册进框架，并以一个类型标识 `TYPE = "Nacos:Naming:v2:ClientData"` 作为路由键。框架收到数据后依据该键找到对应处理器（[`DistroProtocol.onReceive`](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/DistroProtocol.java#L150-L161)）。这种分层让框架本身可复用于其他数据类型，而业务侧只需提供四个组件。

业务侧的核心是一个身兼三职的类 [`DistroClientDataProcessor`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L58-L61)：作为事件订阅者，它监听本地变更以触发写扩散；作为数据存储，它向框架提供本节点的数据、快照与校验指纹；作为数据处理器，它处理来自其他节点的数据、快照与校验请求。正是这三重身份，使它成为「naming 数据模型」与「Distro 框架」之间的唯一桥梁。

---

## 4. 责任分片

> 核心命题：一致性哈希让每份数据只有一个权威节点，从源头消除写冲突。

Distro 的一切都建立在「责任判定」之上——即回答「哪些数据该由本节点主动同步出去」。判定入口 [`isInvalidClient`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L137-L141) 给出三个「无效」条件：数据为空、数据非临时（应走 Raft）、或本节点并不负责它。只有全部通过，本节点才会主动扩散这份数据。

「本节点是否负责」最终由 [`DistroMapper.responsible`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/DistroMapper.java#L78-L99) 判定，其思路是朴素的一致性哈希：先对数据的责任标签（v2 中是 Client 的 `ip:port`）取哈希，再对存活节点数取模得到一个目标下标；若该下标落在本节点在列表中的位置区间内，则本节点负责。单机模式或哈希未就绪等边界会短路为「负责」或「暂不负责」。哈希函数本身极为简单（对 `hashCode` 取绝对值后取模，见 [L125-L127](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/DistroMapper.java#L125-L127)），不追求密码学强度，只求把数据大致均匀地摊到各节点。

这套判定要正确，依赖一个关键不变式：**所有节点必须对节点列表持有完全一致且顺序相同的视图**，否则同一份数据会被多个节点同时认领。源码用两处保证这一点：成员变更时对列表执行 `Collections.sort` 排序（[`DistroMapper.onEvent`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/DistroMapper.java#L129-L142)），并在字段注释中明确要求「所有节点的列表顺序必须一致」。这个不变式是全篇安全性论证的基石（第 9 节）。

责任制的意义在于：每份数据只有一个权威源，写扩散与反熵校验都由它单向发起，其他节点只被动接收——这与 Raft 用单一 Leader 避免冲突异曲同工。区别在于，Raft 是**集群级**的单 Leader，而 Distro 是**数据分片级**的多权威：每个数据分片各有自己的权威节点，因而没有全局选举，也没有单点写入瓶颈。

---

## 5. 写扩散

> 核心命题：权威节点把本地变更异步复制到所有节点，写路径立即返回、不被同步阻塞。

写扩散由事件驱动。数据模型层在 `addServiceInstance` / `removeServiceInstance` 后会发出客户端变更或断连事件，而 `DistroClientDataProcessor` 作为订阅者监听这些事件（[`subscribeTypes`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L92-L99)）——**模型层一改，Distro 立刻感知**。事件回调 [`onEvent`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L101-L111) 首先为单机模式短路（没有其他节点，直接返回），随后分两路：普通变更走广播扩散，而「校验失败」事件走定向补发（后者见第 6 节）。

广播路径 [`syncToAllServer`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L123-L135) 先用第 4 节的责任判定过滤——**只有权威节点才扩散**——再以 `clientId` 为资源键，把变更或删除操作交给框架层的 [`DistroProtocol.sync`](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/DistroProtocol.java#L106-L121)。关键在于，`sync` 只是把任务投递出去，**本身立即返回，不阻塞业务写路径**，这正是 Distro 写延迟低的根本原因。

框架层随后经过三个阶段。首先是**延迟合并**：任务被投入延迟队列（默认延迟 1 秒），短时间内对同一键的多次变更会被合并为一次扩散，抵消心跳抖动。其次是**现取现打包**：延迟到期后转为执行任务（[`DistroSyncChangeTask`](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/task/execute/DistroSyncChangeTask.java#L44-L54)），执行时才去读取该 Client 的**当前**数据，因此即便多次变更被合并，扩散出去的也一定是最终态而非中间态。最后是**发送**：传输代理在发送前检查目标节点是否健康（存在、状态为 UP、gRPC 通道可用，见 [`checkTargetServerStatusUnhealthy`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientTransportAgent.java#L226-L229)），健康则通过集群内 RPC 送出。

若某次发送失败，任务经由失败处理器 [`DistroClientTaskFailedHandler.retry`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientTaskFailedHandler.java#L39-L44) 重新入队一个延迟任务（默认 3 秒后重试）。这里有一个必须澄清的要点：**重试既无次数上限，也不保证成功**。因此写扩散本身并不是最终一致的保证——它只负责「大多数情况下的快速收敛」，真正的兜底是下一节的反熵校验。

接收侧同样值得关注。数据到达后由框架路由到 [`processData`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L148-L166)：变更类操作交给 `handlerClientSyncData` 落地，删除类操作直接移除对应 Client。落地时会先创建一个「非本机直连」的同步客户端，以标记「这是从别处复制来的副本」，随后进入增量对齐 [`upgradeClient`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L179-L214)。

`upgradeClient` 用一次 diff 把本地副本对齐到同步数据，逻辑有三个要点。第一，**只处理真正变化的**：仅当实例信息与本地不相等时才更新，相同则跳过，避免无意义的事件风暴。第二，**发的是业务事件**：新增会发布客户端注册事件，从而触发本节点的倒排索引更新与订阅推送——这意味着即便是复制来的数据，也会让本节点照常通知它的订阅者。正因如此，AP 副本是「可用的只读副本」，而非冷备份，任何节点都能对外提供读与推送。第三，**直接对齐指纹**：把本地副本的 `revision` 设为源节点的值（而不是自行重算），使副本与源保持相同指纹，供下一轮反熵校验比对。

---

## 6. 反熵校验

> 核心命题：写扩散可能丢消息，权威节点便周期性地用轻量指纹探测漂移，只在不一致时才补发全量。原则是「校验轻、修复重」。

「反熵」（anti-entropy）一词借自 Dynamo 等系统。熵在这里比喻「副本随时间发散的无序程度」——消息丢失、节点重启都会让各副本悄悄漂移；反熵机制就是周期性地对抗这种发散，把系统重新拉回一致。既然第 5 节已说明写扩散的重试不保证成功，那么当某次扩散彻底失败时，副本就会漂移，反熵校验正是为此兜底。

校验之所以「轻」，在于它传输的不是完整数据，而只是一个指纹。权威节点为它负责的每个 Client 生成一份 `⟨clientId, revision⟩` 校验数据（[`getVerifyData`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L297-L319)），其中 `revision` 是对数据内容的哈希——用一个整数就代表了「这份数据现在长什么样」。这些指纹由一个周期任务（默认每 5 秒，见 [`DistroProtocol.startVerifyTask`](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/DistroProtocol.java#L89-L94)）发往其余节点。这里有一个前置守卫：**尚未完成初始加载的节点不发校验**（[`verifyForDataStorage`](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/task/verify/DistroVerifyTimedTask.java#L66-L86)）——自己都没追平，自然无资格校验别人。

接收节点收到指纹后做比对（[`verifyClient`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/manager/impl/EphemeralIpPortClientManager.java#L130-L146)）：若本地存在该 Client 且指纹相等（或对端 `revision` 为 0，这是对旧版本的兼容），则校验通过；若本地缺失或指纹不一致，则失败。这里有一个精妙的副作用——**校验通过时会顺带刷新该副本的心跳续约**，从而避免副本被本节点的过期清理任务误删。也就是说，反熵校验同时承担了「探测漂移」与「维持副本存活」两件事。

校验一旦失败，就进入修复闭环。发起方在回调中发布一个「校验失败」事件（[`DistroClientTransportAgent` 中的回调](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientTransportAgent.java#L302-L315)），该事件被权威节点自己的 `onEvent` 捕获，转入定向补发路径 [`syncToVerifyFailedServer`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L113-L121)——**只向那个漂移的节点**立即补发全量数据（延迟置零，不再合并）。至此闭环完成：探测到漂移、通知权威节点、定向补发、副本追平。`revision` 的价值也在此兑现——用一个整数就判断了一致性，只有真的不一致时才付出传输全量的代价。这正是「校验轻、修复重」的含义：绝大多数时候数据本就一致，校验只花费极小代价；只有少数漂移才触发昂贵的全量修复。

一个便于理解的类比是对账：不必逐笔核对完整明细，平时只核对「余额」（指纹），余额对得上就认为无误，对不上才翻出完整账本（补发全量）。

---

## 7. 新节点引导

> 核心命题：新节点启动时一次性拉取全量快照，追平后才对外服务。

新加入的节点没有任何数据，无法靠增量的写扩散在合理时间内追平，因此需要一次全量引导。它会向任一其他节点请求全量快照。提供方 [`getDatumSnapshot`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L281-L295) 把本节点持有的**所有**临时 Client 打包成一个大快照——注意它不做责任过滤，因为新节点需要的是全量视图，而非某个分片；这也说明每个节点都持有全量副本，因而向任意一个节点请求都能拿到完整数据。

拉取由启动时的加载任务驱动（[`DistroLoadDataTask`](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/task/load/DistroLoadDataTask.java#L61-L128)）。它先自旋等待成员列表与数据存储就绪，然后**依次尝试**每个其他节点，只要有一个成功返回快照即可停止。快照落地时逐条复用第 5 节的同步处理逻辑。加载成功后，节点置位一个「就绪门闩」`isFinishInitial`（[L82-L85](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java#L82-L85)）；若失败则按默认 30 秒的间隔重试整个流程。

这个门闩有两重意义：其一，它使 `DistroProtocol` 标记为「已初始化、可对外服务」；其二，如第 6 节所述，未就绪的节点不参与反熵校验。二者共同确保：**一个尚未追平的节点，既不会用陈旧数据误导读请求，也不会用陈旧指纹去误判其他节点**。

---

## 8. 成员变更

Raft 用「联合共识」来安全地变更成员。Distro 的成员变更处理要简单得多，因为它不追求线性一致，只需保证责任划分最终收敛。

当集群成员变化时，成员管理器发布成员变更事件，[`DistroMapper.onEvent`](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/DistroMapper.java#L129-L142) 据此更新本地节点列表：只纳入 UP 与 SUSPICIOUS 状态的节点，并**排序**以保证各节点视图一致（第 4 节的不变式）。

节点增减会改变节点总数，从而改变哈希取模的结果——部分数据的权威归属随之转移。这个转移是自愈的，无需任何显式的数据搬迁指令。成为新权威的节点，本就在此前的写扩散中收到过这些数据的副本，此刻只需让自己的周期校验自然覆盖它们即可；而卸任的旧权威则停止对这些数据发起校验，副本保留到过期清理或被覆盖为止。一致性哈希的性质保证：节点增减时只有少量数据的归属发生变化，扰动被降到最低。

变更传播存在延迟，各节点的列表视图可能短暂不一致，由此产生两种窗口。一种是「短暂双责任」：两个节点同时认为自己负责某数据，都发起校验或扩散——结果只是数据被重复维护，由于接收侧的 diff 对相同数据无操作，**不破坏正确性**。另一种是「短暂无责任」：暂时没有节点认领某数据，它只靠已有副本提供读，直到视图收敛后被重新接管。在最终一致模型下这两种窗口都可接受——这正是 Distro 相比 Raft 大幅简化成员变更的根本原因。

---

## 9. 安全性

本节论证：在「成员视图最终一致」与「故障最终恢复」两个前提下，Distro 保证最终一致，即所有节点最终收敛到相同的临时数据视图。

论证依赖两条性质。**责任唯一性**：当各节点的节点列表一致时，任一数据键的哈希取模是确定值，恰好落入唯一一个节点的位置区间，因而有且仅有一个权威节点——依据是 `responsible` 的区间判定与 `onEvent` 的排序。**副本可收敛性**：接收侧的 `upgradeClient` 是幂等的 diff 对齐，对相同数据无操作，对不同数据对齐到源节点状态并复制其指纹——依据是「仅当不相等才更新」的守卫。

在此基础上，考察任一临时数据 `d`（其权威节点为 `R`）的收敛过程。正常情况下，`R` 上的变更经写扩散推送到所有节点并被 diff 对齐，一次扩散内即收敛。若某次扩散因丢包或节点重启而失败（且重试也失败），某副本便与 `d` 漂移；但 `R` 的周期校验会发送 `d` 的指纹，漂移节点比对失败后触发「校验失败」事件，`R` 随即定向补发全量，漂移被修复——**只要 `R` 存活且网络最终恢复，漂移必被修复**。对于新加入的节点，它在追平（`isFinishInitial`）之前既不对外服务也不参与校验，因而不会用陈旧数据污染集群。最后，由责任唯一性，`d` 的写扩散与校验只由 `R` 单向发起，不存在两个节点对 `d` 做相互矛盾的权威写，收敛目标唯一。综合以上，`d` 最终在全网收敛。

若干边界情形也被源码显式处理：权威节点宕机时，责任经一致性哈希转移给新节点，新权威从自己已有的副本继续维护；目标节点不健康时，扩散与校验前置的健康检查会跳过它，留待后续周期；旧版本节点以 `revision` 为 0 参与校验时被直接放行以兼容；单机模式下 `onEvent` 直接返回、不做任何扩散；副本则因校验通过时的顺带续约而不会被过期清理误删。

同样重要的是明确 Distro **不保证**什么，以免误用：它不保证线性一致（读可能读到扩散或校验窗口内的陈旧数据）；不保证写立即可见（`sync` 异步，默认有 1 秒延迟外加网络传播）；不保证跨键原子性（无事务，各键独立扩散）；也不保证在持续网络分区下收敛（收敛以「网络最终恢复」为前提）。这些正是「以 AP 换性能」的代价，对临时服务数据而言可以接受。

---

## 10. 实现概要

在工程结构上，框架层（`core`）提供协议入口 `DistroProtocol` 与三条流水线的调度器、组件注册表、动态配置；业务层（`naming`）提供数据处理器、gRPC 传输代理、失败重试处理器与一致性哈希映射器。二者通过类型标识 `TYPE` 松耦合，框架据此在多种数据类型间路由。

三条流水线在线程模型上彼此解耦：写扩散由事件驱动，经通知中心的事件线程进入延迟引擎再到执行引擎，全程不阻塞业务写路径；反熵校验由定时器驱动（默认 5 秒），进入执行引擎；快照加载在启动时执行一次，失败按 30 秒重试，且在就绪门闩置位前不对外服务。写扩散与反熵校验共用同一套执行引擎，并按数据键分片串行执行，从而避免同一键上的并发。

整条链路的幂等性由三处协同保证：延迟引擎按键去重合并抖动；执行任务「现取现打包」，确保合并后扩散的是最终态；接收侧的 diff 对相同数据不产生任何副作用。这三点共同确保「多发一次、乱序到达、重复到达」都不会破坏正确性。

---

## 11. 参数与取舍

以下参数全部取自 [`DistroConstants`](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/DistroConstants.java) 的默认值，并支持通过 [`DistroConfig`](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/DistroConfig.java) 运行时动态刷新：

| 配置项 | 默认值 | 作用 |
|--------|--------|------|
| 写扩散延迟 `data.sync.delayMs` | 1000 ms | 合并抖动，短时多次变更并为一次扩散 |
| 扩散 RPC 超时 `data.sync.timeoutMs` | 3000 ms | 单次扩散请求的超时 |
| 扩散重试延迟 `data.sync.retryDelayMs` | 3000 ms | 扩散失败后再次入队的延迟 |
| 校验周期 `data.verify.intervalMs` | 5000 ms | 反熵校验的触发间隔 |
| 校验 RPC 超时 `data.verify.timeoutMs` | 3000 ms | 单次校验请求的超时 |
| 加载重试延迟 `data.load.retryDelayMs` | 30000 ms | 快照加载失败后重试整个流程的延迟 |
| 加载 RPC 超时 `data.load.timeoutMs` | 30000 ms | 拉取全量快照的超时 |

两组数量级对比揭示了设计意图。其一，写扩散延迟（1 秒）与校验周期（5 秒）拉开档次——写扩散是「快通道」，承担绝大多数一致性收敛；校验是「慢通道」，只做低频补偿。其二，快照超时（30 秒）远大于扩散超时（3 秒）——快照是全量大数据，需要更长的容忍；单个 Client 的扩散是小数据，宜快速失败、快速重试。

---

## 12. 与 Raft 的对比

| 维度 | Raft | Distro |
|------|------|--------|
| 一致性模型 | 线性一致（CP） | 最终一致（AP） |
| 目标场景 | 关键元数据，需强一致 | 海量临时实例、心跳、可重建 |
| 角色结构 | 集群级单一 Leader | 数据分片级多权威（一致性哈希） |
| 写路径 | 追加日志→多数派确认→提交 | 权威节点异步扩散→立即返回 |
| 写延迟 | 正比于多数派往返 | 正比于本地处理（扩散异步） |
| 冲突避免 | 单 Leader + 任期 + 日志匹配 | 责任唯一性（哈希分片） |
| 复制粒度 | 日志条目（增量、有序） | 全量数据（覆盖式） |
| 一致性校验 | 日志索引 + 任期 | revision 指纹（内容哈希） |
| 故障恢复 | 选举新 Leader + 日志补齐 | 责任重分配 + 反熵补发 |
| 成员变更 | 联合共识 | 更新有序列表 + 哈希自愈 |
| 新节点引导 | Leader 发快照 + 日志 | 从任一节点拉全量快照 |
| 分区期间可写性 | 仅多数派侧可写 | 各侧均可写（牺牲一致性） |

两者共享同一种设计哲学——以可理解性为纲，把复杂问题分解为独立子问题。Raft 分为领导选举、日志复制与安全性；Distro 分为责任分片、写扩散与反熵校验。差异全部源于目标一致性等级的不同：因为放弃了线性一致，Distro 得以用一致性哈希替代选举、用覆盖式全量替代有序日志、用指纹校验替代日志匹配，整体实现远比 Raft 轻量。

若要找一个更贴近的参照，Distro 更像 Amazon Dynamo：一致性哈希做分片、反熵做修复、最终一致做目标。不同的是，Dynamo 需要处理并发写冲突（如购物车合并），而服务发现数据有天然的「唯一权威源」，因此 Distro 省去了冲突合并；它的 `revision` 指纹相当于 Dynamo 的 Merkle 树，只是粒度更粗——每个 Client 一个整数。

---

## 13. 局限

Distro 的取舍带来了几处必须正视的局限。**读陈旧性**：在扩散延迟与网络传播窗口内，非权威节点可能返回略旧的实例列表；这对服务发现可接受，但对分布式锁、配置等强一致场景并不适用，Nacos 因此为它们保留了 Raft。**反熵的规模成本**：校验虽轻，但周期性地覆盖「全部负责数据 × 全部其他节点」，在超大规模集群下其 RPC 数量随两者乘积增长，仍有可观的固定开销。**哈希倾斜**：责任哈希基于地址字符串的 `hashCode`，理论上分布可能不均，源码中未见虚拟节点一类的均衡措施。**成员视图窗口**：变更传播期内的短暂双责任或无责任虽不破坏最终一致，却会带来短时的冗余流量或读空窗。**重试无保证**：写扩散重试既无上限也不保证成功，真正的兜底始终是反熵——理解这一点，对排查「数据短暂不一致」类问题至关重要。

---

## 14. 结论

Distro 是一个为服务发现量身定制的最终一致性协议。它与 Raft 共享「可理解性优先、问题分解」的方法论，但因目标从 CP 降到 AP，得以用三个极简子问题——责任分片、写扩散、反熵校验——替代 Raft 的选举与日志复制。责任分片以一致性哈希赋予每份数据唯一权威，从源头消除冲突；写扩散以异步复制换取低写延迟，并让每个副本都「可读可推送」而非冷备；反熵校验以「校验轻、修复重」的指纹机制兜底最终一致；成员变更则靠有序列表与哈希自愈，免去了 Raft 式的联合共识。

Distro 的价值不在于它比 Raft「更先进」，而在于它示范了一个朴素而重要的工程判断：**当业务能够容忍最终一致时，主动放弃线性一致，可以换来数量级的实现简化与性能提升**。用正确的一致性模型去解决正确的问题，本身就是一种优雅。

---

## 15. 附录：源码索引

**业务层（`naming` 模块）**

| 文件 | 职责 | 关键位置 |
|------|------|----------|
| [DistroClientDataProcessor.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientDataProcessor.java) | 三重身份的核心处理器 | 类声明 L58-61；onEvent L101-111；syncToVerifyFailedServer L113-121；syncToAllServer L123-135；isInvalidClient L137-141；processData L148-166；upgradeClient L179-214；processVerifyData L248-258；getDatumSnapshot L281-295；getVerifyData L297-319 |
| [DistroClientTransportAgent.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientTransportAgent.java) | gRPC 节点间传输 | syncData L66-112；健康检查 L226-229；校验失败回调 L302-315 |
| [DistroClientTaskFailedHandler.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientTaskFailedHandler.java) | 失败重试 | retry L39-44 |
| [DistroClientComponentRegistry.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/consistency/ephemeral/distro/v2/DistroClientComponentRegistry.java) | 组件注册 | doRegister L66-79 |
| [DistroMapper.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/DistroMapper.java) | 一致性哈希责任映射 | responsible L78-99；distroHash L125-127；onEvent L129-142 |
| [EphemeralIpPortClientManager.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/manager/impl/EphemeralIpPortClientManager.java) | 临时客户端管理 | isResponsibleClient L122-127；verifyClient L130-146 |
| [AbstractClient.java](file:///Users/mmhm/IdeaProjects/nacos/naming/src/main/java/com/alibaba/nacos/naming/core/v2/client/AbstractClient.java) | 数据打包与指纹 | generateSyncData L141-173；recalculateRevision L203-207 |

**框架层（`core` 模块）**

| 文件 | 职责 | 关键位置 |
|------|------|----------|
| [DistroProtocol.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/DistroProtocol.java) | 协议入口 | sync L106-121；onReceive L150-161；startVerifyTask L89-94；startLoadTask L71-87 |
| [DistroLoadDataTask.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/task/load/DistroLoadDataTask.java) | 快照加载 | run L61-75；loadAllDataSnapshotFromRemote L93-128 |
| [DistroVerifyTimedTask.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/task/verify/DistroVerifyTimedTask.java) | 周期校验调度 | verifyForDataStorage L66-86 |
| [DistroSyncChangeTask.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/task/execute/DistroSyncChangeTask.java) | 变更扩散执行 | doExecute L44-54 |
| [DistroConstants.java](file:///Users/mmhm/IdeaProjects/nacos/core/src/main/java/com/alibaba/nacos/core/distributed/distro/DistroConstants.java) | 默认参数 | 全文 |

---

> **相关阅读**：本文是对 [Naming服务端-06-Distro一致性协议.md](./Naming服务端-06-Distro一致性协议.md) 的论文化重写，聚焦形式化论证与 Raft 对比；若需按数据流转顺序理解具体调用，建议对照原篇。
