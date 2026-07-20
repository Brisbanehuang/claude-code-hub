# 供应商竞速方案架构评审结论

**日期**: 2026-07-20  
**范围**: 剩余两个业务问题 + 整体业务闭环 + 基于现有 Claude Code Hub（CCH）实现的可行性  
**结论性质**: 架构/产品决策建议，**不编码**  
**修订**: 按架构审阅反馈修正：双交付路径拆分、超 cap 处理、最后一轮定义、504 breaking 标注、末轮 in-flight 等待边界

---

## 1. 现状基线（与目标方案的差距）

当前 CCH 的 streaming hedge（`ProxyForwarder.sendStreamingWithHedge`）是**串行触发式竞速**，不是目标方案里的「固定每轮 N 家 + 多轮 Discovery + 最终兜底池」：

| 能力 | 现有实现 | 目标方案 |
|------|----------|----------|
| 触发 | 初始 Provider 的 `firstByteTimeoutStreamingMs` 到期后 `launchAlternative` 再起一家 | 每轮固定选 N 家并发 |
| 赢家 | **任意非空首字节** `readFirstReadableChunk` 即 `commitWinner`，立刻 `abortAllAttempts` | **有效首字**（协议语义）才赢；旧轮继续跑并入兜底池 |
| 输家 | 可 cancel，或在系统开关 `billHedgeLosers` 开启时由 `startLoserBilling` → `finalizeHedgeLoserBilling` 后台 drain 计费；**不参与再比较** | 旧结果暂存，最后一轮按首字耗时与完整可交付性兜底 |
| Sticky | 赢家 `SessionManager.updateSessionBindingSmart` 改绑；超时语义绑在 Provider 的 first-byte timeout 上 | Sticky 独立 SLA；超时后 Sticky Discovery（SLA=Sticky SLA），原 Sticky 只进最终兜底 |
| Fake 200 | **流结束后** `detectUpstreamErrorFromSseOrJsonText` / `response-validator`；已透传则无法改 HTTP 状态 | 假成功**不得**进入赢家/兜底结果池 |
| 终态失败 | `resolveHedgeTerminalError` → 客户端不可重试错误原样抛；其余 **503 +「所有供应商暂时不可用…」** | 见下文业务问题 B |

关键落点：

- 竞速主路径：`src/app/v1/_lib/proxy/forwarder.ts`（`sendStreamingWithHedge` / `commitWinner` / `buildBufferedFirstChunkStream`）
- Sticky：`src/lib/session-manager.ts`（`updateSessionBindingSmart` 等）
- Fake 200：`src/lib/utils/upstream-error-detection.ts`、流结束 `response-handler`、`fake-streaming/response-validator.ts`
- 已有「首块补回再续读」：`buildBufferedFirstChunkStream` —— 是**活连接续传**，不是完整响应缓存回放

---

## 2. 业务问题 A：暂存结果的交付方式

### 2.1 问题陈述

旧轮请求不参加后续普通竞赛，但继续产生结果并参与最终兜底。若最终选用**旧轮**结果，客户端尚未收到任何字节时，应如何交付？

可选语义：

1. **完整可重放（full snapshot）**：仅当该 attempt 已自然结束且校验为可交付，才把整段 body（SSE 全事件 / 非流式 JSON）按原样写回客户端。  
2. **边收边缓存、从首字起回放（prefix buffer + live tail）**：持续缓存已收字节；选中时先回放缓存前缀，再接上仍存活的上游 reader 续传。  
3. **仅完整结果可兜底（strict）**：没有完整可交付快照就**不能**用该 attempt 兜底，只能等仍在跑的候选或终态失败。

### 2.2 推荐结论（默认采用 1 + 3 的组合，不以 2 作为默认）

**推荐：旧轮/兜底交付必须「完整可重放 + 已通过可交付校验」；不允许「半截前缀回放」作为默认业务语义。**

这只约束 **快照池兜底路径**，**不**约束当轮活流赢家路径（见 §2.3 双路径拆分）。

理由（与 CCH 现状对齐）：

1. **流式协议正确性**  
   Anthropic / OpenAI SSE 依赖事件完整序列（`message_start` → deltas → `message_delta` usage → `message_stop` 等）。半截前缀 + 事后切流极易出现：  
   - 重复/缺失 `message_start`  
   - usage 落在已丢弃或未转发的 chunk  
   - 客户端已按「流结束」解析，后续又接到字节  

2. **Fake 200 / 可交付判定时机**  
   现有检测强依赖**流结束或足够完整的 body**（`detectUpstreamErrorFromSseOrJsonText`、`validateUpstreamResponse` 的 `stream_no_deliverable` / `stream_error_event` 等）。  
   「只有首字 + 未结束」时**无法**可靠证明不是 fake 200、也不是空可交付。目标规则已写明：HTTP 200 或单个首字不能提前结束。  
   → 兜底池元素的准入条件应是：**有效首字时刻 T_first + 完整结束 + 校验 ok**，比较键主要是 **T_first**（最后一轮与旧最佳比的是首字耗时，不是总耗时）。

3. **实现复杂度与资源**  
   方案 2 需要每 attempt：tee/缓冲上限、内存 cap、中途 abort 一致性、与输家计费 drain（`startLoserBilling` / `finalizeHedgeLoserBilling`）对 **同一 `attempt.reader` 的互斥**、客户端 abort 时多路清理。现有 `buildBufferedFirstChunkStream` 只解决「赢家已选定后的首块补回」，**不能**直接扩展成多候选长时间缓存回放。  
   方案 1 可复用「完整 body 已在内存/有界 buffer」模式（response-handler 已有 head/tail 文本累积思路），或对兜底候选在后台 drain 到有界 buffer，仅 `ok` 的进入 pool。注意：`billHedgeLosers` 只是系统配置布尔开关（DB 列），真正执行 drain/计费的是上述两个函数。

4. **与已确认规则的一致性**  
   - 「A 有完整结果、首字 20s；E/F 首字都 >20s → 可用 A」—— 明确要求 **完整可交付**。  
   - 「E/F 首字都慢，但没有完整可交付 → 继续等」—— 否定「仅有首字即可交付」。  
   - 因此 **strict 完整可重放** 是对上述两条的直接 entailing，而不是额外产品偏好。

### 2.3 双交付路径（必须拆开，禁止混用）

成功结束请求时，赢家只可能来自下列两条路径之一。实现与文档均不得把「末轮活流赢家」误写成「快照整包回放」。

| 维度 | 路径 L：**活流赢家**（Live winner） | 路径 S：**快照兜底赢家**（Snapshot fallback winner） |
|------|--------------------------------------|-----------------------------------------------------|
| **何时出现** | **普通轮**（含 Sticky 单飞）在轮 SLA 内产生有效首字并当场夺冠；**或最后一轮**当轮候选有效首字耗时 `T_first ≤` 池中最佳完整结果的 `T_first`（池空时：首个有效首字的当轮候选亦可夺冠，见 §4.1） | 决胜结束时，**没有任何活流赢家**，且快照池非空：按池内 `T_first` 最优（并列规则可再定）选出旧 attempt |
| **谁** | **当前仍持有上游 reader 的当轮（或 Sticky）attempt** | **已完整结束并校验 ok 的旧轮 / pool-only attempt**（含 Sticky 超时后仅入池的原 Sticky） |
| **对客户端 body** | **活连接续传**：已读首块用 `buildBufferedFirstChunkStream` 类方式补回，其后继续 `reader.read()` 透传；**禁止**先 drain 完整再回放 | **整包回放**：新建 `Response`，body 为已缓冲完整字节的固定 ReadableStream（可分 chunk 推送，内容不再读上游） |
| **延迟特征** | 客户端在有效首字确认后立即开始收流 | 客户端在快照入池完成后才开始收流（内容已齐，无上游尾延迟） |
| **Sticky** | 赢家 Provider 绑定 / 续绑 Sticky（`updateSessionBindingSmart`） | 同样允许成为 Sticky（已确认规则） |
| **计费** | 该 attempt 走**赢家**计费 + 流式 finalization；其余 attempt 输家 cancel 或 `hedge_loser_billed` | 被选中的快照 attempt 走**赢家**计费；不得因「曾进池」对其他快照再计赢家；未选中快照可按输家/已计费策略处理 |
| **Fake 200** | 有效首字门闸 + 流中/流后检测；一旦判定 fake 则该 attempt **不能**保持赢家身份（若已开始透传则按现网「已转发假成功」约束处理侧效应，清 Sticky 等） | **入池前强制完整可交付校验**；fake 200 **不得入池** |

**关键禁令**：最后一轮当轮 Provider 因 `T_first` 胜出时，走路径 L，**不得**为「统一实现」改成路径 S（否则多等一整段生成时间 + 无谓内存）。

### 2.4 快照路径交付形态（仅路径 S）

当最终赢家是「兜底池中的 attempt R」时：

1. 对客户端：新建 `Response`，`status/headers` 取 R 的上游成功响应快照；body 为 **已缓冲的完整字节序列** 的一次性 ReadableStream。  
2. Sticky：按已确认规则绑定到 R。  
3. 计费：R 按赢家路径；其他 attempt 走输家策略，**不得**双计赢家。  
4. **内存 cap（快照缓冲）**  
   - 每 attempt 缓冲上限可配置。  
   - **超 cap → 该 attempt 立即失去兜底资格，记链路 reason（如 `fallback_buffer_cap_exceeded`），并直接 cancel 上游连接**（若系统开关 `billHedgeLosers` 开启，可在 cap 前已读字节上尽力走 `finalizeHedgeLoserBilling` 抽 usage，但**不再**为回放保留连接）。  
   - **不存在**「超 cap 后仍可作为普通轮活流当场赢」的路径：进入快照缓冲的前提是**有效首字已产生且该 attempt 已错过/退出当场夺冠窗口**（已被更快赢家提交，或本轮 SLA 已过进入后台）。此时再谈「Discovery 窗内活流赢」在时序上不可达。  
   - 实现者不得保留悬空「半缓冲半活流」状态。  

5. **硬不变量：单 attempt 的 body/`reader` 独占消费（调度器必须强制）**  
   - 现网 `forwarder.ts` 中输家路径注释写明：实际后台 drain 由 `runAttempt` 流程发起，**「它独占 reader，避免并发读」**；入口为 `startLoserBilling`，落账为 `finalizeHedgeLoserBilling`。  
   - 一个 attempt 的响应 body **只能被消费一次**。因此 **路径 S 快照缓冲 drain** 与 **输家计费 drain（`startLoserBilling`）** 对同一 attempt **互斥**，不得两路并发 `read`。  
   - **决策时刻**：该 attempt **退出夺冠窗口**（普通轮已有路径 L 赢家 / 轮 SLA 到期转后台 / 末轮比较器判定其不能再走路径 L）的瞬间，调度器必须二选一打标，例如：  
     - `consumeMode: "snapshot_pool"` — 仅为路径 S 缓冲；校验 ok 入池；失败或超 cap 则 cancel（可选：缓冲过程中顺带抽 usage，但仍是**同一条** drain，不是第二条 reader）；  
     - `consumeMode: "loser_billing"` — 仅 `startLoserBilling` → `finalizeHedgeLoserBilling`，**不**进快照池；  
     - `consumeMode: "cancel"` — 直接取消，不计费不入池。  
   - **禁止**：先 `startLoserBilling` 再对同一 reader 做快照回放；或 tee 成两条逻辑读而不在文档/实现中显式承担双倍内存与一致性成本（首期不做 tee）。

### 2.5 明确不推荐作为默认

- **从首字开始的实时回放半截流（路径 2）**：仅可作为后续优化，首期不做。  
- **把未校验的 firstChunk 当可交付**：与 fake 200 边界冲突。  
- **把路径 L 做成整包回放**：延迟与内存双惩罚。

### 2.6 业务问题 A 一句话决策

> **旧轮/pool-only 暂存 = 后台独占 drain 至完整 + 校验通过后的只读快照；快照兜底（路径 S）只从池中按 T_first 选优并整包回放。当轮/Sticky 夺冠（路径 L）始终活连接续传。无完整可交付快照则不能用该旧 Provider 结束请求。超 cap：丢兜底资格并 cancel，不留活流悬空。同一 attempt 的路径 S 缓冲与 `startLoserBilling` 互斥，退出夺冠窗时选定唯一 `consumeMode`。**

---

## 3. 业务问题 B：全部失败 / 无可交付结果时的最终失败语义

### 3.1 触发条件（应统一走同一终态）

在整体请求硬超时（若启用）或「候选耗尽且 in-flight 全部 settled」时，若：

- 没有任何 attempt 产生 **有效首字且完整可交付** 的快照，且  
- 当前也没有仍可等待的 in-flight 候选，且  
- 没有任何路径 L 赢家已提交，  

则进入 **Terminal Failure**。

（快照池非空时，即使最后一轮「竞赛」未产生活流赢家，也应走路径 S 成功，而不是失败。）

### 3.2 推荐结论：分层错误；默认保持 503 对外契约

与现有 `resolveHedgeTerminalError` 对齐：

| 优先级 | 条件 | 对客户端 | 对内链/熔断/Sticky |
|--------|------|----------|-------------------|
| P0 | 客户端 abort | **499**（或现有 CLIENT_ABORT 映射） | 不记 Provider 熔断；不写 Sticky |
| P1 | 存在 **NON_RETRYABLE_CLIENT_ERROR** | **原样透传** | 按现有 client_error 链 |
| P2 | 全部为 Provider/基础设施失败、空响应、fake 200、轮次/候选超时、无候选 | **503** + `所有供应商暂时不可用，请稍后重试` | 记失败链；**不**建成功 Sticky |
| P3 | 整体硬超时且池空、无活流赢家 | **默认仍 503**（与现网一致）；可选 504 见下 | in-flight abort；超时前已入池则优先路径 S |

**关于 504（breaking change 标注）**

- 现网 `resolveHedgeTerminalError` **只有**：透传（CLIENT_ABORT / NON_RETRYABLE_CLIENT_ERROR）或统一 **503**。  
- 若引入 **504** 表示「racing 整体 deadline 到期」，属于**对外 HTTP 契约的 breaking change**：客户端重试策略通常对 503 与 504 处理不同（503 更常被重试，504 语义含糊）。  
- **架构默认建议：P3 继续用 503 + 同一文案**，与现网一致，避免 silent 契约破裂。  
- 若产品坚持用 504 区分「供应商都挂了」与「总等待超时」，必须：  
  1. 产品明确签字；  
  2. 客户端 / SDK / 文档同步变更重试与告警；  
  3. 监控按新 status 拆分。  
- 在上述确认完成前，**实现不得擅自改为 504**。

**其他说明**

- Fake 200 链路上记失败，**不得** 200 成功结束；全失败时对外归 P2/P3 的 503。  
- 「明确报错不作为慢响应结果」：显式错误进失败集，不进快照池。

### 3.3 与「继续等待」的边界（含末轮 in-flight）

已确认：最后一轮候选首字都慢于当前最佳完整结果 → 路径 S；若**没有**完整结果 → **继续等仍在运行的候选**，直到可交付、全 settled 或总 deadline。

#### 3.3.1 各层超时职责（必须写清）

| 超时 | 作用 | 到期行为 |
|------|------|----------|
| **轮 Discovery SLA**（冷启动如 10s；Sticky-Discovery 首轮用 Sticky SLA） | 决定「本轮是否还能当场出路径 L 赢家」以及「是否开下一轮」 | 普通轮：无有效首字 → 本轮关闭夺冠窗，attempt **转后台**（drain 争入池），启动下一轮（若仍非最后一轮）。**不**因轮 SLA 单独终态失败。 |
| **最后一轮的轮 SLA** | 与普通轮相同：约束「当轮有效首字」的竞赛窗 | 到期后：**不再**接受「新的当轮有效首字」作为路径 L 夺冠（见下例外比较规则已在窗内完成的首字仍有效）；窗内未出首字的当轮候选 → **转后台 drain**，与旧池一起进入「池空则等完整可交付」阶段。 |
| **整体硬超时** `racingTotalDeadlineMs`（独立配置；**不**复用流式静默期） | 整次 racing 的上限 | 池非空 → 路径 S；池空且无路径 L → 终态 P3（默认 503）。**这是防止无限等待的最终闸门。** |
| **单 Provider 连接/读超时**（现有 first-byte / 传输层） | 单连接卡死 | 该 attempt 失败 settled，不拖死全局 |

#### 3.3.2 末轮 in-flight 迟迟无有效首字

- **不再单独复用「再一个 Discovery SLA」去无限续命**；轮 SLA 到期后该候选只保留「后台出完整快照」资格。  
- **等待上限 = 整体硬超时**（若未配置整体硬超时，则架构要求**必须配置**或回退为「所有 in-flight 自然 settled」——但生产环境**强烈要求**配置 `racingTotalDeadlineMs`，避免网络半开连接导致请求挂死）。  
- 池已非空时：最后一轮比较器在「当轮候选均已证明 `T_first` 更差或已过轮 SLA 仍无有效首字」后即可路径 S，**不必**等慢连接完整结束（未入池的慢连接 cancel 或输家计费即可）。  
- 池空时：等任一后台 attempt 完整可交付入池（路径 S）或全部失败/触顶硬超时。

硬超时其它建议：

- 默认值需产品拍板；量级参考：≥ Sticky SLA × 预期最大轮数 + 余量，或单独业务 SLO。  
- 超时后：池非空 → 路径 S；池空 → P3。

### 3.4 业务问题 B 一句话决策

> **终态对外：abort/不可重试客户端错误透传；其余默认一律 503 统一不可用（504 为需产品+客户端确认的 breaking 可选项）。对内：fake 200/空/协议失败不入池、不绑 Sticky；有快照则路径 S 成功。末轮慢连接：轮 SLA 只关夺冠窗；无限等由整体硬超时兜住。**

---

## 4. 整体业务闭环评估

### 4.0 「最后一轮」定义（blocking 补齐）

**最后一轮（Final Round）** 是 Discovery 中**不会再开启后续并行轮次**的那一轮。触发条件（满足任一即在本轮启动时标记 `isFinalRound=true`）：

1. **余量不足一整轮**：在排除「已选择过的 Provider」（含本请求已 launch 的 id、以及 Sticky-Discovery 中 pool-only 的原 Sticky）之后，**剩余可选 Provider 数量 `R < N`**（`N` 为每轮固定选择数）。则本轮启动 **全部 `R` 个**剩余候选（`R=0` 则无新候选，直接进入「仅后台 + 池」决胜，见下）。  
2. **已用尽**：`R = 0` 时不存在新的并行轮；若仍有后台 in-flight 或池非空，进入决胜/等待，不再 `launch` 新 Provider。  
3. **显式轮次上界**（可选配置 `maxDiscoveryRounds`）：若已完成的非最终轮次数达到上界 − 1，则下一轮强制为最后一轮（即使 `R ≥ N` 也只再开一轮，本轮仍选 `min(N, R)` 家，**本轮结束后剩余未选 Provider 本请求内不再使用**）。未配置上界时仅由 (1)(2) 决定。

**推论**：

- 普通轮（`isFinalRound=false`）：轮 SLA 内有效首字 → **路径 L 立即结束竞赛**；无首字 → 后台化 + 下一轮。  
- 最后一轮（`isFinalRound=true`）：**禁止**「旧池已有完整结果就跳过等待本轮候选」；必须跑完本轮比较器（§4.1）。  
- Sticky 单飞**不是** Discovery 轮次，不计入 `maxDiscoveryRounds`；Sticky 超时后的 Sticky-Discovery **第一轮** SLA 用 Sticky SLA，但是否为最后一轮仍按上面 (1)(2)(3) 判断。

### 4.1 状态机（逻辑闭环）

```
[入口]
  ├─ 有 Sticky Provider？
  │    ├─ 是 → Sticky 单飞（SLA = Sticky SLA，如 20s）
  │    │       ├─ 有效首字 → 路径 L 赢家 + Sticky → 活流透传
  │    │       └─ 超时/明确失败 → Sticky-Discovery
  │    │            · 原 Sticky：不参加普通轮；继续/drain → 仅可入快照池
  │    │            · 按 §4.0 开轮；第一轮 SLA = Sticky SLA
  │    └─ 否 → Cold Discovery（SLA = Discovery SLA）
  │
[Discovery 轮 · 非最后一轮]
  · 选 min(N, R) 家，排除已选；标记 isFinalRound=false
  · 轮 SLA 内有效首字 → 路径 L（活流）+ Sticky；其它 attempt：cancel 或输家计费
    （普通轮当场已有路径 L 时，通常不再为兜底保留全量快照，除非产品另开「并行计费 drain」）
  · 轮 SLA 内无有效首字 → 当轮 attempt 转后台争入快照池；开下一轮
  · 超 cap → 丢池资格 + cancel（§2.4）
  │
[Discovery 轮 · 最后一轮]  （§4.0）
  · 选 min(N, R) 家（R=0 则无新 launch）
  · 轮 SLA 仍生效：用于「当轮有效首字」竞赛窗与 T_first 采样
  · 比较器（不得因旧池完整而跳过等待本轮窗）：
      A. 本轮某候选在轮 SLA 内得到有效首字，且
         T_first(候选) ≤ T_first(池最佳完整快照)（池空则视为通过）
         → 路径 L：该候选活流赢家，截断其它
      B. 本轮所有候选均在轮 SLA 内得到有效首字，且皆 >
         T_first(池最佳)，且池非空
         → 路径 S：整包回放池最佳
      C. 轮 SLA 结束时：部分候选无有效首字
         → 无首字者转后台；已有首字者按 A/B 与池比较；
            若尚不能决胜且池非空且所有「已出首字」均更差 → 路径 S；
            若池空 → 进入「仅后台等待完整可交付」（上限=整体硬超时）
      D. 池空且后台仍无完整可交付直至硬超时或全失败 → 终态 §3
  │
[Fake 200 / 无效]
  · 不进路径 L 赢家、不进快照池、不绑 Sticky
  │
[终态失败]
  · 见 §3
```

**闭环判断：在采用 §2 双路径 + §3 分层终态 + §4.0 最后一轮定义后，规则集合自洽；活流与快照交付不再混写。**

### 4.2 与已确认规则的逐条闭合

| 规则 | 闭环？ | 备注 |
|------|--------|------|
| 每轮固定 N，排除已选 | ✅ | `R < N` 时最后一轮选 R 家 |
| 普通轮 Discovery SLA 内有效首字 → 赢 + Sticky | ✅ | 路径 L |
| 旧轮不参加普通竞赛，但参与最终兜底 | ✅ | 仅路径 S；禁止旧完整直接掐断最后一轮比较窗 |
| 最后一轮按首字耗时比，旧完整不自动结束 | ✅ | §4.0 + 比较器 A/B/C |
| 兜底可成为 Sticky | ✅ | 路径 S 同样绑 Sticky |
| Sticky 超时 → Sticky SLA 的 Discovery；原 Sticky 只进兜底 | ✅ | pool-only + 路径 S |
| Fake 200 不进赢家/兜底 | ✅ | 两路径均剔除 |

### 4.3 关键缺口（实现缺口，非业务逻辑漏洞）

1. **有效首字定义**（协议级）仍粗：现网赢家条件过宽。  
   - 建议夺冠/入池最小集（与 `fake-streaming/response-validator` / 各 family 对齐）：  
     - Anthropic：非 error 的 `content_block_start`（tool_use 等）或带 text/`partial_json` 的 `content_block_delta`；**单独 `message_start` 不足**。  
     - OpenAI Chat：`choices[].delta` 含 content/tool_calls 等。  
     - OpenAI Responses / Gemini：各自可交付事件/parts。  
   - 强 fake 信号在首块/首事件即可淘汰 attempt。

2. **每轮并行 N**：现网是 1 + 超时后再 1，需调度器改造。  

3. **整体硬超时**：现网缺 racing 级 deadline；生产必配以防末轮慢连接挂死。  

4. **多 attempt 完整缓冲**：现网输家 drain 为计费，不为回放；需独立快照通道与 cap（超 cap cancel）。  

5. **Sticky SLA ≠ Discovery SLA 分配置**；静默期超时不复用。

---

## 5. 实现可行性（基于现有 CCH，仍不编码）

### 5.1 可复用

- Shadow session / attempt 隔离、agent 引用计数、client abort 扇出。  
- 输家计费链路：`billHedgeLosers`（配置开关）→ attempt 上 `billAsLoser` → `startLoserBilling`（forwarder，独占 `attempt.reader` drain）→ `finalizeHedgeLoserBilling`（response-handler，`hedge_losers` 账务）。  
- 决策链 reason：可增 `discovery_round` / `final_round` / `live_winner` / `fallback_winner` / `sticky_discovery` / `fallback_buffer_cap_exceeded`。  
- Fake 200 检测库与 circuit / 清 Sticky 绑定侧效应。  
- Provider 排除选择：`pickRandomProviderWithExclusion`。  
- 终态 503 文案与 client-safe 映射；**路径 L** 可继续用 `buildBufferedFirstChunkStream`。  
- **路径 S 与输家计费**：复用「单 reader 独占 drain」模式，但在退出夺冠窗时与 `startLoserBilling` **互斥择一**（见 §2.4-5），不能并行挂两套 drain。

### 5.2 需新增/大改（工作量粗估）

| 模块 | 改动 | 风险 |
|------|------|------|
| Racing 调度器 | 轮次、N 并行、`isFinalRound`、SLA 双轨、末轮比较器 A–D | 高 |
| 有效首字门闸 | 流上增量 parse（有界） | 中 |
| 快照池 | 完整 buffer + 校验 + 路径 S Response；cap → cancel | 中 |
| 路径 L/S 分支 | 决胜点显式枚举，禁止混用回放 | 中（文档/单测约束） |
| Sticky 阶段 | 原 Sticky pool-only | 中 |
| 配置 | Discovery/Sticky SLA、N、maxRounds、total deadline、buffer cap | 低 |
| 测试 | 末轮不提前结束、路径 L 非回放、超 cap cancel、503 契约 | 高 |

### 5.3 可行性结论

- **业务方案在补齐 §2 双路径 / §3 / §4.0 后可闭环。**  
- **工程上可在现有 hedge 演进**，属调度语义升级，非调参。  
- **首期砍掉半截回放**；路径 L 沿用活流，路径 S 才整包回放。  
- **单 attempt reader 独占**：路径 S 缓冲与 `startLoserBilling` 互斥，调度器在退出夺冠窗时强制 `consumeMode`。  
- **不建议**在未定义有效首字规则前上线多轮兜底。

---

## 6. 总裁决（给产品 / 首席架构师对齐用）

1. **暂存交付（旧轮）**：完整可重放快照 + 校验；**当轮赢家活流续传**；二者禁止混用。  
2. **超 cap**：丢兜底资格并 cancel，不保留假「活流赢」出口。  
3. **最后一轮**：由 `R < N` / 用尽 / 可选 `maxDiscoveryRounds` 定义；末轮仍有轮 SLA 夺冠窗。  
4. **最终失败**：默认透传 abort/不可重试，否则 **503**；**504 为 breaking 可选项，需产品+客户端确认后方可落地**。  
5. **末轮慢连接**：轮 SLA 关闭路径 L 窗口；继续等完整可交付仅受 **整体硬超时**（及自然 settled）约束。  
6. **闭环与可行性**：逻辑闭环成立；现网可托底；需专项实现，本次不编码。

---

## 7. 建议的后续实现顺序（仅建议）

1. 固化有效首字与入池校验（按协议 family 单测）。  
2. 快照池 + 路径 S 回放 + cap→cancel。  
3. 调度器：N 轮次 + §4.0 `isFinalRound` + 比较器 A–D + 路径 L 活流。  
4. Sticky 双 SLA 与 pool-only 原 Sticky。  
5. `racingTotalDeadlineMs` 与终态错误表（默认 503）。  
6. 可观测性：round、isFinalRound、T_first、live_vs_fallback、cap_exceeded、fake_200 reject。

---

*文档结束。*
