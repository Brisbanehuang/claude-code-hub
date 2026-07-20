# 供应商竞速方案架构评审结论

**日期**: 2026-07-20  
**范围**: 剩余两个业务问题 + 整体业务闭环 + 基于现有 Claude Code Hub（CCH）实现的可行性  
**结论性质**: 架构/产品决策建议，**不编码**

---

## 1. 现状基线（与目标方案的差距）

当前 CCH 的 streaming hedge（`ProxyForwarder.sendStreamingWithHedge`）是**串行触发式竞速**，不是目标方案里的「固定每轮 N 家 + 多轮 Discovery + 最终兜底池」：

| 能力 | 现有实现 | 目标方案 |
|------|----------|----------|
| 触发 | 初始 Provider 的 `firstByteTimeoutStreamingMs` 到期后 `launchAlternative` 再起一家 | 每轮固定选 N 家并发 |
| 赢家 | **任意非空首字节** `readFirstReadableChunk` 即 `commitWinner`，立刻 `abortAllAttempts` | **有效首字**（协议语义）才赢；旧轮继续跑并入兜底池 |
| 输家 | 可 cancel，或 `billHedgeLosers` 后台 drain 计费；**不参与再比较** | 旧结果暂存，最后一轮按首字耗时与完整可交付性兜底 |
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

旧轮请求不参加后续普通竞赛，但继续产生结果并参与最终兜底。若最终选用旧轮结果，客户端尚未收到任何字节时，应如何交付？

可选语义：

1. **完整可重放（full snapshot）**：仅当该 attempt 已自然结束且校验为可交付，才把整段 body（SSE 全事件 / 非流式 JSON）按原样写回客户端。  
2. **边收边缓存、从首字起回放（prefix buffer + live tail）**：持续缓存已收字节；选中时先回放缓存前缀，再接上仍存活的上游 reader 续传。  
3. **仅完整结果可兜底（strict）**：没有完整可交付快照就**不能**用该 attempt 兜底，只能等仍在跑的候选或终态失败。

### 2.2 推荐结论（默认采用 1 + 3 的组合，不以 2 作为默认）

**推荐：兜底交付必须「完整可重放 + 已通过可交付校验」；不允许「半截前缀回放」作为默认业务语义。**

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
   方案 2 需要每 attempt：tee/缓冲上限、内存 cap、中途 abort 一致性、与 `billHedgeLosers` drain 共用 reader 的互斥、客户端 abort 时多路清理。现有 `buildBufferedFirstChunkStream` 只解决「赢家已选定后的首块补回」，**不能**直接扩展成多候选长时间缓存回放。  
   方案 1 可复用「完整 body 已在内存/有界 buffer」模式（response-handler 已有 head/tail 文本累积思路），或对兜底候选在后台 drain 到有界 buffer，仅 `ok` 的进入 pool。

4. **与已确认规则的一致性**  
   - 「A 有完整结果、首字 20s；E/F 首字都 >20s → 可用 A」—— 明确要求 **完整可交付**。  
   - 「E/F 首字都慢，但没有完整可交付 → 继续等」—— 否定「仅有首字即可交付」。  
   - 因此 **strict 完整可重放** 是对上述两条的直接 entailing，而不是额外产品偏好。

### 2.3 交付形态（实现指引级，非本次编码）

当最终赢家是「兜底池中的 attempt R」时：

1. 对客户端：新建 `Response`，`status/headers` 取 R 的上游成功响应快照；body 为 **已缓冲的完整字节序列** 的一次性 ReadableStream（可分 chunk 回放，但内容固定）。  
2. Sticky：按已确认规则，兜底赢家可 `updateSessionBindingSmart` 绑定到 R。  
3. 计费：R 按赢家路径；其他 attempt 走现有输家 cancel / `hedge_loser_billed` 策略（可配置），**不得**因「曾进过兜底池」双计赢家。  
4. **内存 cap**：每 attempt 缓冲上限（建议与现有 SSE 检测/日志截断同量级可配置）；超 cap 的 attempt **失去兜底资格**，仅可继续作为「若在普通轮内先出有效首字则当场赢」的活流（若仍在 Discovery 窗内）。超 cap 记链路上的明确 reason，避免静默丢兜底。

### 2.4 明确不推荐作为默认

- **从首字开始的实时回放半截流**：仅可作为后续优化（例如「已验证 message_stop 前的完整可交付前缀」且协议允许），首期不做。  
- **把未校验的 firstChunk 当可交付**：与 fake 200 边界冲突。

### 2.5 业务问题 A 一句话决策

> **暂存结果 = 后台 drain 至完整 + 协议/可交付校验通过后的只读快照；最终兜底只从快照池按首字耗时选优并整包回放。无完整可交付快照则不能用该 Provider 结束请求。**

---

## 3. 业务问题 B：全部失败 / 无可交付结果时的最终失败语义

### 3.1 触发条件（应统一走同一终态）

在整体请求硬超时（若配置）或「候选耗尽且 in-flight 全部 settled」时，若：

- 没有任何 attempt 产生 **有效首字且完整可交付** 的快照，且  
- 当前也没有仍可等待的 in-flight 候选，  

则进入 **Terminal Failure**。

（有完整可交付快照时，即使最后一轮「竞赛」输了，也应走兜底成功，而不是失败。）

### 3.2 推荐结论：分层错误，而不是单一 503

与现有 `resolveHedgeTerminalError` 对齐并扩展：

| 优先级 | 条件 | 对客户端 | 对内链/熔断/Sticky |
|--------|------|----------|-------------------|
| P0 | 客户端 abort | **499**（或现有 CLIENT_ABORT 映射），保留 abort 语义 | 不记 Provider 熔断；不写 Sticky |
| P1 | 存在 **NON_RETRYABLE_CLIENT_ERROR**（明确 4xx 业务/鉴权类，且判定为请求本身不可换 Provider 重试） | **原样透传该错误**（status + 安全文案） | 按现有 client_error 链；通常不熔断 Provider |
| P2 | 全部为 Provider/基础设施失败、空响应、fake 200、超时、无候选 | **503** + 现有文案 `所有供应商暂时不可用，请稍后重试`（`ALL_PROVIDERS_UNAVAILABLE_MESSAGE`） | 各 attempt 已按规则记 `retry_failed` / fake-200 / timeout；**清除或不要建立** 成功 Sticky；可保留「失败 Provider 排除」供同 session 后续请求 |
| P3 | 硬超时打断仍有 in-flight | **504 或 503**（建议 **504** 表示网关/代理侧整体等待超时，与单 Provider 524 区分）；body 仍用统一安全文案 | in-flight abort；已完整可交付的若在超时前入池则应优先成功兜底，避免「有快照仍 504」 |

**说明**：

- 现有 hedge 终态已是：**abort/不可重试客户端错误 → 原样；否则 503 统一不可用**。目标多轮方案应**保持这一对外契约**，避免客户端出现「有时 502 有时 500 有时 upstream 原文」的碎片化。  
- Fake 200 在链路上应记 **失败**（effective 4xx/5xx + `FAKE_200_*`），**不得**冒充 200 成功结束；对客户端若已全部失败，归入 P2 的 503，而不是把某个 FAKE_200 的 502 直接当最终 HTTP（除非唯一 attempt 且希望透传——**不推荐**，多 Provider 场景统一 503 更可缓存/重试）。  
- 「明确报错继续用现有错误处理，不作为慢响应结果」—— 与 P1/分类器一致：显式错误进失败集，不进兜底快照池。

### 3.3 与「继续等待」的边界

已确认：最后一轮候选首字都慢于当前最佳完整结果 → 用最佳完整结果；若**没有**完整结果 → **继续等仍在运行的候选**，直到：

1. 出现第一个 **完整可交付** 快照，或  
2. 全部 settled 仍无快照，或  
3. 命中**整体请求硬超时**。

硬超时建议：

- **独立配置**（不要复用流式静默期，也不要仅复用单 Provider first-byte）。  
- 默认值需产品拍板；架构建议：≥ Sticky SLA × 轮数上界，或单独 `racingTotalDeadlineMs`。  
- 超时后：若池非空 → 仍成功兜底；池空 → P3。

### 3.4 业务问题 B 一句话决策

> **终态对外：abort/不可重试客户端错误透传；其余一律 503 统一不可用（硬超时建议 504）。对内：fake 200/空/协议失败全部算失败且不入池、不绑 Sticky；有完整可交付快照则永不走终态失败。**

---

## 4. 整体业务闭环评估

### 4.1 状态机（逻辑闭环）

```
[入口]
  ├─ 有 Sticky Provider？
  │    ├─ 是 → Sticky 单飞（SLA = Sticky SLA，如 20s）
  │    │       ├─ 有效首字 → 赢家 + 保持/续绑 Sticky → 流式透传
  │    │       └─ 超时/明确失败 → 进入 Sticky-Discovery
  │    │            · 首轮并行 N 家（排除原 Sticky 与已选）
  │    │            · 本阶段 Discovery SLA = Sticky SLA（如 20s）
  │    │            · 原 Sticky 若仍有可交付进展 → 只进兜底池，不进普通轮
  │    └─ 否 → Cold Discovery
  │
[Cold / 后续轮]
  · 每轮固定 N，排除已选
  · 轮 SLA = Discovery SLA（冷启动如 10s；Sticky 阶段第一轮例外用 Sticky SLA）
  · 轮内有效首字 → 立即赢家 + Sticky + 透传；其它 in-flight → 输家策略（计费/取消），不进「普通竞赛」但可选择 drain 入池（见下）
  · 轮内无有效首字 → 旧 attempt 继续后台；开下一轮
  · 最后一轮：必须等到本轮候选「有效首字耗时」与池中最佳完整结果比较完毕
       若本轮有效首字 T ≤ 池最佳 T_first → 本轮该 Provider 赢（可截断其它）
       若本轮均 > 池最佳且池非空 → 完整回放池最佳
       若池空 → 继续等 in-flight 直至可交付或终态失败

[Fake 200 / 无效]
  · 不进赢家、不进兜底池、不绑 Sticky；记失败链

[终态失败]
  · 见 §3
```

**闭环判断：在采用 §2 完整快照兜底 + §3 分层终态后，规则集合自洽，无「有结果却无法交付」或「无结果却 200」的空洞。**

### 4.2 与已确认规则的逐条闭合

| 规则 | 闭环？ | 备注 |
|------|--------|------|
| 每轮固定 N，排除已选 | ✅ | 现有 `launchedProviderIds` + `selectAlternative` 可扩展为批量 N |
| 普通轮 Discovery SLA 内有效首字 → 赢 + Sticky | ✅ | 需把「非空字节」升级为「有效首字」检测 |
| 旧轮不参加普通竞赛，但参与最终兜底 | ✅ | 依赖 §2 快照池；**禁止**旧轮完整结果直接掐断最后一轮 |
| 最后一轮按首字耗时比，旧完整不自动结束 | ✅ | 比较器：`T_first`，准入：完整可交付 |
| 兜底可成为 Sticky | ✅ | 与现网 `updateSessionBindingSmart` 一致 |
| Sticky 超时 → Sticky SLA 的 Discovery；原 Sticky 只进兜底 | ✅ | 需区分 `phase: sticky | sticky_discovery | cold` |
| Fake 200 不进赢家/兜底 | ✅ | 需前移部分检测；完整校验在入池时强制 |

### 4.3 关键缺口（闭环上的「实现缺口」，不是业务逻辑漏洞）

1. **有效首字定义**（协议级）仍粗：现网赢家条件过宽。  
   - 建议入池/夺冠最小集（与 `fake-streaming/response-validator` / 各 family 对齐）：  
     - Anthropic：非 error 的 `content_block_start`（非空类型 tool_use 等）或带 text/`partial_json` 的 `content_block_delta`；**单独 `message_start` 不足**（与 usage 过早出现的现网注释一致）。  
     - OpenAI Chat：`choices[].delta` 含 content/tool_calls 等可交付。  
     - OpenAI Responses：可交付 output 事件。  
     - Gemini：candidates parts 可交付。  
   - 强 fake 信号（HTML、顶层 error 非空、空 body）在**首块/首事件**即可淘汰 attempt。

2. **每轮并行 N**：现网是 1 + 超时后再 1，需调度器改造。  

3. **整体硬超时**：现网多为单 Provider first-byte / 非流式 total，缺 racing 级 deadline。  

4. **多 attempt 完整缓冲**：现网输家 drain 为计费，不为回放；需独立「兜底快照」通道与 cap。  

5. **Sticky SLA ≠ Provider.firstByteTimeoutStreamingMs 混用**：目标要求 Discovery 与 Sticky 分配置；静默期超时不复用 —— 需配置模型扩展（本次不设计表结构细节）。

---

## 5. 实现可行性（基于现有 CCH，仍不编码）

### 5.1 可复用

- Shadow session / attempt 隔离、agent 引用计数、client abort 扇出。  
- 输家计费与 `hedge_losers` 账务。  
- 决策链 reason：`hedge_triggered` / `hedge_launched` / `hedge_winner` / `hedge_loser_*`（可增 `discovery_round` / `fallback_winner` / `sticky_discovery`）。  
- Fake 200 检测库与 circuit / 清 Sticky 绑定侧效应。  
- Provider 排除选择：`pickRandomProviderWithExclusion`。  
- 终态 503 文案与 client-safe 映射。

### 5.2 需新增/大改（工作量粗估，供排期）

| 模块 | 改动 | 风险 |
|------|------|------|
| Racing 调度器 | 轮次、N 并行、phase、SLA 双轨、最后一轮比较器 | 高：状态并发、与现 hedge 互斥替换 |
| 有效首字门闸 | 流上增量 parse（有界） | 中：误判会导致慢切换或错杀 |
| 兜底快照池 | 完整 buffer + 校验 + 整包 Response | 中：内存；需 cap 与背压 |
| Sticky 阶段 | 超时后不立刻 abort 原 Sticky，改为 pool-only | 中：与现「赢家即 abortAll」相反 |
| 配置 | Discovery SLA、Sticky SLA、N、total deadline、buffer cap | 低 |
| 测试 | 多轮时序、最后一轮不提前结束、fake 200 不入池、503/499 | 高：应用现有 hedge 单测模式扩展 |

### 5.3 可行性结论

- **业务方案在补齐 §2/§3 决策后可闭环。**  
- **工程上可在现有 `sendStreamingWithHedge` 演进**，但不是参数微调：属于调度语义升级（并行轮次 + 延迟决胜 + 快照兜底）。  
- **首期应砍掉半截回放**，把复杂度压在调度与入池校验；与现网「计费 drain」并行时注意 reader 所有权单一。  
- **不建议**在未定义有效首字增量规则前上线「多轮兜底」，否则 fake 200 会重新污染 Sticky。

---

## 6. 总裁决（给产品 / 首席架构师对齐用）

1. **暂存交付**：完整可重放快照 + 可交付校验；禁止默认半截回放。  
2. **最终失败**：透传 abort/不可重试客户端错误；否则 503 统一不可用；整体硬超时建议 504；有快照则成功兜底。  
3. **闭环**：竞速 →（可选）多轮 → 最后一轮首字比较 → 快照兜底 → Sticky；fake 200 全程剔除；Sticky 超时进 Sticky-Discovery 且原 Sticky 仅兜底 —— **逻辑闭环成立**。  
4. **可行性**：现网 hedge/Sticky/fake-200/503 **可托底**；缺并行轮次、有效首字、快照池、双 SLA、总时限 —— **可做，需专项设计与测试，本次不编码。**

---

## 7. 建议的后续实现顺序（仅建议）

1. 固化有效首字与入池校验（单测矩阵按协议 family）。  
2. 快照池 + 完整回放 + 内存 cap。  
3. 多轮 N 调度 + 最后一轮比较器。  
4. Sticky 双 SLA 与 pool-only 原 Sticky。  
5. 总 deadline 与终态错误表。  
6. 可观测性：round、T_first、pool hit、fake_200 reject。

---

*文档结束。*
