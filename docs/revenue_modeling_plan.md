# Revenue Breakdown Modeling — AI Research Assistant 升级方案（生产版）

> Target: 把现有 Deep Research 聊天升级为一套能对特定公司做**可审计、可编辑、可进化**的收入拆分建模工作流，非技术研究员可直接在网页上调整工作流。
> 目标输出形态：参考 `/home/ygwang/images/Modeller-example.xlsx`（WDC / STX / LITE / AXTI 四家公司的量×价→收入→毛利→净利→EPS→PE 级联表）。
> **交付定位：生产版（非 MVP）**。Phase 0 聚焦光通信行业打磨，但数据模型、Recipe 引擎、Playbook、公式引擎从第一行代码起就按**多行业可扩展**设计 —— HDD/半导体/软件后续只需加 Recipe 模板 + Playbook，不改任何骨架代码。
> 作者：Claude，2026-04-23。

---

## 0. TL;DR（给忙人的一页纸）

**一句话**：在现有 chat_llm + tool-calling 基础之上，加一个"**Recipe 驱动 + 单元格级可审计**"的建模层 —— 用户看到的是一张**每个数字都可点开看来源与推理链**的活 Excel，研究员用画布编辑工作流，系统把每次人工改写累积成 industry-level 的 **markdown playbook**，自我进化。

**四个支柱，对应用户四个需求**：

| 用户需求 | 技术支柱 | 关键技术 |
|---|---|---|
| 1. 可扩展、研究员可编辑 | **Recipe Canvas + Industry Pack** | 可视化 DAG 编辑器 + 行业插件包（Daloopa Skills 模式），光通信首发，半导体 / HDD / 软件可插拔 |
| 2. 多步迭代、高度准确、无幻觉 | **ModelCell + Chain-of-Verification + Debate** | 每数字一个结构化单元格，CoVe 反事实核验 + LLM 三方辩论（Opus/Gemini/GPT）+ 数值健全性 |
| 3. 自我进化 | **Lessons.md Playbook（人工审批）** | 研究员反馈 → 周度 LLM 蒸馏 → admin/boss 审批 → 下一轮 few-shot 注入 |
| 4. 每个数字有来源 | **Provenance Trace** | 从 Phase B 向量库 / MCP 工具到单元格的完整链路，前端 UI 一键追溯 + 源文档原文并排 |

**用户已拍板决策（§13 已解锁）**：
- **首发行业**：光通信，但全栈按多行业可扩展设计
- **公式引擎**：生产级 Excel 兼容（完整算术 + 数学/统计/查找/条件/文本/日期函数全集，详见 §4.3）
- **Playbook 进化**：每条 lesson 需 admin/boss 审批
- **LLM 选型**：Opus 4.7（1M context）为主 agent 与建模主力 / Gemini 3.1 Pro 为独立 verifier / GPT-5.4 为 debate tiebreaker

**实施分 6 期、约 14–18 周**（详见 §11）。生产版 Phase 1 即交付完整可用的光通信建模（非半成品）。

---

## 1. 业界对标（做什么像什么）

我们不是凭空发明这套系统，下面是**各家投行/数据商/AI 原生产品**目前已经验证可行的模式。我们的方案=把它们的关键设计裁剪/融合到咱们现有技术栈里。

| 产品 | 对我们最有借鉴价值的设计 | 我们要抄的部分 |
|---|---|---|
| **Hebbia Matrix** | "Iterative Source Decomposition (ISD)"—把复杂问题拆成小步、每步独立取证、结果呈现为**带 citation 的 spreadsheet**；citation-first 原则："an un-sourced fact is worse than no fact at all" | 表格式呈现 + 单元格 citation + 步骤级分解 |
| **Bloomberg Terminal AI / ASKB** | 每条结论**紧贴源文档**并排显示；entailment + factuality checks；关键决策有 human-in-the-loop | 并排源文档验证 UI、模型输出→源文档定位 |
| **Morgan Stanley AskResearchGPT** | 把 7 万份自研报告作为 RAG 底座，回答要能 trace 到具体段落 | 内部纪要库（进门 / Alpha派 / 久谦）是我们的"研报库"—充分利用 |
| **Daloopa Skills (on Claude MCP)** | "Skill" = 一个可调优的 prompt+工具+输出模板，**分析师可改**，跑在 MCP 上。15+ 种 skill：earnings read、comp sheet、unit economics、model update | 我们的 **Recipe** 直接继承这个概念 |
| **AlphaSense** | Segment-by-geo / product 自动拆分，结构化 JSON 输出 | 收入拆分字段的 schema 参考 |
| **Letta / MemGPT / ACE** | Markdown 源 + 派生索引的 agent memory；execution feedback → playbook evolution | 我们 §8 的"教训 markdown 进化"直接抄 |
| **Chain-of-Verification (ACL 2024)** | LLM 先 draft → 生成核验问题 → **独立回答**每个核验问题 → 合成最终答案；F1 +23% | §7 的多步核验主算法 |

**关键判断**：不要重写 Matrix/Daloopa 的能力，要做的是**把这些能力接入咱们私有的纪要库/专家网络**，并让**研究员能自己改工作流**（这是咱们相对 SaaS 竞品的唯一 moat）。

---

## 2. 主观研究员工作流逆向工程

研究员描述的流程可抽象成**一个依赖图**：

```
业绩会纪要(CC) ──┐
                ├─► 公司业务部门拆分 ──┐
专家访谈纪要 ────┘                    ├─► 量×价模型 ─► 各业务 rev
                                      │
管理层指引 ─► 业务增长曲线分类 ───────┘
         │
         └─► 稳定/负增长业务：3-5% 或 guidance 直接填
         └─► 高成长业务：
               ├─ 量：跟某个外部增速铆（如 TPU/GPU 增速）或从专家/纪要获取
               └─ 价：问专家或搜纪要

各业务 rev ─► 合计 Total Rev ─► × Operating margin (EBIT/EBITDA，看管理层或可比公司)
                                                │
                                                └─► - Interest/Tax (历史) ─► NI
                                                                            │
                                                                          ÷ Shares ─► EPS
                                                                                      │
                                                                     Price ÷ EPS ─► PE
```

**Excel 样本印证这个流程**（WDC sheet 解读）：
- **行 10–15（EB 量）** = Nearline + Other → 每行独立 YoY。黄色单元格 = 研究员主观假设（F13=35%, G13=25%, H13=23%）
- **行 17（ASP 亿美元/EB）** = Rev / EB，F18/G18/H18（12%/7%/3%）= 研究员对涨价节奏的主观判断
- **行 5（Non-HDD）** 直接填历史小幅下滑值 → 对应"稳定/负增长直接填数"规则
- **行 20–22（GM / Opex / NM）** = 管理层指引框架（GM 从 26% → 49%，Opex 16% 稳定，NM = GM - Opex）
- **列 P（文字批注）** = "WDC给出24–28年出货位元CAGR约23%指引…" → 假设的 **来源与锚点**
- **E41="久谦的专家" / H41="自己找的两个专家"** → 同一指标的**多组来源**，研究员会看口径差异

**核心结构（我们的数据模型要能表达的）**：
1. 每个数字是**单元格**，挂在**业务部门 / 期间**的路径上（`LITE.EML+CW.100G.volume.2026`）。
2. 每个单元格要么是**硬编码值**、要么是**公式**（上下游单元格的函数）。
3. 每个硬编码值有**source_type**（guidance / expert / historical / assumption / inferred）和**文本批注**。
4. 同一指标常有**多个来源并列**（Excel 里通过不同分组表格表达，数字系统里应用多 source 列表 + 研究员选用哪个做主值）。
5. 公式级联自动传播（改 F13 → F12 → F3 → F23 → F25 → F26）。

---

## 3. 架构总图

```
┌────────────────────────────────────────────────────────────────────┐
│  FRONTEND  (React)                                                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────┐  │
│  │ Model Spreadsheet│  │ Recipe Canvas    │  │ Cell Inspector    │  │
│  │ (编辑/查看单元格) │  │ (研究员改流程)   │  │ (源文档 + 推理链) │  │
│  └────────┬────────┘  └────────┬─────────┘  └─────────┬─────────┘  │
└───────────┼───────────────────┼──────────────────────┼────────────┘
            │ SSE (新事件类型: cell_update, step_progress, verify_flag)
┌───────────┼───────────────────┼──────────────────────┼────────────┐
│  BACKEND  │                   │                      │            │
│  ┌────────▼────────┐   ┌─────▼─────────┐    ┌───────▼─────────┐   │
│  │ Modeling API    │   │ Recipe Engine │    │ Provenance API  │   │
│  │ /api/models/…   │   │ (DAG 执行器)  │    │ /api/provenance │   │
│  └────────┬────────┘   └──────┬────────┘    └───────┬─────────┘   │
│           │                   │                     │             │
│  ┌────────▼───────────────────▼─────────────────────▼─────────┐   │
│  │  ModelCell Store  (PostgreSQL JSONB + 完整版本历史)        │   │
│  └────────┬───────────────────────────────────────────────────┘   │
│           │                                                        │
│  ┌────────▼────────┐   ┌──────────────┐    ┌─────────────────┐    │
│  │ Step Executors  │◄──┤ Recipe Store │◄───┤ Playbook.md     │    │
│  │ (8 种步骤类型)  │   │ (YAML/JSON)  │    │ (Lessons + Rules)│    │
│  └────────┬────────┘   └──────────────┘    └────────┬────────┘    │
│           │                                          │             │
│           │  已有基础设施，直接复用：              │             │
│           │  • chat_llm.call_model_stream_with_tools (多轮tool-call)│
│           │  • web_search_tool / kb_vector_query (Phase B hybrid) │
│           │  • alphapai_service / jinmen_service (MCP recall)     │
│           │  • user_kb_tools (团队共享个人库)                     │
│           │  • CitationTracker (全局 [N] 索引)                    │
│           │                                                        │
└───────────┴────────────────────────────────────────────────────────┘
```

**新增的只有 4 块**（其余全是复用）：
1. **ModelCell 表 + Provenance 表**（Postgres，Alembic migration）
2. **Recipe Engine**（Python 执行器，DAG 解释）
3. **8 种 Step Executor**（薄封装，底层调现有 tools）
4. **前端三视图**（Spreadsheet / Canvas / Inspector）

---

## 4. 核心数据模型

### 4.1 `ModelCell` —— 一切的原子单元

每个数字、每个公式、每个假设都是一个 `ModelCell`。对应 Excel 里一格。

```python
# backend/app/models/revenue_model.py （新建）

class RevenueModel(Base):
    """一次对某公司某期间的收入拆分建模。"""
    id: UUID
    ticker: str                     # "LITE.US" / "AXTI.US"
    company_name: str
    industry: str                   # 驱动用哪个 playbook & recipe
    fiscal_periods: list[str]       # ["FY24", "FY25", "FY26"] or ["26Q1",...]
    recipe_id: UUID                 # 跑哪个 Recipe 生成的
    recipe_version: int
    status: str                     # draft / running / ready / archived
    conversation_id: UUID | None    # 挂到 chat，可追溯创建上下文
    owner_user_id: UUID
    created_at, updated_at

class ModelCell(Base):
    """一个建模单元格。"""
    id: UUID
    model_id: UUID (FK RevenueModel)
    path: str                       # "segment.HDD.Nearline.volume.FY26" (点分层级)
    label: str                      # "HDD 近线存储出货量 FY26"
    period: str                     # "FY26"
    unit: str                       # "EB" / "亿美元" / "%" / "美元/EB"
    value: float | None             # 最终显示值（公式单元格计算后也存在这里）
    formula: str | None             # "=volume * asp"；None 表示硬编码
    depends_on: list[str]           # 被引用的 cell.path 列表（公式单元格）
    value_type: str                 # number / percent / currency / count
    source_type: str                # guidance / expert / historical / inferred / assumption / derived
    confidence: str                 # HIGH / MEDIUM / LOW
    confidence_reason: str          # "3 个来源一致" / "仅 1 专家口径" / "LLM 外推"
    citations: list[dict]           # [{index, source_id, url, title, snippet, date, tool, page}]
    notes: str                      # 自然语言批注：管理层指引原文、专家口径差异等
    alternative_values: list[dict]  # 并列来源（"久谦专家 60 万，自己专家 20 万"）
    provenance_trace_id: UUID       # 指向 ProvenanceTrace —— 完整 agent 推理链
    locked_by_human: bool           # 研究员是否手动锁定（锁定后 re-run 不覆盖）
    human_override: bool            # 是否被人工改写过（进化训练数据）
    review_status: str              # pending / approved / flagged
    created_at, updated_at
    # index: (model_id, path) unique

class ProvenanceTrace(Base):
    """一个单元格是怎么来的 —— Agent 从搜索 / 抽取 / 验证到最终值的完整链路。"""
    id: UUID
    cell_id: UUID
    steps: JSONB                    # list of {step_type, tool, query, result_preview, llm_reasoning, tokens, latency}
    raw_evidence: JSONB             # 原始证据快照（防止源文档后期变化）
    created_at

class ModelCellVersion(Base):
    """所有 ModelCell 变更历史（人工 / agent）。"""
    id: UUID
    cell_id: UUID
    value: float | None
    formula: str | None
    source_type: str
    edited_by: UUID | None          # user_id 或 null=agent
    edit_reason: str
    created_at
```

**关键设计决定**：
- **path 用点分层级（而非 row/col）**：非结构化 UI 层自己决定怎么摆成 Excel。这样不同行业的拆分结构（HDD 按 Nearline/Other，光模块按 400G/800G/1.6T）都能装。
- **公式存字符串 + 解析后的 AST 双写**：字符串便于研究员查看编辑，AST 便于引擎执行和循环检测。见 §4.3 生产级公式引擎。
- **alternative_values 并列存**：研究员能同时看到"久谦专家 / 自己专家 / 管理层指引"三个来源，主值采纳哪个显式选择。这是 Excel 没有的结构化进步。
- **所有行业无差别使用同一套 `ModelCell` schema**：行业差异完全在 Recipe 图、Step prompt 模板、Playbook markdown 里，**绝不出现 `if industry == "optical" ...` 的代码分支**（防止第二个、第三个行业接入时改骨架）。

### 4.3 生产级公式引擎

**不是简化版 DSL。**用户明确要求"完全可用的生产级版本"，因此我们选择 `formulas`（Python 库，覆盖 ~300 个 Excel 函数）+ 自研循环检测层。对比评估：

| 方案 | 函数覆盖 | 性能 | 循环依赖检测 | 维护负担 | 决定 |
|---|---|---|---|---|---|
| 自写简化 DSL（+−×÷、SUM、IF） | ~10 | 极快 | 手写 | 低 | ❌ 用户拒绝 |
| `xlcalculator` | ~200 | 中 | 内置 | 中 | 备选 |
| **`formulas`（PyPI）** | **~300+** | **中快** | **可加** | **中** | **✅ 选它** |
| `pycel` | ~180 | 快 | 内置 | 中 | 备选 |
| 全量 LibreOffice headless | 1000+ | 慢（启进程） | 内置 | 高 | ❌ 过重 |

**`formulas` 库支持的 Excel 函数**（覆盖 98% 研究员日常场景）：
- 算术：`+ - * / ^ %`、括号
- 统计：`SUM, AVERAGE, MEDIAN, STDEV, VAR, COUNT, COUNTIF, SUMIF, SUMIFS, AVERAGEIFS, MIN, MAX, PERCENTILE, RANK`
- 逻辑：`IF, IFS, AND, OR, NOT, XOR, IFERROR, IFNA, SWITCH`
- 查找：`VLOOKUP, HLOOKUP, INDEX, MATCH, XLOOKUP, CHOOSE, OFFSET`
- 数学：`ABS, ROUND, ROUNDUP, ROUNDDOWN, CEILING, FLOOR, MOD, POWER, SQRT, LOG, LN, EXP`
- 文本：`CONCAT, LEFT, RIGHT, MID, LEN, FIND, SEARCH, SUBSTITUTE, TEXT, VALUE, TRIM, UPPER, LOWER`
- 日期：`DATE, YEAR, MONTH, DAY, EDATE, EOMONTH, DATEDIF, WORKDAY, YEARFRAC`
- 金融：`PV, FV, NPV, IRR, PMT, RATE` （研究员做贴现时会用到）
- 数组：`SUMPRODUCT, MMULT, TRANSPOSE`
- 条件聚合：`SUMIFS, COUNTIFS, MAXIFS, MINIFS`

**自研的增量**（因为 `formulas` 默认处理 A1 格引用，我们是 path 引用）：
1. **Path → cell 解析器**：`=segment.HDD.rev.FY26 * segment.HDD.margin.FY26` 在传入 `formulas` 前先转成虚拟 A1 格引用，计算完再映射回 path。
2. **循环依赖检测**：Tarjan SCC 算法在 `depends_on` 图上跑，编辑前预检，UI 提示"A 依赖 B，B 依赖 A"。
3. **脏位传播 & 惰性重算**：改一个 cell，只重算其传递依赖下游，不全图重算。
4. **并行化**：独立子图 `asyncio.gather` 并发评估（重要：一个行业模型可能有 500-2000 cell）。
5. **NaN / 错误传播语义**：任何上游 `#N/A`、`#REF!`、`#DIV/0!` 下游标注同样错误，UI 染红。
6. **单位一致性检查**（非 Excel 功能但金融必备）：`=$ * %` 合法，`=$ + %` 报错；见 §7.3 sanity 层。

**实现文件**：
```
backend/app/services/formula_engine/
    __init__.py
    parser.py          # Path → AST，包装 formulas.Parser
    graph.py           # 依赖图 + Tarjan 循环检测
    evaluator.py       # 异步批量求值，脏位传播
    unit_checker.py    # 单位语义层
    functions_extra.py # 行业特定函数（e.g. CAGR, YoY, 调用 kb_search 做历史数据对比）
    tests/             # 1000+ 测试，对比 Excel 真实计算结果
```

**验收标准**（生产级意味着）：
- 1000 个 Excel 公式 golden-file 测试，与 openpyxl/LibreOffice 对比，匹配率 ≥ 99%
- 2000 cell 的大模型全量重算 P95 < 500ms
- 循环依赖 100% 检出，错误消息定位到环中每个 cell
- 前端编辑 cell 时 < 100ms 反馈（局部重算）

### 4.2 `Recipe` —— 工作流定义

```python
# backend/app/models/recipe.py （新建）

class Recipe(Base):
    """一个可复用、可编辑的建模工作流。"""
    id: UUID
    name: str                       # "光通信公司收入拆分"
    slug: str                       # url-friendly
    industry: str | None            # semiconductors / optical / software / HDD
    description: str                # 给研究员看的说明
    graph: JSONB                    # DAG: {"nodes": [...], "edges": [...]}
    version: int
    is_public: bool                 # 团队共享 / 个人私有
    parent_recipe_id: UUID | None   # fork 自哪个
    created_by: UUID
    # 当前 active 版本 + 所有历史版本都在同张表（version 区分）
```

`graph.nodes[i]` 格式（8 种 step type 详见 §6）：

```json
{
  "id": "step_7",
  "type": "EXTRACT_STEP",
  "label": "从 FY24 年报提取分业务收入",
  "config": {
    "query_template": "{ticker} 分业务收入 FY{year} 10-K segment",
    "target_paths": ["segment.HDD.rev.FY24", "segment.non_HDD.rev.FY24"],
    "source_filter": {"doc_type": ["10-K", "annual_report"], "ticker": "{ticker}"},
    "required_fields": ["amount", "currency", "segment_name"],
    "verification_threshold": 0.7
  },
  "next_on_success": "step_8",
  "next_on_fail": "step_7b_ask_human"
}
```

研究员在 Canvas 上编辑的就是这个 graph。

---

### 4.4 行业插件包（Industry Pack） —— 可扩展性的落地

为防止"第二个行业接入时改骨架代码"，所有行业差异点外化到 `industry_packs/`：

```
industry_packs/
├── __init__.py                       # PackRegistry，运行时动态发现
├── base_pack.py                      # IndustryPack 抽象基类
├── optical_modules/                  # Phase 0 首发
│   ├── __init__.py
│   ├── pack.yaml                     # 元数据：name, ticker patterns, periods, unit hints
│   ├── segments_schema.yaml          # 业务拆分骨架（400G/800G/1.6T/EML/CW/OCS/CPO/DCI...）
│   ├── recipes/
│   │   ├── standard_v1.json          # 标准建模 Recipe
│   │   └── expert_heavy_v1.json      # 重专家铆定版
│   ├── playbook/
│   │   ├── overview.md
│   │   ├── lessons.md                # 初始种子 lesson（从 Excel 样本反推）
│   │   ├── rules.md
│   │   └── peer_comps.yaml           # LITE/Innolight/Coherent/等同业
│   ├── sanity_rules.yaml             # 行业专属数值合理区间
│   │                                  # e.g. optical_module_gross_margin ∈ [0.25, 0.55]
│   └── formulas_extra.py             # 光通信特有函数（CAGR_optical, etc.）
├── semiconductors/                    # Phase 2 扩展
├── HDD_SSD/                           # Phase 2 扩展
└── software_saas/                     # Phase 3 扩展
```

**PackRegistry** 在启动时扫描并加载；每个 Recipe 声明所属 pack；执行时：
- `GATHER_CONTEXT` step 把 `pack/overview.md` + `playbook/*.md` 片段注入 LLM system prompt
- `DECOMPOSE_SEGMENTS` step 以 `segments_schema.yaml` 作为骨架强约束
- `model_sanity.py` 加载 `sanity_rules.yaml`
- 公式引擎合并 `formulas_extra.py` 注册的自定义函数

**接入第 N 个行业的成本**：写 1 个 pack 目录，0 行骨架代码变更。这是"生产级可扩展"的核心保证。

---

## 5. Recipe Engine（工作流执行器）

### 5.1 设计原则
- **不重新发明调度器**：用 Python async + 手写 DAG 遍历，不引 Airflow / Prefect。步骤之间通过 ModelCell 读写通信（类似 GitHub Actions 的 output）。
- **一切步骤最终落地到 ModelCell 的写入**：这样前端始终能从"有哪些 cell"反推"建模到哪一步了"。
- **所有步骤都流式**：通过 SSE 复用现有 chat 的推送通道（只是新事件类型 `cell_update` / `step_progress`）。
- **步骤可暂停、可断点续跑**：`RecipeRun` 表里记录当前 step_id，服务重启也能继续。

```python
# backend/app/services/recipe_engine.py （新建）

class RecipeRun(Base):
    id: UUID
    recipe_id: UUID
    recipe_version: int
    model_id: UUID (FK RevenueModel)
    ticker: str
    status: str                     # pending / running / paused_for_human / completed / failed
    current_step_id: str | None
    step_results: JSONB             # {step_id: {started_at, ended_at, status, output_paths, error}}
    total_tokens: int
    total_cost_usd: float
    created_at, updated_at

async def run_recipe(run_id: UUID, resume: bool = False) -> None:
    """
    1. 载入 Recipe graph
    2. 拓扑序遍历；每个节点：
       (a) 从 ModelCell 读依赖
       (b) 调用对应 StepExecutor
       (c) 写回 ModelCell（含 provenance_trace）
       (d) 发 SSE: step_progress / cell_update
       (e) 出错 → 按 next_on_fail 分支或暂停为 paused_for_human
    3. 全跑完 → 触发 Verification Pass（§7）
    4. 人工校对完成 → 归档，喂给 Feedback loop（§8）
    """
```

### 5.2 对比 n8n / Dify / Flowise
- **n8n / Zapier 风格**（通用自动化）：太细粒度，研究员看到 HTTP node / IF node 会懵。
- **Dify / Flowise**（LLM-native）：更接近我们要的，但它们的 node 还是"LLM 节点 / 工具节点"级别，对研究员来说还是技术词。
- **我们的 Step 抽象更贴业务语义**：节点叫"提取分业务收入 / 专家对价格的判断 / 核验 YoY / 填充余下年份"—— 研究员直接读得懂。研究员可以改的是每个节点的 **prompt template** 和 **source_filter**，不是管线结构本身（等他们熟悉了再开放）。

---

## 6. 8 种 Step Type（覆盖研究员 90% 工作流）

| # | Step Type | 作用 | 底层调用 | 输出 |
|---|---|---|---|---|
| 1 | `GATHER_CONTEXT` | 读业绩会纪要 + 年报，生成公司基本面摘要 | kb_vector_query + jinmen + alphapai | 写入 `company.overview.*` 若干 cell |
| 2 | `DECOMPOSE_SEGMENTS` | 让 LLM 依据管理层指引框架，拆分业务部门结构 | chat_llm with reflection | 生成 cell path 骨架（只占位、不填数） |
| 3 | `CLASSIFY_GROWTH_PROFILE` | 每个业务部门归类：stable / declining / high-growth / new | chat_llm | 写入 `segment.*.growth_profile` cell |
| 4 | `EXTRACT_HISTORICAL` | 从年报/10-K 提取过去 3–5 年各业务收入历史值 | kb_vector_query + CoVe | 写入 `segment.*.rev.FYxx`（historical） |
| 5 | `MODEL_VOLUME_PRICE` | 对高成长业务做量×价拆分（专家口径 / 外部铆定） | jinmen + alphapai 专家访谈 | 写入 `segment.*.volume.*` 和 `segment.*.asp.*` |
| 6 | `APPLY_GUIDANCE` | 对稳定业务直接套管理层指引或 3–5% 规则 | chat_llm + recent earnings call | 写入 `segment.*.rev.FY2X+` |
| 7 | `MARGIN_CASCADE` | 从 GM → Opex → NM → NI → EPS → PE 一路算下来 | chat_llm + peer comp | 写入 `margin.*`, `ni`, `eps`, `pe` |
| 8 | `VERIFY_AND_ASK` | 触发 CoVe（§7），对低置信度 cell 打 `flag`，等研究员处理 | chat_llm (独立 verifier 实例) | 更新 `confidence / review_status` |

**每个 Step 的 prompt template 都是研究员可编辑的**（存在 Recipe.graph.nodes[i].config.prompt_template），带几个占位符（`{ticker}`, `{period}`, `{segment}`），研究员在 Canvas 上改 → 立即以该 Recipe 的下一 run 生效。

**映射到研究员自己的描述**：
- "先读业绩会纪要建立基本了解" → Step 1 `GATHER_CONTEXT`
- "对每个业务部门建模" → Step 2+3 `DECOMPOSE_SEGMENTS` + `CLASSIFY_GROWTH_PROFILE`
- "建模方式参考专家访谈纪要" → Step 5 `MODEL_VOLUME_PRICE`（调 jinmen/alphapai 工具，搜的是对应公司/业务的专家访谈）
- "不足的就接着做访谈" → `VERIFY_AND_ASK`（Step 8）触发 `ASK_HUMAN` 分支，待研究员补充后恢复
- "稳定业务填 3-5% 或管理层指引" → Step 6 `APPLY_GUIDANCE`（内置该分支逻辑）
- "margin 参考管理层指引框架或可类比成熟公司" → Step 7 `MARGIN_CASCADE`（内置两个子策略：`from_guidance` 或 `from_peer_comp`）
- "涨价 vs 增量" → Step 5 产出 `price_driver` 和 `volume_driver` 两个 meta cell，Step 7 会读它们决定 margin 曲线（纯涨价 → 增量几乎全变毛利）

---

## 7. 多步验证与降幻觉（满足需求 2 & 4）

### 7.1 三层降幻觉架构

**Layer 1 — Chain-of-Verification (CoVe)** ：
每次 Agent 决定往一个 cell 写入值，经过 4 阶：

```
  Draft     →   Plan                →   Verify Independently    →  Finalize
  生成候选值      生成 N 个核验问题         每个问题独立检索+答         合成 + 置信度
  (Opus 起草)   ("这个数是不是来自       (用全新 context，禁看       (用核验结果校正 draft，
                Q3 earnings call?")   原 draft)                   打 confidence 标签)
```

**Layer 2 — 跨模型 Debate（生产版新增）**：
对**关键 cell**（`value_type ∈ {revenue, eps, pe}` 或研究员标记 `critical=True`），再走一轮三方辩论：
- **Opus 4.7** 出 draft value（建模主力，1M context 能塞整份 10-K + 业绩会）
- **Gemini 3.1 Pro** 独立 verify（原生 Google 搜索能抓外部源，独立取证）
- **GPT-5.4** 作 tiebreaker —— 当 Opus 与 Gemini 差异 > 10% 时，由 GPT-5.4 读两家论证 + 重新取证，给出裁决
- 所有三家的输出都存到 `cell.alternative_values`，UI 显式展示"三家模型各自认为什么值、各自引用什么源"

**Layer 3 — 数值健全性（非 LLM 层，决定性）**：见 §7.3。

三层防线联合命中率（内部测试目标）：幻觉率 < 2%（一个 2000-cell 模型期望最多 40 个可疑 cell，且会被 flag 出来供研究员抽检）。

**Chain-of-Verification 实现细节**：
- `verification_agent.py`：独立 httpx client pool（不共享 chat_llm 的，避免 context 泄漏）
- 独立起一个 LLM conversation（**严格不带 draft context**，工具只给 `kb_search` + `web_search`）
- 如果 verifier 得出的数字 vs. draft 差异 > 10%，cell 标记 `flagged`，并写 alternative_values
- verifier 返回 HIGH/MEDIUM/LOW 置信度，写到 cell.confidence

### 7.1b Debate 协调器

```python
# backend/app/services/debate_coordinator.py
async def debate_cell(cell_id: UUID, draft: Decimal, draft_sources: list, draft_model: str):
    """
    输入 Opus 的 draft，让 Gemini 独立建模 → 若差异 > 10%，交给 GPT tiebreaker
    返回：(final_value, confidence, all_three_opinions)
    """
    opus_opinion = {"value": draft, "model": draft_model, "sources": draft_sources}
    gemini_opinion = await run_independent_verifier("google/gemini-3.1-pro-preview", cell_id)
    
    diff = abs(gemini_opinion["value"] - opus_opinion["value"]) / max(abs(opus_opinion["value"]), 1e-9)
    if diff < 0.10:
        return (opus_opinion["value"], "HIGH", [opus_opinion, gemini_opinion])
    
    # 调 GPT 作 tiebreaker
    gpt_opinion = await run_tiebreaker(
        "openai/gpt-5.4",
        cell_id,
        context_opinions=[opus_opinion, gemini_opinion],
    )
    # tiebreaker 返回的 value 为 final，confidence 由它主观评估
    return (gpt_opinion["value"], gpt_opinion["confidence"], [opus_opinion, gemini_opinion, gpt_opinion])
```

### 7.2 辅助：**跨源一致性检查**

每个 cell 的 citations 至少来自 2 个独立来源（业绩会纪要 + 专家 / 10-K + 研报），才打 `HIGH`；只有 1 个来源 → `MEDIUM`；纯 LLM 外推 / 推导 → `LOW`。

### 7.3 **数值健全性检查** (决定性的非 LLM 层)

```python
# backend/app/services/model_sanity.py
def check_model(model_id, industry_pack: IndustryPack) -> list[SanityIssue]:
    - 单位一致性（$ vs %, EB vs 亿美元 不可混运算）— 单位 DAG 验证
    - Sum check（各业务 rev 之和 = total rev，差异 > 0.5% 报警）
    - YoY 合理区间（从 pack.sanity_rules.yaml 读，光通信 ASP YoY ∈ [-20%, +30%]）
    - Margin 上下限（从 pack 读行业区间，光通信 GM ∈ [25%, 55%]）
    - Period 单调性（FY24 > FY25 > FY26 → 触发审阅 除非显式标"衰退"）
    - 循环依赖（Tarjan SCC 检测，cell.formula 改写时预检）
    - NaN / DIV0 传播（任何一个上游错误染红全部下游）
    - Outlier 检测（z-score > 3 的 cell 相对其 peer 同期 → 标 ⚠️）
    - Peer sanity（对比 peer_comps.yaml 的同期同指标，> 2 σ 提示）
```

**输出**：每个 SanityIssue 带 `severity` (info/warn/error)、`cell_paths`、`message`、`suggested_fix`。error 级阻塞 RecipeRun 完成；warn 级展示给研究员。

**三层防线联合命中**：
- Layer 1 CoVe —— 抓事实错误（agent 引错数字）
- Layer 2 三方 Debate —— 抓推理错误（agent 对数字解读错）
- Layer 3 Sanity —— 抓运算/一致性错误（公式链路 bug、单位混用）

### 7.4 满足"每个数字都有来源"

**强约束**：Step 执行器写 cell 时，`source_type ∈ {historical, guidance, expert}` 必须带 `citations ≥ 1` 非空；`inferred / derived` 必须带 `depends_on`；`assumption` 必须带 `notes` 说明来由。 DB 层加 CHECK 约束，API 层 pydantic 校验。

---

## 8. 自我进化：Lessons-driven Playbook（满足需求 3）

### 8.1 反馈采集点（即"哪些操作算用户反馈"）

| 反馈信号 | 强度 | 进化动作 |
|---|---|---|
| 研究员**编辑一个 cell 的值** | 极强 | 记录 (原值, 新值, 差异, cell.path, 上下文) |
| 研究员**改 source_type 标签** | 强 | 记录系统判断 vs. 人工判断的分歧 |
| 研究员**点 "👍 正确 / 👎 错误"** | 强 | 直接训练信号 |
| 研究员**改 Recipe 某 step 的 prompt** | 极强 | 该 step 的黄金 prompt 更新 |
| 研究员**补充一条 note** | 中 | 该 cell 的知识扩展 |
| 研究员**引用新来源** | 中 | 该来源入团队可信源白名单 |

所有都写 `user_feedback_events` 表（JSONB payload + context + model_cell_id）。

### 8.2 Lessons.md —— 进化的载体

按行业分层，存在团队个人库 + 硬盘：

```
playbooks/
├── _global/
│   ├── hallucination_traps.md      # 跨行业通用幻觉陷阱
│   └── citation_etiquette.md       # 来源引用规范
├── semiconductors/
│   ├── revenue_modeling.md         # 半导体收入建模总原则
│   ├── lessons.md                  # 经验教训（新 lesson 自动 append）
│   └── rules.md                    # 硬规则（"英伟达 GPU 单位用 10万颗 而非 万颗"）
├── optical_modules/
│   ├── lessons.md
│   └── rules.md
└── HDD_SSD/
    └── ...
```

**格式（每条 lesson）**：

```markdown
## L-2026-04-23-017 | HDD Nearline ASP yoy 节奏

**场景**: WDC / STX 收入拆分
**触发**: 研究员将 F18 (FY26 ASP yoy) 从 Agent 生成的 8% 改为 12%

**观察**: Agent 倾向用管理层指引的中位数推 ASP 增长；研究员更倾向看行业 TAM 增速 
         + 管理层"短期新增收入毛利率 50%+" 的隐含涨价力度，合起来 FY26 应更激进 12%。

**新规则**（后续 HDD 建模 ASP 时注入该 playbook）: 
  - HDD Nearline ASP YoY 启动年取 **10–13%**（高于管理层明确给出的区间），
    若业绩会明确提及 "新增收入毛利率 > 50%" 则用上限；
  - 次年降速至 7–9%，第 3 年回落至 3–5%（符合 HDD 代际节奏）。

**来源**: WDC 2026Q1 earnings call | 研究员 yugang 批注 2026-04-20
**支撑 cells**: model_id=..., cell_path="segment.HDD.asp.FY26.yoy"
```

### 8.3 Playbook 进化流程（周度）

```
1) 收集：user_feedback_events（过去 7 天）
2) 蒸馏：LLM 跑一个 "feedback_consolidator" 任务
   input: 这一周所有 feedback + 当前 playbook
   output: 
     (a) 新增 lessons (append 到 lessons.md)
     (b) 升级 rules.md（矛盾规则合并或废弃旧版）
     (c) 建议的 Recipe 节点 prompt 更新（研究员审批后生效）
3) 注入下一轮：
   - Step executor 启动前，先 kb_search 相关 playbook 片段 → system prompt
   - 高频命中的 lesson 自动升级为 Recipe 硬规则
```

**Letta / MemGPT 的核心思想**：markdown 是 source of truth，派生索引（BM25 + dense）用于快速召回。咱们的 Phase B 向量栈天然能做。

### 8.4 重要护栏

- **playbook 不是自动注入所有 context**：会膨胀 tokens。按 cell.path 前缀检索相关 lessons（例：建 HDD ASP cell 时只拉 `HDD_SSD/lessons.md` 中命中 "ASP" 关键词的片段）。
- **进化需要人工审批**：`feedback_consolidator` 产出新 lesson 后写到 `pending_lessons`，研究员（或 boss 角色）审批后才合并入 main。防止 agent 因为少数异常 case 污染 playbook。
- **保留否决权**：研究员可标记一条 lesson `archived`，Agent 不再注入。

---

## 9. 前端（非技术研究员友好）

### 9.1 三个核心视图

#### A. **Model Spreadsheet** —— 主视图，Excel-like

```
┌──────────────────────────────────────────────────────────────────┐
│ Model: LITE | Recipe: 光通信公司 v3 | Status: 🟢 Ready           │
├──────────────────────────────────────────────────────────────────┤
│          │ 25E   │ 26E        │ 27E        │ notes               │
├──────────┼───────┼────────────┼────────────┼─────────────────────┤
│ 400G 出货 │  60  │  20 [E1]   │  10 [E1]   │ 泰国厂产能 [G1]     │
│ 800G 出货 │  80  │ 200-250[E1]│  350 [E2]  │ 26年 200-250 万块   │
│ 1.6T 出货 │   0  │  50  [E2]  │ 250  [E1]  │                     │
│ 400G ASP │ 200  │ 200        │ 180        │ ⚠️ 两位专家有分歧   │
│ ...                                                                │
│                                                                    │
│  Legend: 🟢 HIGH 置信  🟡 MEDIUM  🔴 LOW  ✏️ 人工改过  🤖 Agent   │
└──────────────────────────────────────────────────────────────────┘
```

- 每个单元格显示：**数值** + **微型置信度圆点** + **[N]** 来源索引 + 小图标标识 source_type
- 点单元格 → 右侧打开 **Cell Inspector**
- 编辑单元格 → 如果是公式 cell 只读（会提示"此为公式，改上游"），硬编码值可改，改完自动级联重算下游
- **颜色编码沿用 Excel 原惯例**：黄色 = assumption，蓝色 = guidance，绿色 = historical，灰色 = derived

#### B. **Cell Inspector** —— 右侧抽屉

```
┌─────────────────────────────────────────┐
│ Cell: segment.EML_CW.100G.volume.FY26  │
├─────────────────────────────────────────┤
│ Value: 12,000 万颗          Confidence: 🟡 MEDIUM │
│ Source Type: expert         Unit: 万颗              │
│                                          │
│ 📚 Sources (3)                           │
│  [E1] 久谦的专家 2026-04-15 "EML 25/26/27 出货…"  │
│  [E2] 我们自己的专家 2026-04-10 "EML 今年产能…"   │
│  [G1] LITE Q3 FY2026 earnings call 管理层指引     │
│                                          │
│ 🔀 Alternative values                    │
│  • 8,000 万颗 (E2 口径) │  选为主值 ↩   │
│  • 15,000 万颗 (E1 口径) │ 选为主值 ↩  │
│                                          │
│ 🧠 Agent reasoning (8 steps)             │
│  1. Searched jinmen '久谦 光模块 EML 2026' (8 hits)│
│  2. Searched alphapai '光模块专家 25Q4' (12 hits)   │
│  ... [展开全部]                          │
│                                          │
│ 📝 Notes                                 │
│  E1 与 E2 口径差异较大。E1 口径包括 CW…  │
│                                          │
│ [✏️ Override value]  [👎 Flag wrong]   │
└─────────────────────────────────────────┘
```

- 这里是研究员信任系统的核心。**每一步 agent 做什么、看了什么、得出什么**都看得到。
- "选为主值 ↩" 按钮触发人工覆盖 → 记录 feedback event → 下游级联 recompute。
- **源文档一键并排查看**（按 Bloomberg 那个模式）：点 `[E1]` 直接打开一个侧边 panel 显示那条纪要的原文 + 高亮被 agent 抽取的片段。

#### C. **Recipe Canvas** —— 研究员改工作流

```
┌────────────────────────────────────────────────────────────────┐
│ Recipe: 光通信公司收入拆分 v3                                   │
│ [+ Fork] [▶ Run on 新 ticker] [💾 Save]                       │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌──────────────┐   ┌─────────────────┐   ┌───────────────┐  │
│   │ 1. 读业绩会  │──▶│ 2. 拆分业务部门 │──▶│ 3. 归类增长型 │  │
│   └──────────────┘   └─────────────────┘   └──────┬────────┘  │
│                                                    │           │
│                     ┌─────────────────┐           │           │
│                     │ 4. 提取历史收入 │◀──────────┘           │
│                     └────────┬────────┘                        │
│                              │                                 │
│               ┌──────────────┼───────────────┐                │
│               ▼              ▼               ▼                │
│       ┌───────────────┐ ┌─────────┐ ┌───────────────────┐    │
│       │ 5a. 量×价拆分 │ │ 5b.应用 │ │ 5c. 问专家(分支) │    │
│       │  [高成长]     │ │ guidance│ │                   │    │
│       └───────┬───────┘ └────┬────┘ └────────┬──────────┘    │
│               └────────────┬─┴─────────────┘                  │
│                            ▼                                   │
│                    ┌───────────────┐                          │
│                    │ 6. Margin 级联│                          │
│                    └───────┬───────┘                          │
│                            ▼                                   │
│                    ┌───────────────┐                          │
│                    │ 7. CoVe 核验  │                          │
│                    └───────────────┘                          │
└────────────────────────────────────────────────────────────────┘

[点击节点 5a 打开编辑器]
┌────────────────────────────────────────────────────────────────┐
│  Step 5a: 量×价拆分（高成长业务）                              │
├────────────────────────────────────────────────────────────────┤
│  Prompt 模板 (你可修改):                                       │
│  ┌────────────────────────────────────────────────────────────┐│
│  │ 你是光通信行业研究员。针对 {ticker} 的 {segment}          ││
│  │ 业务在 {periods} 期间：                                    ││
│  │ 1) 先从业绩会 / 10-K 查是否有量和价的分拆口径              ││
│  │ 2) 用 jinmen_search / alphapai_recall 拉 2 位以上专家口径  ││
│  │ 3) 分别给出 volume 和 ASP 的值，优先引专家，指引作 sanity  ││
│  │ 4) 对 volume 做外部铆定（如果是AI相关，铆TPU/GPU增速…）   ││
│  └────────────────────────────────────────────────────────────┘│
│                                                                 │
│  工具开关: [✓] jinmen [✓] alphapai [✓] web_search [ ] user_kb  │
│                                                                 │
│  置信度阈值: 🟡 MEDIUM (低于此值进入 "asp_human" 分支)         │
│                                                                 │
│  下一步（成功 → step_6, 失败 → step_5c_ask_human）              │
│                                                                 │
│  [💾 保存]  [🧪 试跑]  [↶ 放弃变更]                             │
└────────────────────────────────────────────────────────────────┘
```

**三个给研究员的杠杆**（不需要懂代码）：
1. **改 prompt 模板**（自然语言）—— 决定这一步 Agent 怎么想
2. **打开/关闭工具**（checkbox）—— 决定这一步 Agent 找哪些源
3. **调整置信度阈值**（滑块）—— 决定什么时候卡人工

**不给研究员的**（避免复杂度炸弹）：
- 不改拓扑结构（连线加减 MVP 只给 admin）
- 不改 output cell.path（由 step type 决定）
- 不写 Python

### 9.2 SSE 事件扩展

在现有事件类型基础上新增：

| Event | Payload | 触发时机 |
|---|---|---|
| `step_started` | `{step_id, label, started_at}` | 每个 Recipe step 开始 |
| `step_progress` | `{step_id, tool_call, tool_args}` | Step 内部调工具 |
| `step_completed` | `{step_id, output_paths, confidence}` | Step 结束 |
| `cell_update` | `{path, value, source_type, confidence, citations}` | 每个 cell 被写或改 |
| `verify_flag` | `{cell_path, reason, alternatives}` | CoVe 核验打 flag |
| `run_paused` | `{reason, awaiting}` | 等待人工（e.g. 专家口径分歧） |

复用现有 SSE 通道（`/api/chat/conversations/{id}/messages/stream` 以及在它上面新加一个 `/api/models/{id}/run/stream` 专用 endpoint）。

### 9.3 交付形态
- 新增页面 `frontend/src/pages/RevenueModel.tsx`（主 Spreadsheet + Inspector）
- 新增页面 `frontend/src/pages/RecipeEditor.tsx`（Canvas）
- 复用 `CitationRenderer` / `MarkdownRenderer` 组件
- 导出：**一键导出 xlsx**，保留颜色编码、保留 note 列、保留公式；底层 `openpyxl` + 在 AppLayout 菜单里加 "📥 Export Excel"

---

## 10. 复用现有基础设施清单（精确到文件）

| 既有资产 | 路径 | 复用方式 |
|---|---|---|
| LLM 多轮 tool-calling | `backend/app/services/chat_llm.py:907` `call_model_stream_with_tools` | 直接调；新增一个 `modeling_mode=True` 参数控制 system_prompt 注入 playbook |
| Tool dispatch | `chat_llm.py:965` `dispatch_tool` | 在前缀路由上添加一类 `modeling_*` step，但**尽量不加新 tool**（复用 alphapai/jinmen/kb/user_kb/web_search） |
| Citation 全局索引 | `web_search_tool.py:208` `CitationTracker` | 扩展 `add_modeling_citation(cell_id=..., ...)` 方法；index 体系与 chat 共享 |
| SSE 推送 | `api/chat.py:870-945` | 复制该流式协议；新端点 `/api/models/{id}/stream` |
| Phase B 向量检索 | `kb_vector_query.py` + `kb_service.py` | 建 cell 时的 `search` / `extract` 步都走 hybrid RAG |
| 前端渲染 | `CitationRenderer.tsx` + `MarkdownRenderer.tsx` | 单元格 note / agent reasoning 渲染都用这两个 |
| Spreadsheet 编辑 | `frontend/src/components/SpreadsheetEditor.tsx` | 生产级不够用（研究员模型可达 2000 cell）—— 弃用此组件，**新建 `ModelSpreadsheet.tsx` 基于 `@glideapps/glide-data-grid`**（虚拟滚动、键盘导航、公式提示） |
| WebSocket 进度 | `backend/app/ws/feed.py` | 大 run 失败可走 WS 通知研究员 |

**必须新建的**：
- DB migrations（`revenue_model`, `model_cell`, `model_cell_version`, `provenance_trace`, `recipe`, `recipe_run`, `user_feedback_events`, `debate_opinion`, `sanity_issue`, `pending_lesson`）
- `backend/app/services/formula_engine/`（生产级公式引擎，§4.3，~3000 行含测试）
- `backend/app/services/recipe_engine.py` + `step_executors/` 子包（8 个 executor，每个 200-400 行）
- `backend/app/services/verification_agent.py`（CoVe 独立 verifier）
- `backend/app/services/debate_coordinator.py`（三方 LLM 辩论，§7.1b）
- `backend/app/services/model_sanity.py`（数值健全性 + pack 规则加载）
- `backend/app/services/playbook_service.py`（markdown 读写 + path-prefix 检索）
- `backend/app/services/feedback_consolidator.py`（周度蒸馏 cron）
- `backend/app/services/industry_pack_loader.py`（PackRegistry + hot reload）
- `backend/app/api/revenue_models.py`（REST + SSE）
- `backend/app/api/recipes.py`（CRUD + fork + dry-run）
- `backend/app/api/playbook.py`（读 / 审批 / 归档）
- `frontend/src/pages/RevenueModel.tsx`, `RecipeEditor.tsx`, `PlaybookReview.tsx`, 及其下组件
- `industry_packs/optical_modules/` 全量内容（schema / recipes / playbook / sanity / formulas_extra）

---

## 11. 实施路线图（生产版，~14–18 周，分 6 期）

不做 "MVP 后再补全"，每一期都按生产标准收尾（完整测试、文档、监控）。光通信行业贯穿 Phase 0→5，每期都在光通信上把那一层做到生产可用；多行业扩展在 Phase 5+。

### **Phase 0 —— 共建与奠基（1.5 周）**
- 与 3 位光通信研究员共建，实录 LITE（已有样本）和另 2 家（Innolight / Coherent）建模全过程
- 从三家样本反推 `industry_packs/optical_modules/segments_schema.yaml` 和种子 `lessons.md`
- 固化技术选型：`formulas` 公式库、`reactflow` Canvas、`glide-data-grid` Spreadsheet
- **交付**：`docs/optical_modules_research_playbook.md`（研究员工作流白皮书）+ 技术选型 ADR

### **Phase 1 —— 数据模型与公式引擎（生产级，3 周）**
- DB Migration (Alembic)：`revenue_model`, `model_cell`, `model_cell_version`, `provenance_trace`, `recipe`, `recipe_run`, `user_feedback_events`, `debate_opinion`, `sanity_issue`
- `backend/app/services/formula_engine/` 完整实现：
  - `formulas` 库集成 + path ↔ A1 映射层
  - Tarjan SCC 循环依赖检测
  - 脏位传播 + 惰性重算 + `asyncio.gather` 并行
  - 单位一致性层
  - **1000 公式 golden-file 测试集**，覆盖 §4.3 所有函数族，对比 openpyxl/LibreOffice ≥ 99% 匹配
  - 性能基准：2000-cell 全量重算 P95 < 500ms、局部重算 < 100ms
- `IndustryPack` 抽象 + `optical_modules` 首包填充完成
- **交付**：公式引擎独立库（含 90%+ 单测覆盖）、Postgres schema v2、Industry Pack 加载器

### **Phase 2 —— Recipe Engine + 8 步 executor（3 周）**
- `backend/app/services/recipe_engine.py`：DAG 解释器，拓扑序执行，断点续跑（service 重启能 resume）
- 8 个 `step_executors/`（§6 全量）每个带：
  - 独立 prompt template（存 Industry Pack 内，可热更新）
  - 工具调度（复用 chat_llm.dispatch_tool）
  - 输出 cell 写入 + provenance_trace 记录
  - 单测：mock 工具返回固定数据，断言 cell 写入正确
- Recipe JSON schema（JSON Schema Draft 7，启动时验 recipe graph）
- SSE 事件扩展（新 6 种 event type）
- **交付**：端到端跑通 LITE recipe，写出 50+ cell，全部带 citation，前端通过 SSE 实时可见

### **Phase 3 —— Spreadsheet + Inspector + Provenance UI（3 周）**
- `frontend/src/pages/RevenueModel.tsx` 主页
- `ModelSpreadsheet.tsx`：基于 `glide-data-grid`（高性能，10k+ cell 滚动丝滑）
  - 颜色编码（黄/蓝/绿/灰 对应四种 source_type）
  - 置信度圆点渲染层
  - cell 编辑 → 即时局部重算 → SSE 推送其他客户端
  - 右键菜单：改公式、锁定、复制粘贴（含公式引用相对/绝对）
- `CellInspector.tsx`：抽屉式，展示全部 §9.1B 内容
- `ProvenancePanel.tsx`：源文档并排高亮（像 Bloomberg）
- `ModelDiff.tsx`：模型版本对比（便于与上季度模型对比）
- 可访问性 / i18n / 加载骨架 / 错误边界 —— 生产级前端标配
- **交付**：研究员可独立操作，0 工程师介入，完成一次 LITE 建模并导出 Excel

### **Phase 4 —— Recipe Canvas 编辑器（2 周）**
- `frontend/src/pages/RecipeEditor.tsx` 基于 `reactflow`
- 研究员可操作（三个杠杆）：
  - 改 prompt 模板（带语法高亮 + 变量自动补全：`{ticker}`, `{segment}`, `{period}` 等）
  - 开关工具 checkbox
  - 调置信度阈值滑块
- 节点拓扑结构变更 **Admin Only**（MVP 没开，现在需开但权限门控）
- Fork / Version / Diff：研究员 fork 公共 recipe → 私有版 → 验证 OK 后可 PR 回公共，boss 审批合并
- 试跑模式（dry-run）：选一个 test ticker 单步执行，看每步输出但不写 cell
- **交付**：3 个光通信研究员各自 fork 出适合自己覆盖标的的 recipe 版本

### **Phase 5 —— Verification 三层 + Debate（2.5 周）**
- `verification_agent.py` —— CoVe 独立 verifier（Gemini 3.1 Pro 为主）
- `debate_coordinator.py` —— 关键 cell 三方辩论（§7.1b）
- `model_sanity.py` + `industry_packs/*/sanity_rules.yaml` 驱动
- UI：flagged cell 高亮、debate 三方意见 diff 视图、sanity issue 汇总栏
- **引入"已知错误"测试集**：从历史 6 个月研究员修正过的 cell 构建 benchmark，要求系统对这些 case 召回 ≥ 85%
- 成本控制：Debate 仅对 `critical` cell 跑（rev / eps / pe / 第一级 segment rev），研究员可手动标 critical 让某 cell 也走 debate
- **交付**：每份模型带完整 confidence 分布 + sanity report；研究员抽检结果可自动入 feedback 流

### **Phase 6 —— Playbook 自我进化 + 多行业扩展 + 生产化（3 周）**
- `user_feedback_events` 全量埋点
- `playbooks/` 目录结构 + `playbook_service.py` (读写 + path-prefix 检索)
- `feedback_consolidator.py` + 每周五 23:00 cron (复用 `backtest_scheduler` 模式)
- 审批流 UI：`pending_lessons` 页面 (admin/boss 看得到)；approve/reject/edit 后写入 main
- 归档 / 否决：研究员标 lesson `archived`，agent 不再注入
- **HDD_SSD 和 半导体 Industry Pack** 同步开工（对照 Excel 里 WDC/STX/AXTI 样本反推），验证 0 骨架代码变更即可接入
- 生产化：权限矩阵、审计日志、LLM 成本 dashboard 接入 `/api/analytics`、P0 告警接入飞书
- Excel 导出：颜色 / 公式 / 批注 / 多 sheet (per company) / 源文档超链接回 doc_id
- **交付**：生产上线，2 个以上行业同时可用，每周自动进化

### **Phase 7 —— Scale-out（持续）**
- 更多 Industry Pack（软件 SaaS / 互联网 / 新能源 / 医药）
- Scenario (bull/base/bear) 三情景建模
- Peer comp 自动批量建模
- Alert 耦合（模型 cell 与新出业绩会数据大幅偏差 → 自动通知研究员）

**总周期**：**14–18 周**，由 2 后端 + 1 前端 + 0.5 研究员（共建）承担。

### 质量门（每期通过才进下一期）

每一期结束前必须通过：
- **单元测试覆盖 ≥ 85%** on `backend/app/services/recipe_engine/`, `formula_engine/`, `verification_agent`, `model_sanity`
- **集成测试**：从建新 model → run recipe → verify → human edit → export Excel 的 E2E 跑通 at least 3 家公司
- **文档**：每个新 service 有 docstring 级 API 描述 + docs/ 下使用指南更新
- **研究员冒烟测试**：至少 1 位研究员签字确认该期功能可用

---

## 12. 风险与缓解

| 风险 | 概率 | 影响 | 缓解 |
|---|---|---|---|
| LLM 成本膨胀（一次建模 = 50+ tool call × 3 model） | 高 | 中 | ①默认只跑 1 个主 model（Opus），研究员按需起二审；②cache 同 query（复用现 `SEARCH_CACHE_HIT` 机制）；③每 step 的 tool_rounds 上限从现在的 5 提到 8 但按 cell 组批处理 |
| 研究员不愿用、回到 Excel | 中 | 高 | Phase 0 就拉 2-3 个研究员共建，不搞闭门造车；Excel 导出一键同步（关键）；研究员看得见 Agent 怎么推理才愿意信 |
| 幻觉：Agent 编造一个 citation（给了 URL 但内容不符） | 中 | 极高 | §7.1 CoVe + §7.2 跨源一致 + 每个 cite 存 snippet 快照，人工抽检时能一秒识破 |
| Playbook 被少数异常 case 污染 | 低 | 高 | §8.4 pending_lessons 需审批 + 可 archive；consolidator prompt 里显式要求"至少 3 次一致反馈才升级为 rule" |
| Recipe 拓扑被研究员改坏 | 低 | 中 | MVP 只开 prompt / tool / threshold 3 个杠杆，不开拓扑；拓扑改动需 admin |
| Phase B 向量库未覆盖某公司 | 中 | 中 | Fallback 到 web_search；新增"补录 KB"按钮（研究员上传 PDF 后 trigger 现有 `user_kb` 流水线） |
| 公式评估 DSL bug 导致级联错乱 | 中 | 高 | 简单 DSL + 强测试覆盖；对比 Excel 交叉校验 |

---

## 13. 已确认的决策（锁定版）

用户已于 2026-04-23 确认：

| # | 决策 | 选择 | 理由 |
|---|---|---|---|
| 1 | **首发行业** | 光通信；全栈多行业可扩展 | Excel 样本丰富；Industry Pack 架构保证 0 骨架改动接下一个行业 |
| 2 | **Recipe 存储** | **JSON 存 Postgres JSONB + 版本号** | 支持运行时修改、API 编辑、diff；Industry Pack 里的种子 recipe 以 JSON 打包 |
| 3 | **公式复杂度** | **生产级，Excel 兼容**（`formulas` 库 + 自研扩展，§4.3） | 用户明确要求；覆盖 300+ 函数 ≈ 研究员 98% 使用场景 |
| 4 | **Playbook 审批** | **每条 lesson 需 admin/boss 审批** | 防止少数异常 case 污染 playbook；研究员 trust 很重要 |
| 5 | **LLM 选型** | **Opus 4.7 主建模 + Gemini 3.1 Pro verifier + GPT-5.4 tiebreaker** | 三方辩论抗幻觉；用户已拍板 |
| 6 | **进化频率** | **每周五 23:00 cron 蒸馏** | 噪声少、审批负担合理 |
| 7 | **UI 选型** | **Spreadsheet: `@glideapps/glide-data-grid`** (10k+ cell 高性能滚动)；**Canvas: `reactflow`**（生态成熟） | 生产级前端需要高性能 grid；现有 `SpreadsheetEditor.tsx` 太轻量不够用 |

### 未决待 Phase 0 共建中确认的细节
- **LITE 之外的 Phase 0 陪跑公司**（建议 Innolight + Coherent/II-VI，需研究员确认）
- **第一批接入行业（Phase 6）**：HDD/SSD 和 半导体哪个先？建议 HDD/SSD（Excel 样本更完整）
- **Debate 必跑范围**：默认 rev/eps/pe + 第一层 segment rev；研究员可否改阈值？建议可配置，default 锁 7-8 个关键 path

---

## 14. 后续扩展（Out of scope for now，但设计上已预留）

- **Scenario / Sensitivity 分析**：基于同一 ModelCell 图，跑 bull / base / bear 三情景 → 自动生成 tornado 图
- **Peer comp auto-build**：建一家公司时自动拉 3 家同业到隔壁 tab，列 GM / NM / EPS 对比
- **Alert 耦合**：模型里某个 cell 的值与最新业绩会/纪要出现大幅偏差 → `/api/portfolio-news` 里自动提示
- **Model-to-recommendation loop**：PE 算出来后与当前股价对比，自动生成"买/持/卖"倾向（要研究员启用）
- **Multi-company batch run**：组合持仓里所有公司一键全建模，周度 refresh

---

## 附录 A —— 关键代码命名约定（生产版）

```
backend/app/models/
    revenue_model.py          # RevenueModel, ModelCell, ModelCellVersion, ProvenanceTrace
    recipe.py                 # Recipe, RecipeRun
    feedback.py               # UserFeedbackEvent, PendingLesson
    debate.py                 # DebateOpinion
    sanity.py                 # SanityIssue

backend/app/api/
    revenue_models.py         # REST + SSE: /api/models
    recipes.py                # REST: /api/recipes (含 fork, version, dry-run)
    playbook.py               # REST: /api/playbook (read/approve/archive)

backend/app/services/
    formula_engine/
        __init__.py
        parser.py             # Path ↔ A1 映射 + formulas 库包装
        graph.py              # Tarjan SCC 循环检测
        evaluator.py          # 异步批量求值 + 脏位传播
        unit_checker.py       # 单位语义层
        functions_extra.py    # 自定义函数 (CAGR, YoY, 行业扩展)
        tests/
            golden_set.py     # 1000+ Excel 对照测试
            fixtures.xlsx
    recipe_engine.py          # 核心 DAG 执行器
    step_executors/
        __init__.py
        base.py
        gather_context.py
        decompose_segments.py
        classify_growth.py
        extract_historical.py
        model_volume_price.py
        apply_guidance.py
        margin_cascade.py
        verify_and_ask.py
    verification_agent.py     # CoVe 独立 verifier
    debate_coordinator.py     # 三方 LLM 辩论 (Opus/Gemini/GPT)
    model_sanity.py           # 数值健全性 + pack 驱动
    playbook_service.py       # Lessons.md I/O + path-prefix 检索
    feedback_consolidator.py  # 每周五 23:00 cron
    industry_pack_loader.py   # PackRegistry + hot reload
    llm_roles.py              # 三家模型角色封装

backend/app/schemas/
    revenue_model.py
    recipe.py
    playbook.py

industry_packs/
    base_pack.py
    optical_modules/                          # Phase 0/1 首发
        pack.yaml
        segments_schema.yaml
        recipes/
            standard_v1.json
            expert_heavy_v1.json
        playbook/
            overview.md
            lessons.md
            rules.md
            peer_comps.yaml
        sanity_rules.yaml
        formulas_extra.py
    HDD_SSD/                                  # Phase 6
    semiconductors/                           # Phase 6
    software_saas/                            # Phase 7+

frontend/src/
    pages/
        RevenueModel.tsx      # 主模型页面
        RecipeEditor.tsx      # Recipe 画布
        PlaybookReview.tsx    # 审批 pending lessons
    components/modeling/
        ModelSpreadsheet.tsx        # glide-data-grid 包装
        CellInspector.tsx           # 单元格抽屉
        ProvenancePanel.tsx         # 源文档并排
        FormulaBar.tsx              # Excel 风格公式栏
        ConfidenceBadge.tsx
        SourceTypeChip.tsx
        DebateDiffView.tsx          # 三家 LLM 意见对比
        SanityIssueList.tsx
    components/recipe/
        RecipeCanvas.tsx            # reactflow 包装
        StepConfigPanel.tsx         # prompt 编辑
        StepDryRunModal.tsx

backend/alembic/versions/
    {new}_revenue_modeling_core.py            # Phase 1 schema
    {new}_recipe_engine.py                    # Phase 2 schema
    {new}_verification_and_debate.py          # Phase 5 schema
    {new}_playbook_pending_lessons.py         # Phase 6 schema
```

## 附录 B —— 参考资料

- Hebbia Matrix / ISD: https://www.hebbia.com/product
- Bloomberg ASKB roadmap: https://www.bloomberg.com/professional/insights/press-announcement/bloomberg-unveils-askb-roadmap-for-clients-to-augment-their-investment-process-with-agentic-ai/
- Morgan Stanley AskResearchGPT: https://openai.com/index/morgan-stanley/
- Daloopa Skills on Claude MCP: https://daloopa.com/blog/product-updates/run-faster-earnings-analysis-daloopa-skills
- AlphaSense segment breakdown: https://www.alpha-sense.com/resources/product-articles/ai-tools-earnings-analysis/
- Chain-of-Verification (ACL 2024): https://arxiv.org/abs/2309.11495
- Letta / MemGPT agent memory: https://www.letta.com/blog/agent-memory
- Reflection / self-evaluation patterns: https://zylos.ai/research/2026-03-06-ai-agent-reflection-self-evaluation-patterns
- Financial agent multi-agent eval: https://arxiv.org/html/2603.27539v1
- Anthropic financial services plugins: https://github.com/anthropics/financial-services-plugins
