# 光通信建模硬规则 (Hard Rules)

> 本文档是 Agent **必须遵守** 的硬约束。Lessons 通过审批后升级为 rule 写入此处。

## R-001 | 量价拆分规则

- `400G / 800G / 1.6T` 光模块：**必须**分别建量与价，不能合并。
- `EML + CW` 光芯片：按 `100G / 200G / CW` 三条子线拆量。
- `OCS / CPO / DCI`：直接建收入，不做量价拆分（管理层不披露颗粒度）。
- `传统通信 / 工业`：直接建收入，growth profile = stable，用 `guidance 或 ±5%`。

## R-002 | 单位规范

- 模块出货：单位 "万块"
- 芯片出货：单位 "万颗"
- 所有收入：单位 "亿美元"
- ASP：模块 "美元/块"，芯片 "美元/颗"
- 股份：单位 "亿股"

## R-003 | Margin 基准

本行业使用 **operating margin (EBIT)** 口径，不是 EBITDA 也不是 gross margin。
理由：管理层 guidance 主要围绕 OM；EBITDA 口径细节差异大难以可比。

## R-004 | 税率

缺省使用 **15%** 有效税率（2024-2025 行业均值）。如公司业绩会明确给出 tax rate
guidance，以管理层口径为准。`ni = ebit × (1 - tax_rate)`。

## R-005 | 来源强约束

- `source_type = historical` 必须带 ≥1 条引用，来源类型应为 10-K / 10-Q / earnings call transcript。
- `source_type = guidance` 必须引用最近一次业绩会或 Investor Day，日期不超过 180 天。
- `source_type = expert` 必须引用至少一位对口专家（Alphapai/Jinmen/Meritco/ThirdBridge 标签含"光模块/光芯片/数通/XX 供应链"）。
- `source_type = assumption` 必须在 `notes` 中说明假设依据与同行可比。

## R-006 | 数值边界

见 `sanity_rules.yaml`；Agent 在写入 cell 前应检查是否 in-range，若越界必须：
1. 在 notes 中写明越界原因
2. 降低 confidence 到 MEDIUM 或 LOW
3. 主动增加 alternative_values 说明可能的其他口径

## R-007 | 多期单调性例外处理

若某 segment 某期明显下滑 (e.g. FY26 800G < FY25 800G)：
- 仅当业绩会或专家明确说明 "客户转移" / "代际替代" / "失单" 可接受
- 否则需要 flag 人工确认
