# 光通信建模 — Lessons (经验教训累积)

> 本文档由研究员反馈自动累积，格式：一条 lesson 一个二级标题。
> 新 lesson 通过 PendingLesson 审批后写入。

## L-2026-04-23-001 | 专家与管理层口径不一致时优先级

**场景**: LITE / Coherent 建模时，EML+CW 出货量，管理层给区间，专家给点估

**观察**: 管理层口径偏保守（避免 beat-and-guide 后下修），专家口径靠近实际
（尤其对口供应链的专家）。25-26 年 EML+CW 实际出货通常超管理层区间上限 10-15%。

**新规则**: 
- 高增长业务 (growth_profile = "high-growth"): 专家口径主值，管理层作 sanity
- 稳定业务 (stable / declining): 管理层主值，专家仅作交叉验证
- 若两者差 > 25%，alternative_values 都存，`confidence = MEDIUM`，flag 人工决断

**支撑**: 研究员 yugang 多次在 LITE/AXTI 模型中如此处理

## L-2026-04-23-002 | 纯涨价 vs 增量对营业利润率的传导

**场景**: 模块行业大客户保价，2026 年 100G/200G EML ASP 涨 10-15% 无新增出货

**观察**: 纯涨价时，增量收入的 90%+ 转化为营业利润（无边际成本变动）。
OM 可从 18% 跳升至 28-33%，不能按历史 OM × 新收入简单外推。

**新规则**: MARGIN_CASCADE 执行时读 `price_driver` / `volume_driver` 的相对贡献：
- 若 `price_contribution > 0.7`：margin 曲线按 "increment_to_margin" 处理
  （增量收入的 margin ≈ 90%，在基础 OM 上面累加）
- 若 `volume_contribution > 0.7`：margin 按历史 OM 比例线性
- 介于之间（0.3-0.7）：两端插值

**支撑**: LITE 2026 年业绩会管理层口径 + 专家佐证

## L-2026-04-23-003 | 光模块 volume 铆 AI 基建增速

**场景**: 800G / 1.6T 出货预测

**观察**: 光模块出货与 GPU/TPU 季度出货存在稳定配比。2025 年约为 
`400G + 800G + 1.6T 总量 ≈ (GPU Q出货 × 8) + (TPU Q出货 × 6)`。
纯比例法有 10-15% 误差，但作为 sanity check 很稳。

**新规则**: 在 MODEL_VOLUME_PRICE 的 volume 估计完成后，附加一个 sanity 检查：
- 取 NVIDIA 数据中心季度营收增速作 `gpu_growth_hint`
- 取 GOOG TPU 出货增速作 `tpu_growth_hint`
- 光模块 volume YoY 与 (gpu+tpu) YoY 偏离超过 40%，flag 提醒

**支撑**: 2024-2025 历史回测，LITE/COHR/Innolight 与 NVDA 数据中心营收高相关 (r > 0.85)

## L-2026-04-23-004 | 管理层指引的保守性修正

**场景**: 新业务 (CPO/OCS) 管理层给初始季度指引

**观察**: 新业务首年管理层口径系统性低估（公司避免首年未达预期），
常见向上修正幅度 20-40%。

**新规则**: 对 growth_profile="new" 的 segment：
- FY 首年收入：guidance × 1.2 作为主值，guidance 作为 lower bound
- FY 第二年：guidance × 1.3 - 1.5
- 从第三年起回归 guidance 中位数
- 置信度 MEDIUM，显示说明 "首年 guidance 偏保守，已上修 20%"

**支撑**: LITE OCS 2024-2025 实际超管理层初始口径 ~35%
