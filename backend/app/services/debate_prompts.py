"""Domain-specific prompt templates for investment debate mode.

Provides structured, Chain-of-Thought prompts for different debate formats
and topic-aware customization for stock/industry/macro questions.
"""
from __future__ import annotations

import re

# ── Topic detection ──────────────────────────────────────────────

# A-share: 6-digit codes (600xxx, 000xxx, 300xxx, 00xxxx.SZ, etc.)
_RE_A_SHARE = re.compile(r"\b[036]\d{5}\b|\b\d{6}\.(SH|SZ|BJ)\b", re.IGNORECASE)
# HK: 5-digit codes or xxxx.HK
_RE_HK = re.compile(r"\b\d{4,5}\.HK\b", re.IGNORECASE)
# US: 1-5 uppercase letters (AAPL, TSLA, etc.)
_RE_US_TICKER = re.compile(r"\b[A-Z]{1,5}\b")

_MACRO_KEYWORDS = {
    "gdp", "cpi", "ppi", "pmi", "利率", "降息", "加息", "通胀", "通缩",
    "货币政策", "财政政策", "美联储", "fed", "央行", "汇率", "国债",
    "经济增长", "衰退", "recession", "就业", "失业率", "贸易战",
    "关税", "地缘", "宏观", "macro",
}

_INDUSTRY_KEYWORDS = {
    "行业", "板块", "赛道", "产业链", "上下游", "竞争格局",
    "市场份额", "渗透率", "半导体", "新能源", "光伏", "锂电",
    "人工智能", "ai", "芯片", "消费", "医药", "地产", "银行",
    "券商", "保险", "汽车", "军工", "白酒", "互联网", "云计算",
    "机器人", "低空经济", "算力",
}

# Common US tickers to avoid false positives from generic English words
_KNOWN_US_TICKERS = {
    "AAPL", "MSFT", "GOOG", "GOOGL", "AMZN", "META", "TSLA", "NVDA",
    "AMD", "INTC", "NFLX", "BABA", "JD", "PDD", "NIO", "XPEV", "LI",
    "TSM", "AVGO", "CRM", "ORCL", "ADBE", "QCOM", "BIDU", "TCOM",
    "BILI", "IQ", "TME", "WB", "ZH", "FUTU", "TIGR", "SPY", "QQQ",
}


def detect_topic_type(content: str) -> str:
    """Detect topic type from user content using keyword matching.

    Returns: 'stock', 'industry', 'macro', or 'general'
    """
    text_lower = content.lower()

    # Check for specific stock tickers
    if _RE_A_SHARE.search(content) or _RE_HK.search(content):
        return "stock"
    # US tickers: only match known ones to avoid false positives
    words = set(content.split())
    if words & _KNOWN_US_TICKERS:
        return "stock"

    # Check for macro keywords
    if sum(1 for kw in _MACRO_KEYWORDS if kw in text_lower) >= 2:
        return "macro"

    # Check for industry keywords
    if sum(1 for kw in _INDUSTRY_KEYWORDS if kw in text_lower) >= 2:
        return "industry"

    return "general"


# ── Topic-specific context instructions ──────────────────────────

_TOPIC_CONTEXT = {
    "stock": "请围绕该具体标的展开分析，引用财务数据（营收、利润、ROE、现金流）、估值指标（PE/PB/PS及历史分位）、技术面信号和近期催化剂。",
    "industry": "请从行业整体视角分析，关注市场规模与增速、竞争格局、政策环境、技术趋势、产业链上下游关系，以及行业内代表性公司的表现。",
    "macro": "请从宏观经济视角分析，关注核心经济指标（GDP/CPI/PMI等）、货币与财政政策走向、全球经济联动、地缘政治影响，以及对大类资产配置的启示。",
    "general": "",
}


# ── Bull/Bear debate prompts (3-round) ───────────────────────────

def get_bull_bear_prompts(topic_type: str = "general") -> dict[int, str]:
    """Get debate prompts for bull/bear format with topic-aware customization."""
    topic_ctx = _TOPIC_CONTEXT.get(topic_type, "")
    topic_line = f"\n\n**领域背景：**{topic_ctx}" if topic_ctx else ""

    return {
        1: (
            "你是一位顶尖的买方投资分析师，拥有15年A股/港股/美股研究经验。"
            "请对以下投资问题进行深度分析，给出明确的看多立场。"
            f"{topic_line}"
            "\n\n**分析框架要求：**\n"
            "1. 先明确分析的标的/行业及当前市场背景\n"
            "2. 基本面分析：营收增速、利润率趋势、ROE、现金流质量\n"
            "3. 估值分析：PE/PB/PS与历史分位及同业对比\n"
            "4. 催化剂：未来6-12个月可能的股价驱动因素\n"
            "5. 技术面辅助：当前趋势、关键支撑/阻力位（如适用）\n"
            "\n**输出结构：**\n"
            "- **核心观点**（一句话总结）\n"
            "- **详细论证**（分点展开，每个论点附数据或逻辑支撑）\n"
            "- **目标价/预期回报**（如可估算）\n"
            "- **关键假设**（列出你的分析依赖的前提条件）\n"
            "- **信心水平**：高/中/低，并说明原因\n"
            "\n注意：避免泛泛而谈，请给出具体数据点和可验证的论据。"
            "警惕大盘股偏好等认知偏差，对中小盘标的给予同等分析深度。"
        ),
        2: (
            "你是一位资深风险管理专家和卖方分析师。你的职责是严格审视看多方的论证，找出漏洞。"
            "\n\n看多方的分析如下：\n\n{prev_content}"
            "\n\n**反驳框架：**\n"
            "1. 逐一审视看多方的每个核心论据，指出逻辑漏洞或数据偏差\n"
            "2. 提出被忽视的风险因素（行业风险、政策风险、竞争格局变化等）\n"
            "3. 挑战估值假设（增长率是否过于乐观？可比公司是否恰当？）\n"
            "4. 考虑宏观/地缘/流动性等外部风险\n"
            "5. 历史类比：过去类似情况下股价表现如何？\n"
            "\n**注意事项：**\n"
            "- 不要为了反驳而反驳，给出实质性的、有数据支撑的质疑\n"
            "- 如果看多方某个论点确实站得住脚，承认它，但指出其不足\n"
            "- 指出看多方可能存在的认知偏差（锚定效应、确认偏差、幸存者偏差等）\n"
            "\n**信心水平**：你认为看多方论证的可靠程度（高/中/低），并说明理由。"
        ),
        3: (
            "你是一位管理千亿资产的独立首席投资官。你需要综合看多方和质疑方的观点，做出投资决策。"
            "\n\n【看多方分析】\n{round1_content}"
            "\n\n【质疑方反驳】\n{round2_content}"
            "\n\n**综合判断框架：**\n"
            "1. **共识点**：双方都认同的事实和逻辑\n"
            "2. **核心分歧**：双方最关键的分歧点及你的判断\n"
            "3. **信息缺口**：还需要哪些数据/信息才能做出更好的判断\n"
            "4. **投资结论**：\n"
            "   - 总体评级：强烈看多 / 看多 / 中性 / 看空 / 强烈看空\n"
            "   - 信心水平：1-10分\n"
            "   - 时间维度：这个判断适用的时间框架\n"
            "5. **配置建议**：\n"
            "   - 建议仓位占比\n"
            "   - 建仓策略（一次性还是分批？什么价位？）\n"
            "   - 止损位和目标价\n"
            "6. **关键监控指标**：需要持续跟踪的指标和触发重新评估的条件\n"
            "\n请务必给出明确的、可执行的投资建议，而不是模棱两可的结论。"
        ),
    }


# ── Multi-perspective debate prompts ─────────────────────────────

MULTI_PERSPECTIVE_ROLES = {
    1: "基本面分析师",
    2: "情绪与新闻分析师",
    3: "技术面分析师",
    4: "综合判断",
}


def get_multi_perspective_prompts(topic_type: str = "general") -> dict[int, str]:
    """Get prompts for multi-perspective format (3-4 models, parallel + synthesis)."""
    topic_ctx = _TOPIC_CONTEXT.get(topic_type, "")
    topic_line = f"\n\n**领域背景：**{topic_ctx}" if topic_ctx else ""

    return {
        1: (
            "你是一位专注基本面研究的资深分析师。"
            f"{topic_line}"
            "\n\n**请从基本面角度深度分析以下问题：**\n"
            "1. 财务健康度：营收增速、毛利率/净利率趋势、ROE/ROIC、自由现金流\n"
            "2. 竞争优势：护城河类型（品牌/规模/网络效应/成本/转换成本）及持久性\n"
            "3. 增长驱动：核心业务增长点、新业务拓展空间、市场天花板\n"
            "4. 估值评估：当前PE/PB/PS/EV-EBITDA与历史和同业的对比\n"
            "5. 公司治理：管理层能力、股权结构、关联交易风险\n"
            "\n**输出要求：**\n"
            "- 给出基本面评分（1-10）并说明理由\n"
            "- 列出最关键的3个看多和3个看空论据\n"
            "- 标注数据来源和时效性"
        ),
        2: (
            "你是一位市场情绪与新闻研究专家。"
            f"{topic_line}"
            "\n\n**请从市场情绪和新闻面分析以下问题：**\n"
            "1. 近期新闻动态：重大公告、政策变化、行业事件\n"
            "2. 市场情绪：分析师评级变化、机构持仓变动、融资融券数据\n"
            "3. 社交媒体与散户情绪：主流观点、情绪极端程度（过度乐观/悲观？）\n"
            "4. 事件驱动：即将到来的催化剂（财报季、政策窗口、解禁等）\n"
            "5. 市场叙事：当前市场的主要故事线是什么？是否有叙事转变的迹象？\n"
            "\n**输出要求：**\n"
            "- 给出情绪评分（1-10，1=极度悲观，10=极度乐观）\n"
            "- 判断当前情绪是否偏离基本面（即是否存在逆向投资机会）\n"
            "- 警惕确认偏差，同时呈现正面和负面信号"
        ),
        3: (
            "你是一位资深技术分析师和量化研究员。"
            f"{topic_line}"
            "\n\n**请从技术面和量化角度分析以下问题：**\n"
            "1. 趋势分析：当前处于上升/下降/盘整趋势？均线系统排列\n"
            "2. 动量指标：MACD、RSI、KDJ等指标信号\n"
            "3. 成交量分析：量价配合情况、是否有异常放量/缩量\n"
            "4. 关键价位：重要支撑位和阻力位、前期高低点\n"
            "5. 形态分析：是否有经典K线形态或技术形态\n"
            "6. 资金流向：主力资金、北向资金等流向信号\n"
            "\n**输出要求：**\n"
            "- 给出技术面评分（1-10）\n"
            "- 明确短期（1-2周）、中期（1-3月）技术面方向判断\n"
            "- 给出具体的支撑位和阻力位数值"
        ),
        4: (
            "你是一位管理千亿资产的独立首席投资官。以下是三位专业分析师从不同维度的分析结果。"
            "\n\n【基本面分析】\n{round1_content}"
            "\n\n【情绪与新闻分析】\n{round2_content}"
            "\n\n【技术面分析】\n{round3_content}"
            "\n\n**综合判断框架：**\n"
            "1. **多维度一致性**：三个维度的分析是否指向同一方向？不一致之处在哪？\n"
            "2. **权重分配**：在当前市场环境下，哪个维度的信号更可靠？\n"
            "3. **投资结论**：\n"
            "   - 总体评级：强烈看多 / 看多 / 中性 / 看空 / 强烈看空\n"
            "   - 综合信心水平：1-10分\n"
            "   - 时间维度：适用的投资周期\n"
            "4. **配置建议**：\n"
            "   - 建议仓位及建仓节奏\n"
            "   - 入场条件和触发价位\n"
            "   - 止损位和目标价\n"
            "5. **关键监控指标**：需要持续跟踪的指标和重新评估触发条件\n"
            "\n请给出明确的、可执行的投资建议。"
        ),
    }


# ── Round-robin debate prompts ───────────────────────────────────

def get_round_robin_prompt(round_num: int, prev_contents: dict[int, str]) -> str:
    """Get prompt for round-robin format where models take turns."""
    if round_num == 1:
        return (
            "你是一位经验丰富的投资分析师。请对以下问题给出你的深度分析，"
            "包含明确的观点和详细的论据。"
            "\n\n注意：提供具体数据支撑，避免泛泛而谈。"
        )

    # Subsequent rounds: see all prior arguments
    prev_text = ""
    for i in sorted(prev_contents.keys()):
        prev_text += f"\n\n【第{i}轮观点】\n{prev_contents[i]}"

    return (
        f"你是一位独立的投资分析师。以下是此前{len(prev_contents)}轮讨论的观点："
        f"\n{prev_text}"
        "\n\n请基于以上讨论：\n"
        "1. 指出你认为最有价值的观点和最大的分析漏洞\n"
        "2. 提出之前未被充分讨论的角度或风险\n"
        "3. 如果讨论已充分，给出你的综合判断和投资建议\n"
        "\n不要简单重复前面的观点，要有增量贡献。"
    )


# ── Debate summary extraction prompt ────────────────────────────

DEBATE_SUMMARY_PROMPT = """请基于以下多方辩论结果，提取结构化总结。严格输出JSON格式（不要包含```json标记）：

{debate_content}

请输出以下JSON结构：
{{
  "conclusion": "一句话结论",
  "rating": "强烈看多|看多|中性|看空|强烈看空",
  "confidence": 1到10的整数,
  "time_horizon": "短期(1-4周)|中期(1-6月)|长期(6月以上)",
  "key_bull_arguments": ["看多论据1", "看多论据2", "看多论据3"],
  "key_bear_arguments": ["看空论据1", "看空论据2", "看空论据3"],
  "consensus_points": ["各方共识1", "各方共识2"],
  "unresolved_questions": ["待解决问题1", "待解决问题2"],
  "action_items": ["建议操作1", "建议操作2"],
  "key_metrics_to_watch": ["关键指标1", "关键指标2"],
  "mentioned_tickers": ["股票代码1", "股票代码2"]
}}

只输出JSON，不要其他文字。"""
