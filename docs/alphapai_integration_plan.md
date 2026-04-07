# AlphaPai 数据集成方案

> 目标：帮助主观研究员高效聚合信息，不信息过载，不丢失关键信息

---

## 一、AlphaPai 平台数据摸底

### 1.1 四大数据源概况

| 接口 | apiName | 内容 | 数据量 | 更新频率 | 数据特点 |
|------|---------|------|--------|---------|---------|
| 公众号文章 | `get_wechat_articles_yjh` | 财经公众号精选文章 | ~6400篇 | 日均数百篇 | 完整HTML内容，平均3098字，来自券商/财经大V |
| A股纪要 | `get_summary_roadshow_info_yjh` | 券商路演/电话会纪要 | ~440条 | 日均数十条 | **成对出现**：MT(完整速记)+AI(结构化摘要)，含行业/公司标签 |
| 美股纪要 | `get_summary_roadshow_info_us_yjh` | 美股earnings call/路演 | ~144条 | 日均数条 | HTML纪要+AI辅助JSON(含topic_bullets/qa_list) |
| 点评数据 | `get_comment_info_yjh` | 分析师微信群点评 | ~607条 | 日均数十条 | 短文本(平均560字)，含机构/分析师/新财富标签 |

### 1.2 关键发现

**数据特征：**
- **A股纪要**是最高价值数据：每条路演都有MT(原始速记JSON,带角色/时间戳)和AI(结构化HTML摘要)两个版本
- **美股纪要**部分条目有丰富的 `ai_auxiliary_json_s3`，内含 `full_text_summary`、`topic_bullets`(V1/V2)、`qa_list`、`speaker_recognition` 等结构化字段
- **点评数据**时效性最强，来自分析师微信群的一手观点，22%来自新财富分析师
- **公众号文章**量最大但无行业/内容标签，需要我方LLM进行分类

**API限制：**
- 无关键词/行业过滤参数，只支持 `start_time`/`end_time`/`size` 查询
- 分页(offset/page)不生效，需通过**时间窗口滑动**实现全量拉取
- `fields` 参数可正常过滤返回字段，减少传输量
- 内容获取是两步：先查元数据列表 → 再通过 `/file/download` 下载正文

---

## 二、核心设计理念

### 2.1 问题分析

主观研究员的痛点：
1. **信息过载** — 每日数百条内容不可能逐一阅读
2. **关键信息遗漏** — 在海量信息中容易错过对持仓/关注标的有影响的内容
3. **缺乏个性化** — 不同研究员关注不同行业/股票，需要定制化筛选
4. **上下文碎片化** — 同一事件的多条信息散落在不同来源，缺乏关联

### 2.2 设计原则

```
原则1: 先聚合，再筛选，最后呈现
  → 不是把所有数据搬过来让人看，而是AI先"读"完，挑出对你重要的

原则2: 利用AlphaPai已有AI成果，不重复劳动
  → AI纪要、topic_bullets等已经是高质量摘要，直接利用而非重新生成

原则3: 分层呈现：摘要 → 要点 → 全文
  → 研究员先看一行摘要决定要不要深入，避免一上来就是万字长文

原则4: 与用户watchlist联动
  → 只有匹配到关注的行业/个股/关键词的内容才会高优推送
```

---

## 三、系统架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────┐
│                   Frontend (React)                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │ 每日简报  │ │ 智能信息流│ │ 纪要中心  │ │点评速递│ │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └───┬────┘ │
│       └────────────┴────────────┴────────────┘      │
│                         │ REST API + WebSocket       │
├─────────────────────────┼───────────────────────────┤
│                   Backend (FastAPI)                   │
│  ┌──────────────────────┼──────────────────────────┐│
│  │          AlphaPai API Routes                     ││
│  │  /api/alphapai/digest    — 每日简报              ││
│  │  /api/alphapai/feed      — 智能信息流            ││
│  │  /api/alphapai/roadshow  — 纪要列表/详情         ││
│  │  /api/alphapai/comments  — 点评列表              ││
│  │  /api/alphapai/articles  — 文章列表              ││
│  └──────────────────────┼──────────────────────────┘│
│                         │                            │
│  ┌──────────────────────┼──────────────────────────┐│
│  │        AlphaPai Processing Pipeline              ││
│  │                                                  ││
│  │  ┌──────────┐   ┌──────────┐   ┌─────────────┐ ││
│  │  │ Sync     │──▶│ Process  │──▶│ Score &     │ ││
│  │  │ Service  │   │ & Enrich │   │ Personalize │ ││
│  │  └──────────┘   └──────────┘   └─────────────┘ ││
│  └──────────────────────────────────────────────────┘│
│                         │                            │
│  ┌──────────────────────┼──────────────────────────┐│
│  │              PostgreSQL + Redis                   ││
│  │  alphapai_articles, alphapai_roadshows,           ││
│  │  alphapai_comments, alphapai_digests              ││
│  └──────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
                         │
                 AlphaPai API (外部)
         https://api-test.rabyte.cn
```

### 3.2 为什么用独立模型而非复用 news_items

AlphaPai 数据与现有 news_items 的根本区别：

| 维度 | news_items (现有) | AlphaPai 数据 |
|------|------------------|-------------|
| 来源 | RSS/网页爬虫/公开API | 商业付费平台 |
| 内容类型 | 新闻/公告 (单一) | 文章/纪要/点评 (多种) |
| 结构 | 标题+正文 | 元数据+AI摘要+全文+Q&A+speaker等 |
| 处理方式 | 3-phase LLM pipeline | 已有AI摘要，仅需轻量评分 |
| 更新频率 | 分钟级轮询 | 5-10分钟批量拉取 |

使用独立模型可以：
- 保留AlphaPai丰富的结构化字段(行业、机构、分析师、AI摘要等)
- 避免在通用news_items模型中塞入过多JSONB字段
- 处理流程独立，不影响现有pipeline的性能
- 未来方便扩展(如AlphaPai新增接口)

---

## 四、数据模型设计

### 4.1 数据库表

```sql
-- 1. 公众号文章 (来自 get_wechat_articles_yjh)
CREATE TABLE alphapai_articles (
    id SERIAL PRIMARY KEY,
    arc_code VARCHAR(64) UNIQUE NOT NULL,     -- AlphaPai唯一ID
    title VARCHAR(500),                        -- arc_name
    author VARCHAR(200),
    publish_time TIMESTAMPTZ,
    word_count INTEGER,
    read_duration VARCHAR(20),
    is_original BOOLEAN DEFAULT false,
    wechat_url TEXT,                           -- 原始微信链接
    content_html_path VARCHAR(500),            -- AlphaPai文件路径
    content_cached TEXT,                       -- 下载后缓存的正文
    -- AI处理字段
    ai_summary TEXT,                           -- LLM生成的一句话摘要
    ai_tags JSONB DEFAULT '[]',               -- LLM提取的标签 ["行业","主题"]
    ai_tickers JSONB DEFAULT '[]',            -- LLM提取的相关股票
    ai_sectors JSONB DEFAULT '[]',            -- LLM提取的相关行业
    relevance_score FLOAT DEFAULT 0,           -- 对当前用户的相关性评分
    -- 元数据
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}'
);

-- 2. 路演纪要 (来自 get_summary_roadshow_info_yjh + _us_yjh)
CREATE TABLE alphapai_roadshows (
    id SERIAL PRIMARY KEY,
    roadshow_id VARCHAR(64),                   -- AlphaPai roadshow_id
    trans_id VARCHAR(64) UNIQUE NOT NULL,       -- 纪要唯一ID
    market VARCHAR(10) NOT NULL,               -- 'cn' 或 'us'
    title VARCHAR(500),                        -- show_title
    company VARCHAR(200),
    guest VARCHAR(500),                        -- 嘉宾/发言人
    event_time TIMESTAMPTZ,                    -- stime
    word_count INTEGER,
    est_reading_time VARCHAR(20),
    -- 分类标签
    industry JSONB DEFAULT '[]',               -- ind_json 解析
    trans_source VARCHAR(10),                  -- 'MT' 或 'AI'
    is_conference BOOLEAN DEFAULT false,
    is_investigation BOOLEAN DEFAULT false,
    is_executive BOOLEAN DEFAULT false,
    is_buyside BOOLEAN DEFAULT false,
    -- 内容
    content_path VARCHAR(500),                 -- AlphaPai文件路径
    ai_summary TEXT,                           -- 会议核心摘要(来自AI纪要或我方LLM)
    ai_key_points JSONB DEFAULT '[]',          -- 关键要点列表
    ai_qa_highlights JSONB DEFAULT '[]',       -- Q&A精华
    full_text_cached TEXT,                     -- 下载后缓存的全文
    -- AI辅助(美股特有)
    ai_auxiliary JSONB,                        -- ai_auxiliary_json_s3的完整内容
    -- 评分
    relevance_score FLOAT DEFAULT 0,
    -- 元数据
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}'
);

-- 3. 分析师点评 (来自 get_comment_info_yjh)
CREATE TABLE alphapai_comments (
    id SERIAL PRIMARY KEY,
    cmnt_hcode VARCHAR(64) UNIQUE NOT NULL,    -- AlphaPai唯一ID
    title VARCHAR(500),
    content TEXT,
    analyst_name VARCHAR(100),                 -- psn_name
    team_name VARCHAR(200),                    -- team_cname
    institution VARCHAR(200),                  -- inst_cname
    comment_date TIMESTAMPTZ,                  -- cmnt_date
    is_new_fortune BOOLEAN DEFAULT false,      -- 新财富分析师
    src_type INTEGER,
    group_id VARCHAR(100),
    -- AI处理字段
    ai_summary TEXT,                           -- 一句话提炼
    ai_tickers JSONB DEFAULT '[]',             -- 提及的股票
    ai_sectors JSONB DEFAULT '[]',             -- 相关行业
    ai_sentiment VARCHAR(20),                  -- 看多/看空/中性
    relevance_score FLOAT DEFAULT 0,
    -- 元数据
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}'
);

-- 4. 每日简报 (LLM生成)
CREATE TABLE alphapai_digests (
    id SERIAL PRIMARY KEY,
    digest_date DATE UNIQUE NOT NULL,
    market_overview TEXT,                       -- 市场概览
    key_events JSONB DEFAULT '[]',             -- 今日关键事件
    sector_highlights JSONB DEFAULT '{}',      -- 按行业的要闻
    watchlist_alerts JSONB DEFAULT '[]',       -- 与用户watchlist相关的提醒
    full_digest TEXT,                          -- 完整简报文本
    source_stats JSONB DEFAULT '{}',           -- 数据统计(今日文章数/纪要数等)
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    model_used VARCHAR(50),
    token_cost FLOAT DEFAULT 0
);

-- 索引
CREATE INDEX idx_articles_publish ON alphapai_articles(publish_time DESC);
CREATE INDEX idx_articles_relevance ON alphapai_articles(relevance_score DESC);
CREATE INDEX idx_roadshows_event ON alphapai_roadshows(event_time DESC);
CREATE INDEX idx_roadshows_market ON alphapai_roadshows(market, event_time DESC);
CREATE INDEX idx_roadshows_source ON alphapai_roadshows(trans_source);
CREATE INDEX idx_comments_date ON alphapai_comments(comment_date DESC);
CREATE INDEX idx_comments_institution ON alphapai_comments(institution);
CREATE INDEX idx_digests_date ON alphapai_digests(digest_date DESC);
```

---

## 五、处理流程设计

### 5.1 数据同步服务 (AlphaPaiSyncService)

```
每5分钟执行一次:

1. 拉取增量数据
   ├─ get_wechat_articles_yjh(start_time=上次同步时间, size=100)
   ├─ get_summary_roadshow_info_yjh(同上)
   ├─ get_summary_roadshow_info_us_yjh(同上)
   └─ get_comment_info_yjh(同上)

2. 去重入库
   ├─ 按 arc_code/trans_id/cmnt_hcode 判断是否已存在
   └─ 新数据插入对应表

3. 内容下载(异步)
   ├─ 纪要: 优先下载AI版本的content → 缓存到full_text_cached
   ├─ 美股: 下载ai_auxiliary_json_s3 → 存到ai_auxiliary字段
   └─ 文章: 按需下载(仅高相关性文章才下载全文)
```

### 5.2 智能处理流程 (AlphaPaiProcessor)

针对不同内容类型使用**差异化处理策略**，最大化利用AlphaPai已有AI成果：

#### A. 公众号文章处理

```
文章标题 + 作者 + 前500字
        │
        ▼
  ┌─────────────────┐
  │ MiniMax M2 LLM  │  ← 轻量评分，不下载全文
  │ Prompt:         │
  │ "判断这篇文章的: │
  │  1.一句话摘要    │
  │  2.相关行业      │
  │  3.相关股票代码   │
  │  4.主题标签      │
  │  5.对投资的价值   │
  │    (0-10分)"     │
  └────────┬────────┘
           │
           ▼
  score >= 6? ──Yes──▶ 下载全文HTML → 生成详细摘要
       │
       No
       │
       ▼
  仅保存一句话摘要，不下载全文(节省带宽和存储)
```

#### B. A股纪要处理

```
同一场路演有MT + AI两条记录
        │
        ▼
  ┌─────────────────┐
  │ 识别配对关系     │  ← 通过 roadshow_id 配对
  │ (同roadshow_id)  │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │ 优先使用AI纪要   │  ← AlphaPai的AI摘要质量已经很高
  │ 下载AI版HTML     │
  │ 提取:           │
  │  - 会议要点      │
  │  - 关键数据点    │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │ LLM 轻量增强    │  ← 只做watchlist匹配和投资信号提取
  │ - 匹配watchlist │
  │ - 提取买卖信号   │
  │ - 评估新增信息量 │
  └────────┬────────┘
           │
           ▼
  与用户watchlist匹配 → 推送通知(如果匹配到)
```

#### C. 美股纪要处理

```
有 ai_auxiliary_json_s3?
    │
    ├── Yes ──▶ 直接解析JSON:
    │           full_text_summary → ai_summary
    │           topic_bullets_v2  → ai_key_points
    │           qa_list_v2        → ai_qa_highlights
    │           (无需LLM处理!)
    │
    └── No ───▶ 下载HTML内容 → LLM生成摘要和要点
```

#### D. 分析师点评处理

```
点评content (平均560字，已经很短)
        │
        ▼
  ┌─────────────────────────┐
  │ 批量处理(10条一批)       │  ← 减少API调用次数
  │ MiniMax M2 LLM Prompt:  │
  │ "对以下10条分析师点评:   │
  │  逐条提取:              │
  │  1.核心观点(一句话)      │
  │  2.看多/看空/中性        │
  │  3.涉及股票代码          │
  │  4.涉及行业              │
  │  5.信息新鲜度(0-10)"     │
  └──────────┬──────────────┘
             │
             ▼
  按行业/股票聚合 → 发现同一标的多空分歧时高亮提醒
```

### 5.3 每日简报生成 (DigestGenerator)

```
每天早上 8:00 自动运行:

输入:
  ├─ 过去24小时所有高分内容(relevance_score >= 6)
  ├─ 用户watchlist中的行业/股票
  ├─ 按行业聚合的纪要数量和关键变化
  └─ 新财富分析师的最新观点

        │
        ▼
  ┌──────────────────────┐
  │ MiniMax M2 LLM       │
  │ 生成结构化简报:       │
  │                      │
  │ 1. 市场概览(3-5句)    │
  │ 2. 今日关键事件(3-5条)│
  │ 3. 行业要闻           │
  │    - 你关注的行业优先  │
  │ 4. 重要纪要精选        │
  │    - 投资信号提取      │
  │ 5. 分析师观点速览      │
  │    - 多空分歧标记      │
  │ 6. 需要关注的风险      │
  └──────────────────────┘
        │
        ▼
  存入 alphapai_digests
  推送到WebSocket/飞书
```

---

## 六、后端API设计

### 6.1 路由设计

```python
# backend/app/api/alphapai.py

# === 每日简报 ===
GET  /api/alphapai/digest                     # 获取今日简报(或指定日期)
     ?date=2026-03-10

# === 智能信息流(统一) ===
GET  /api/alphapai/feed                       # 按相关性排序的统一信息流
     ?hours=24                                # 时间范围
     &type=article,roadshow,comment           # 内容类型过滤
     &market=cn,us                            # 市场
     &sector=非银金融,汽车                      # 行业过滤
     &min_score=5                             # 最低相关性分数
     &page=1&page_size=20

# === 纪要中心 ===
GET  /api/alphapai/roadshows                  # 纪要列表
     ?market=cn                               # cn/us
     &industry=轻工制造                        # 行业
     &company=长江证券                         # 券商
     &source=AI                               # MT/AI
     &hours=48
     &page=1&page_size=20

GET  /api/alphapai/roadshows/{trans_id}       # 纪要详情(含全文)
GET  /api/alphapai/roadshows/{trans_id}/pair   # 获取配对的MT/AI版本

# === 分析师点评 ===
GET  /api/alphapai/comments                   # 点评列表
     ?institution=华创证券                     # 机构
     &analyst=王鲜俐                           # 分析师
     &fortune_only=true                       # 仅新财富
     &sentiment=bullish                       # 看多/看空
     &hours=24
     &page=1&page_size=20

# === 公众号文章 ===
GET  /api/alphapai/articles                   # 文章列表
     ?author=吴开达团队                        # 作者
     &min_score=6                             # 最低评分
     &hours=48
     &page=1&page_size=20

GET  /api/alphapai/articles/{arc_code}        # 文章详情(含全文)

# === 统计 ===
GET  /api/alphapai/stats                      # 数据统计(今日各类数量/处理状态)
```

---

## 七、前端页面设计

### 7.1 新增页面: AlphaPai 智能研究台

在左侧导航栏新增 "研究台" 入口，包含以下子页面：

#### A. 每日简报 (Morning Brief)

```
┌────────────────────────────────────────────────────┐
│  📋 每日研究简报 — 2026年3月10日          [← →] 日期 │
├────────────────────────────────────────────────────┤
│                                                    │
│  市场概览                                           │
│  ┌──────────────────────────────────────────────┐  │
│  │ 两会后首个交易周，政策面信号积极。造纸、保险   │  │
│  │ 等顺周期板块受关注，科技股分化...              │  │
│  └──────────────────────────────────────────────┘  │
│                                                    │
│  🔔 与你相关 (基于Watchlist)                        │
│  ┌──────────────────────────────────────────────┐  │
│  │ • [轻工制造] 造纸板块迎涨价潮，白卡纸+200元/吨│  │
│  │ • [非银金融] 期货板块投资价值被重申            │  │
│  └──────────────────────────────────────────────┘  │
│                                                    │
│  今日重要纪要 (3)                                   │
│  ┌──────────────────────────────────────────────┐  │
│  │ 1. 国海轻工 | 看好顺周期+地产链 ⭐ 7565字      │  │
│  │    → 造纸涨价、地产后周期修复                   │  │
│  │ 2. 长江非银 | 保险尘读第5期    📊 1910字        │  │
│  │    → 保险板块估值修复逻辑                      │  │
│  │ 3. Korn Ferry Q3 Earnings  🇺🇸 2929字          │  │
│  │    → FY26Q3 presentation                       │  │
│  └──────────────────────────────────────────────┘  │
│                                                    │
│  分析师观点精选 (5)                                  │
│  ┌──────────────────────────────────────────────┐  │
│  │ 🟢 涤纶长丝 POY 涨8.1% — 中信建投化工        │  │
│  │ 🟢 海光信息受益Agent发展 — 某群观点            │  │
│  │ 🟡 电解铝受地缘冲突影响 — 国金金属            │  │
│  └──────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────┘
```

#### B. 智能信息流 (Smart Feed)

```
┌────────────────────────────────────────────────────┐
│  信息流  [全部|文章|纪要|点评]  [A股|美股]  [24h▾]   │
│          [行业▾] [机构▾] [仅高相关▾]                │
├────────────────────────────────────────────────────┤
│                                                    │
│  ┌─ 09:15 ─ 纪要 ─ 相关度 8.5 ──────────────────┐ │
│  │ 国海轻工 | 看好顺周期+地产链                    │ │
│  │ 🏢 国海证券 · 林昕宇 · 轻工制造                │ │
│  │ AI摘要: 造纸行业迎涨价潮，白卡纸/文化纸/特种  │ │
│  │ 纸全线提价，龙头企业协同提价意愿强...           │ │
│  │ [查看AI纪要] [查看完整速记] [标记已读]          │ │
│  └────────────────────────────────────────────────┘ │
│                                                    │
│  ┌─ 00:16 ─ 点评 ─ 相关度 7.2 ──────────────────┐ │
│  │ 涤纶长丝：本周去库显著，产品价格强势上涨 🟢    │ │
│  │ 🏢 华创证券 · 王鲜俐 · 🏆新财富                │ │
│  │ POY价格上涨575元至7650元/吨(+8.1%)，下游       │ │
│  │ 补库周期启动...                                │ │
│  │ [展开全文] [标记已读]                           │ │
│  └────────────────────────────────────────────────┘ │
│                                                    │
│  ┌─ 00:01 ─ 文章 ─ 相关度 6.0 ──────────────────┐ │
│  │ 静待波动率冲高见顶                              │ │
│  │ ✍️ 吴开达团队 · 5469字 · 原创                   │ │
│  │ AI摘要: 当前市场波动率处于高位，建议等待VIX     │ │
│  │ 回落信号后再加仓...                             │ │
│  │ [查看全文] [标记已读]                           │ │
│  └────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────┘
```

#### C. 纪要中心 (Roadshow Hub)

```
┌────────────────────────────────────────────────────┐
│  纪要中心  [A股|美股]  [行业▾]  [券商▾]  [48h▾]     │
├────────────────────────────────────────────────────┤
│                                                    │
│  行业概览 (今日纪要数)                               │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐              │
│  │轻工│ │非银│ │交运│ │石化│ │汽车│ ...           │
│  │ 2  │ │ 2  │ │ 4  │ │ 3  │ │ 2  │              │
│  └────┘ └────┘ └────┘ └────┘ └────┘              │
│                                                    │
│  纪要列表                                           │
│  ┌──────────────────────────────────────────────┐  │
│  │ 国海轻工 | 看好顺周期+地产链                   │  │
│  │ 🏢国海证券 · 林昕宇 · 2026-03-09 07:15       │  │
│  │ 📝 7565字(MT) / 3122字(AI) · ⏱️ 13:45阅读    │  │
│  │ 💡 造纸涨价 | 地产后周期 | 稳增长              │  │
│  │ [AI摘要] [完整纪要] [原始速记]                 │  │
│  └──────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────┐  │
│  │ 长江非银 | 保险尘读第5期                       │  │
│  │ 🏢长江证券 · 谢宇尘 · 2026-03-09 07:00       │  │
│  │ 📝 1910字(MT) / 1926字(AI) · ⏱️ 3:28阅读     │  │
│  │ 💡 保险估值修复 | 利差改善                     │  │
│  │ [AI摘要] [完整纪要] [原始速记]                 │  │
│  └──────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────┘

─── 纪要详情页 ───

┌────────────────────────────────────────────────────┐
│  ← 返回  国海轻工 | 看好顺周期+地产链               │
│  🏢国海证券 · 林昕宇(首席) · 2026-03-09            │
│  轻工制造 · 7565字 · ⏱️ 13:45                      │
├────────────────────────────────────────────────────┤
│  [AI摘要✓] [完整纪要] [原始速记JSON]  ← Tab切换     │
│                                                    │
│  会议要点                                           │
│  ═══════                                           │
│  1. 造纸行业行情与投资分析                           │
│     • 白卡纸：1-2月上调200元/吨，3月执行            │
│     • 文化纸：3月1日起提价200元/吨                   │
│     • 特种纸：白牛皮纸提价300元/吨                   │
│                                                    │
│  2. 地产后周期投资机会                               │
│     • 上海"沪七条"落地                              │
│     • 两会扩内需政策加持                             │
│     ...                                            │
└────────────────────────────────────────────────────┘
```

### 7.2 导航结构

```
原有菜单:
  Dashboard          → 保持
  NewsFeed          → 保持 (原有新闻源)
  Watchlist         → 保持
  Analytics         → 保持
  Settings          → 保持
  Admin             → 保持

新增:
  研究台 (AlphaPai)  → 新增一级菜单
    ├─ 每日简报      → /alphapai/digest
    ├─ 智能信息流    → /alphapai/feed
    ├─ 纪要中心      → /alphapai/roadshows
    └─ 点评速递      → /alphapai/comments
```

---

## 八、LLM 使用策略 (MiniMax M2)

### 8.1 Token 预算估算

| 处理场景 | 日均条数 | 平均输入tokens | 平均输出tokens | 日均消耗 |
|---------|---------|-------------|-------------|---------|
| 文章评分(标题+摘要) | ~200 | ~500 | ~200 | 140K |
| 文章全文摘要(高分) | ~30 | ~3000 | ~500 | 105K |
| A股纪要信号提取 | ~30 | ~1000 | ~300 | 39K |
| 点评批量处理(10条/批) | ~6批 | ~3000 | ~1000 | 24K |
| 每日简报生成 | 1 | ~5000 | ~2000 | 7K |
| **合计** | | | | **~315K tokens/天** |

MiniMax M2 成本极低(约¥0.01/千tokens)，日均成本约 ¥3。

### 8.2 Prompt 设计原则

```
1. 批量处理 — 点评10条一批，减少API调用
2. 结构化输出 — 要求JSON格式输出，便于解析
3. 中文优先 — 中文prompt效率更高
4. 带例子 — Few-shot提高输出一致性
5. watchlist注入 — 将用户关注的股票/行业注入prompt，提高匹配率
```

### 8.3 示例 Prompt

```
文章评分 Prompt:
───────────────
你是一名资深A股研究助理。请评估以下财经文章对机构投资研究的价值。

文章标题: {title}
作者: {author}
发布时间: {publish_time}
正文摘要: {first_500_chars}

请以JSON格式输出:
{
  "summary": "一句话摘要(不超过50字)",
  "sectors": ["相关行业1", "相关行业2"],
  "tickers": ["600000.SH", "000001.SZ"],
  "tags": ["主题标签1", "标签2"],
  "score": 7,  // 0-10, 对投资研究的参考价值
  "reason": "评分理由(一句话)"
}

评分标准:
- 8-10: 含具体投资建议、独到市场观点、重要数据变化
- 5-7: 有一定参考价值的行业分析、市场解读
- 0-4: 泛泛而谈、旧闻重述、营销软文
```

---

## 九、关键实现细节

### 9.1 数据同步的时间窗口策略

由于AlphaPai API不支持标准分页，采用**时间窗口滑动**策略：

```python
class AlphaPaiSyncService:
    async def sync_incremental(self, api_name: str, table, last_sync_time: datetime):
        """增量同步 — 基于时间窗口"""
        current_time = datetime.now()
        window_start = last_sync_time - timedelta(minutes=5)  # 5分钟重叠防漏

        all_items = []
        batch_size = 50

        while True:
            result = await self._query(api_name, start_time=window_start,
                                        end_time=current_time, size=batch_size)
            items = result.get("data", {}).get("data", [])
            all_items.extend(items)

            if not result.get("data", {}).get("hasMore", False):
                break

            # 用最后一条的时间作为下一个窗口起点
            if items:
                last_time = items[-1].get("stime") or items[-1].get("publish_time")
                window_start = parse_datetime(last_time)
            else:
                break

        # 去重后入库
        new_count = await self._upsert_items(table, all_items)
        return new_count
```

### 9.2 MT/AI纪要配对逻辑

```python
async def pair_roadshow_records(self, roadshow_id: str):
    """将同一场路演的MT和AI版本关联"""
    records = await db.query(
        AlphaPaiRoadshow.filter(roadshow_id=roadshow_id)
    )
    mt_version = next((r for r in records if r.trans_source == "MT"), None)
    ai_version = next((r for r in records if r.trans_source == "AI"), None)

    # 前端展示时:
    # - 默认显示AI版本(更精炼)
    # - 提供"查看完整速记"按钮切换到MT版本
    return {"ai": ai_version, "mt": mt_version}
```

### 9.3 与 Watchlist 联动

```python
async def score_relevance(self, item, user_watchlist):
    """基于用户watchlist计算个性化相关性"""
    base_score = item.relevance_score  # LLM评分

    # 加分项
    bonus = 0
    item_tickers = set(item.ai_tickers or [])
    item_sectors = set(item.ai_sectors or [])

    watched_tickers = set(user_watchlist.tickers)
    watched_sectors = set(user_watchlist.sectors)
    watched_keywords = user_watchlist.keywords

    # 股票匹配: +3分
    if item_tickers & watched_tickers:
        bonus += 3

    # 行业匹配: +2分
    if item_sectors & watched_sectors:
        bonus += 2

    # 关键词匹配: +1分
    title = item.title or ""
    if any(kw in title for kw in watched_keywords):
        bonus += 1

    # 新财富分析师: +1分 (仅点评)
    if getattr(item, 'is_new_fortune', False):
        bonus += 1

    return min(base_score + bonus, 10)
```

---

## 十、实施计划

### Phase 1: 基础数据通路 (3天)

| 任务 | 工作量 |
|------|--------|
| 创建数据库模型 + Alembic migration | 0.5天 |
| 实现 AlphaPaiClient (API封装) | 0.5天 |
| 实现 AlphaPaiSyncService (定时同步) | 0.5天 |
| 实现基础 REST API (列表/详情) | 1天 |
| 配置管理 (.env + config.py) | 0.5天 |

**验收标准**: 数据能自动同步到PostgreSQL，API可返回列表数据

### Phase 2: AI处理流程 (2天)

| 任务 | 工作量 |
|------|--------|
| 实现文章评分 Prompt + MiniMax M2调用 | 0.5天 |
| 实现点评批量处理 | 0.5天 |
| 实现纪要AI摘要提取 | 0.5天 |
| 实现每日简报生成 | 0.5天 |

**验收标准**: 新同步的数据自动经过LLM处理，生成摘要和评分

### Phase 3: 前端页面 (3天)

| 任务 | 工作量 |
|------|--------|
| AlphaPai导航入口 + 路由 | 0.5天 |
| 每日简报页面 | 0.5天 |
| 智能信息流页面 | 1天 |
| 纪要中心页面(列表+详情) | 0.5天 |
| 点评速递页面 | 0.5天 |

**验收标准**: 所有页面可正常展示数据，支持过滤和搜索

### Phase 4: 个性化 + 打磨 (2天)

| 任务 | 工作量 |
|------|--------|
| Watchlist联动评分 | 0.5天 |
| WebSocket实时推送新内容 | 0.5天 |
| 飞书高优消息推送 | 0.5天 |
| 性能优化 + 错误处理 | 0.5天 |

**验收标准**: 内容匹配用户关注时自动高亮推送

---

## 十一、配置项

```ini
# .env 新增配置
ALPHAPAI_APP_ID=wdWQMvEwFTKWZoFE1Qen0iIb
ALPHAPAI_BASE_URL=https://api-test.rabyte.cn
ALPHAPAI_SYNC_INTERVAL=300          # 同步间隔(秒)
ALPHAPAI_ENABLED=true

# MiniMax M2 (用于AlphaPai数据处理)
MINIMAX_API_KEY=xxx
MINIMAX_MODEL=MiniMax-M2
MINIMAX_BASE_URL=https://api.minimax.chat/v1
```

---

## 十二、风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| AlphaPai API不稳定/限流 | 数据同步中断 | 指数退避重试 + 健康检查告警 |
| 无标准分页导致数据遗漏 | 漏掉部分内容 | 时间窗口5分钟重叠 + 去重机制 |
| LLM评分质量不稳定 | 重要信息被低评 | 新财富分析师自动加分 + 关键词白名单 |
| 日数据量增长 | 处理延迟/成本增加 | 分级处理(低分内容不下载全文) |
| content_html路径变更 | 内容下载失败 | 缓存已下载内容 + 路径兜底策略 |

---

## 总结

本方案的核心价值主张：

**从"人读平台"到"AI读平台，人读摘要"**

- AlphaPai 每天产出数百条内容 → 我们的系统用AI先"读"一遍
- 结合用户的 Watchlist → 只推送相关的、重要的
- 分层呈现（一句话摘要 → 要点列表 → 完整全文）→ 研究员按需深入
- 每日简报 → 一页纸掌握全天要闻
- 利用AlphaPai已有AI成果(AI纪要/topic_bullets) → 不重复生成，节省成本
