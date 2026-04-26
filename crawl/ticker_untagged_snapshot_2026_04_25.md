# 打标覆盖率快照 — 2026-04-25

快照时间:`2026-04-25 Round 2 跑完`,远端 Mongo `192.168.31.176:35002`(快照后 2026-04-26 已迁回本机 `127.0.0.1:27018`)

## TL;DR

- **空标总数(除 wechat): 863,748 条** / 总量 1,852,612 / 空率 **46.6%**
- 一早发现昨天 Round 1 的 40,600 条标题兜底被 prod cron 用旧代码覆写回空,已在
  今晨(2026-04-25 上午)把 cron line 16/17 迁到 staging 新代码,并重跑一次
  `--only-empty` 恢复 + 追加。Round 2 新增 **92,351** 条打标(含 **46,720** 条标题兜底)。
- 今晚 03:00 CST 的 `--reload-aliases` 全量 cron 现在跑的是 staging 新代码,**不会再覆写**。

## Round 2 结果(2026-04-25 上午重跑)

- 扫描总数:**996,690** 文档
- 新打标(`[]` → 非空):**92,351** 条
- 其中标题兜底:**46,720** 条(占新增 51%)
- 全量耗时:约 2h25m,112 docs/s
- 异常:0,locks 释放

| 集合 | 扫描 | 新命中 | 命中率 | title 兜底 |
|---|---:|---:|---:|---:|
| alphapai.roadshows | 15,318 | 691 | 4% | 689 |
| **alphapai.reports** | 52,361 | **19,633** | **37%** | 2,665 |
| alphapai.comments | 2,380 | 3 | 0% | 3 |
| alphapai.wechat_articles | 40,606 | 42 | 0% | 42 |
| jinmen.meetings | 6,684 | 2 | 0% | 2 |
| jinmen.reports | 6,076 | 572 | 9% | 572 |
| **jinmen.oversea_reports** | **763,708** | **39,325** | **5%** | **39,325** |
| meritco.forum | 737 | 1 | 0% | 1 |
| funda.sentiments | 298 | 15 | 5% | 0 |
| acecamp.articles | 4,928 | 17 | 0% | 16 |
| **alphaengine.summaries** | 893 | **709** | **79%** | 0 |
| alphaengine.china_reports | 591 | 25 | 4% | 11 |
| **alphaengine.foreign_reports** | 1,044 | 519 | **49%** | 27 |
| alphaengine.news_items | 3,468 | 123 | 3% | 0 |
| gangtise.summaries | 21,896 | 4,466 | 20% | 73 |
| gangtise.researches | 33,304 | 4,499 | 13% | 1,402 |
| **gangtise.chief_opinions** | 42,185 | **21,709** | **51%** | 1,892 |
| 其他(jiuqian/third/funda/meritco/semianalysis)| <700 | 0 | — | 0 |
| **合计** | **996,690** | **92,351** | **9.3%** | **46,720** |

## 当前全库打标覆盖率快照

| Collection | 总量 | 已打标 | 空 `[]` | 空率 |
|---|---:|---:|---:|---:|
| alphapai.roadshows | 48,954 | 34,327 | 14,627 | 29.9% |
| alphapai.reports | 52,361 | 19,633 | 32,728 | 62.5% |
| alphapai.comments | 12,881 | 10,504 | 2,377 | 18.4% |
| jinmen.meetings | 14,929 | 8,247 | 6,682 | 44.8% |
| jinmen.reports | 11,712 | 6,208 | 5,504 | 47.0% |
| **jinmen.oversea_reports** | 1,513,274 | **788,891** | 724,383 | 47.9% |
| jiuqian.forum | 2,748 | 2,011 | 737 | 26.8% |
| jiuqian.research | 68 | 7 | 61 | 89.7% |
| third-bridge.interviews | 148 | 55 | 93 | 62.8% |
| funda.posts | 364 | 354 | 10 | 2.7% |
| funda.earnings_reports | 1,079 | 1,079 | 0 | 0.0% |
| funda.earnings_transcripts | 3,355 | 3,355 | 0 | 0.0% |
| funda.sentiments | 10,846 | 10,563 | 283 | 2.6% |
| acecamp.articles | 25,199 | 20,288 | 4,911 | 19.5% |
| acecamp.events | 0 | 0 | 0 | — |
| alphaengine.summaries | 1,807 | 1,623 | 184 | 10.2% |
| alphaengine.china_reports | 1,692 | 1,126 | 566 | 33.5% |
| alphaengine.foreign_reports | 1,200 | 675 | 525 | 43.8% |
| alphaengine.news_items | 4,027 | 706 | 3,321 | 82.5% |
| gangtise.summaries | 51,083 | 33,656 | 17,427 | 34.1% |
| gangtise.researches | 40,657 | 11,856 | 28,801 | 70.8% |
| gangtise.chief_opinions | 54,162 | 33,700 | 20,462 | 37.8% |
| **合计(除 wechat)** | **1,852,612** | **988,864** | **863,748** | **46.6%** |

**alphapai.wechat_articles 单列参考:** 44,858 / 4,294 tagged / 40,564 empty (90.4%)

## 与昨日对比

| | 2026-04-24 Round 1 后(被覆写前) | 2026-04-25 Round 2 后 | 变化 |
|---|---:|---:|---:|
| 空标总数(除 wechat) | ~916k(估算)| **863,748** | **-52k** |
| 空率 | ~49.5% | **46.6%** | **-2.9 pp** |
| `_title` 兜底条数持久化 | 40,600 | **46,720** | 恢复 + 新增 |

## 修复的 cron(2026-04-25 上午)

```cron
# 两行前缀从 /home/ygwang/trading_agent 改为 /home/ygwang/trading_agent_staging
*/10 * * * * cd /home/ygwang/trading_agent_staging && ... enrich_tickers.py --incremental ...
0 3 * * *   cd /home/ygwang/trading_agent_staging && ... enrich_tickers.py --reload-aliases ...
```

## 下一步候选(优先级递减)

1. **去噪 `_unmatched_raw`** —— 当前 `gangtise.chief_opinions` 的 `aflScr` 字段吐出大量
   `821xxx.CI` / `821xxx.SWI` 行业指数代码,占 Top30 未匹配的主力。在 extractor 里
   `re.match(r"\d{6}\.(CI|SWI)$", s)` 命中丢 skip list,或新开
   `_canonical_industry_indices` 字段单独收。改完再做一次 Top100 聚合才能看到真
   alias candidate。
2. **alphapai.reports 空率仍 62.5%** —— 52k 报告里 32k 空。其中很多是英文研报,结构化
   字段里没代码,标题兜底只抓到 2,665 条。剩下需要更激进的正则(比如 `[Ll]td\s*\(` 后
   跟的代码)或 LLM NER。
3. **gangtise.researches 空率 70.8%** —— 同样结构化弱,title 兜底只救了 1,402 条。

---

生成脚本:`scripts/enrich_tickers.py --only-empty`(staging worktree,agent env)

---

## LLM 自动打标脚本(`scripts/llm_tag_tickers.py`)

`enrich_tickers.py` 的两层规则(结构化字段 + 标题正则)已经把"无成本"路径
跑到极限,**剩下 ~86 万条 `[]` 文档绝大部分是英文研报正文里公司名作主题但无
括号代码** —— 例如 `gangtise.researches` 28k 条空、`alphapai.reports` 32k
条空。这些只能靠 LLM NER 打标。

`llm_tag_tickers.py` 是这个 LLM 路径的执行器。设计要点:

- **跟规则脚本严格分工** —— 只扫 `_canonical_tickers: []` 的文档,且
  默认 skip 已经被 LLM 标过的(`_canonical_extract_source` 含 `_llm:`)
- **白名单兜底** —— LLM 输出走 `ticker_normalizer.normalize_with_unmatched`,
  编出来的 market 后缀直接进 `_unmatched_raw`,不会污染 canonical
- **预算硬停** —— `--max-docs` 和 `--max-cost-usd` 任一触达即落盘并退出
- **样本估算** —— 跑前抽 N 条估算平均 token,投影到目标总数,弹
  `(预估 tokens / 美元 / 时长)` 表后等用户确认
- **审计字段** —— 写入 `_canonical_extract_source: "{source}_llm:{model_key}"`
  方便回滚 / 区分 / 重跑

### 模型菜单 + 单价(2026-04 list)

| key | provider | model_id | $/1M in | $/1M out | qps | 适用场景 |
|---|---|---|---:|---:|---:|---|
| `claude-sonnet` | OpenRouter | anthropic/claude-sonnet-4-6 | 3.00 | 15.00 | 5 | 英文研报准确度最高,适合 alphapai.reports / alphaengine.foreign_reports |
| `claude-haiku` | OpenRouter | anthropic/claude-haiku-4-5 | 1.00 | 5.00 | 8 | 性价比甜点,Sonnet 召回的 90% 但便宜 3× |
| `gpt-5-mini` | OpenAI | gpt-5.4-mini | 0.40 | 1.60 | 10 | 英文 NER 强,需 Clash 代理 |
| `gpt-5` | OpenAI | gpt-5.4 | 2.50 | 10.00 | 8 | 与 Sonnet 平起平坐,需代理 |
| `gemini-flash` | OpenRouter | google/gemini-3.1-flash | 0.30 | 2.50 | 10 | **首选海量集合**(jinmen.oversea_reports);中英双语都行 |
| `gemini-pro` | OpenRouter | google/gemini-3.1-pro-preview | 1.25 | 10.00 | 6 | 复杂中英混合纪要场景 |
| `deepseek-v3` | OpenRouter | deepseek/deepseek-chat-v3.2 | 0.27 | 1.10 | 8 | 国产替代,中英都行,JSON 遵循度好 |
| `qwen-plus` | Bailian | qwen-plus | 0.11 | 0.27 | 8 | **最便宜**;只用于纯中文集合(英文召回明显低) |

> qps 是脚本内置的并发上限,LLM 错误退避两次后丢回失败计数器,不会阻塞批次。

### Token 预估(per doc)

prompt 由"系统提示(~250 tokens) + Title(≤240 chars ≈ 80 tokens) +
正文 1500 chars(≈ 500 tokens)"构成,output 是 JSON 列表(≤200 tokens):

| 段 | 平均 tokens |
|---|---:|
| system prompt | ~250 |
| title + body | ~500–700(中文≈ 1.4×字符,英文≈ 0.25×字符) |
| **per-doc input** | **~750–950** |
| **per-doc output** | **~30–80**(命中多则偏 80,空标偏 30) |

**单条成本(in 850 tok / out 50 tok 假设):**

| 模型 | $/doc | 10k docs | 100k docs |
|---|---:|---:|---:|
| claude-sonnet | $0.003305 | $33.05 | $330.5 |
| claude-haiku | $0.001100 | $11.00 | $110.0 |
| gpt-5 | $0.002625 | $26.25 | $262.5 |
| gpt-5-mini | $0.000420 | $4.20 | $42.0 |
| gemini-pro | $0.001563 | $15.63 | $156.3 |
| gemini-flash | $0.000380 | $3.80 | $38.0 |
| deepseek-v3 | $0.000285 | $2.85 | $28.5 |
| qwen-plus | $0.000107 | $1.07 | $10.7 |

### 全库目标 ≈ 86 万条空标的预估总价

(含 jinmen.oversea_reports 的 72 万条;若 jinmen 用规则解决,实际 LLM 目标
约 14 万条 → 全表价 / 6)

| 模型 | 全库 86 万 | 仅非-jinmen 14 万 |
|---|---:|---:|
| claude-sonnet | **$2,840** | $463 |
| claude-haiku | $946 | $154 |
| gemini-flash | $327 | $53 |
| deepseek-v3 | $245 | $40 |
| qwen-plus | **$92** | **$15** |

> ⚠️ 这里只算 LLM 调用费,不含失败重试。实测 `gemini-flash` 上一次小样
> 失败率 ~1%,基本可忽略。

### 推荐组合(按成本/产出比)

**默认推荐 `qwen-plus`** —— 全库 86 万条只要 ~$92,中文集合召回足够;
英文研报会比 `claude-haiku` 召回低 5–10pp 但成本低 10×,等量预算下覆盖
更多文档。EN-only 集合后跑一遍 `claude-haiku --force-relabel` 补差即可。

1. **一刀切方案(推荐) — 全部空标 ≈ 14 万条(不含 jinmen.oversea_reports)**
   - 模型:**`qwen-plus`**
   - 预估:**$15**,~5h(qps=8 单线)
   - 命中率预估:35–50%(规则没解决的部分多半正文里有具体公司)
   - 跑完后看 `_unmatched_raw` Top 30 + 抽样命中,
     如果发现 `alphapai.reports` 命中率明显偏低,再用 `claude-haiku --force-relabel`
     单独重跑(增量 ~$33,3-5pp 召回提升)
2. **海外研报 — `jinmen.oversea_reports`(724k)** 单独决策
   - 模型:**`qwen-plus`** 或 **`gemini-flash`**(中英混合)
   - 预估:$78–$275(分批跑,每天 30k 条左右)
   - **注意:** 命中率历来低(标题正则只抓到 5%),LLM 也只能把
     真正提到具体股票的几万条捞出来;期望 hit-rate 5–10%
3. **若有充裕预算追求最高召回 — `claude-haiku` 全库**
   - 预估:$946 (86 万条) / $154 (非 jinmen 14 万条)
   - 适用:对最终覆盖率敏感的场景(如 KB 检索精准度)

### 使用示例

```bash
# Smoke test(50 条 + 抽样估算 + 必须确认;不填 --model 走默认 qwen-plus):
PYTHONPATH=. python3 scripts/llm_tag_tickers.py \
    --model qwen-plus \
    --collection gangtise.chief_opinions \
    --max-docs 50

# 交互模式(模型 / 上限 / 预算 都问你,默认就是 qwen-plus):
PYTHONPATH=. python3 scripts/llm_tag_tickers.py \
    --source gangtise

# 默认方案:全部空标用 qwen-plus 跑(预算硬停 $20):
PYTHONPATH=. python3 scripts/llm_tag_tickers.py \
    --model qwen-plus \
    --source all \
    --max-cost-usd 20 \
    --yes

# Dry-run(LLM 跑但不写 Mongo,看抽样命中):
PYTHONPATH=. python3 scripts/llm_tag_tickers.py \
    --model qwen-plus \
    --source alphapai \
    --max-docs 100 \
    --dry-run --yes

# 后续补救:英文研报用 claude-haiku 重跑提升召回:
PYTHONPATH=. python3 scripts/llm_tag_tickers.py \
    --model claude-haiku \
    --collection alphapai.reports,alphaengine.foreign_reports \
    --force-relabel --max-cost-usd 30 --yes
```

### 输出 / 审计

- 进度日志:`stdout`,每 BATCH=100 条打一行
- run log:`logs/llm_tag/<ts>_<model_key>.json`(包含 in/out tokens、cost、
  failures、目标列表 —— 跨多次跑做累计统计)
- 写入字段:
  - `_canonical_tickers`(LLM 输出 → normalize_with_unmatched 后的 canonical)
  - `_canonical_tickers_at`(本次跑的 UTC 时间)
  - `_unmatched_raw`(LLM 编出来但 ticker_normalizer 不认的 ticker)
  - `_canonical_extract_source: "<source>_llm:<model_key>"`(provenance)

### 回滚 / 重跑

```python
# 回滚某次 LLM 打标(例如发现 deepseek-v3 误打太多)
db.<coll>.update_many(
    {"_canonical_extract_source": {"$regex": "_llm:deepseek-v3$"}},
    {"$set": {"_canonical_tickers": [], "_canonical_extract_source": "<source>"}}
)
# 接着用更准的模型重跑,加 --force-relabel 让它再处理这些文档
PYTHONPATH=. python3 scripts/llm_tag_tickers.py \
    --model claude-haiku --collection <coll> --force-relabel --max-cost-usd 30 --yes
```

### 已知限制

1. **正文太长会被截断到 1500 chars** —— 大部分文档主体股票在前几段,
   但深度研报里的次级 ticker 可能漏掉。需要时把 `_BODY_CHAR_CAP` 调到 3000
   (input token 翻倍,成本也翻倍)
2. **LLM "幻觉" ticker** —— 例如把 `Wonderful Industries` 编成 `WIND.US`
   这种不存在的代号。`normalize_with_unmatched` 在 alias 表里查不到就丢
   `_unmatched_raw`,**不会污染 canonical**,但需要定期看 `_unmatched_raw`
   找规律(类似 enrich_tickers 的 `--report-unmatched` 思路)
3. **不处理 `wechat_articles`** —— 信噪比过低,显式从 SOURCES 里拿掉
4. **JSON 解析失败 0.5%–2%** —— 主要是 LLM 在罕见输入上输出非 JSON;
   单条失败计入 `budget.failures`,不写回,下次再跑会自动 pick up
