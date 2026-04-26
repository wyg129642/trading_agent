# 规则化打标扩展 (2026-04-24)

## 背景

`_canonical_tickers` 字段 100% 写入率,但**~52%** 的文档是空数组 `[]`
—— 上游平台本来就没给个股,或市场后缀不在 normalizer 认识的 canonical 表里。
人工抽查 18 个集合的标题规律后,发现**绝大部分可以靠规则 + 别名扩展捞回**,
不需要上 LLM NER 打标。本次改动做了三件事:

1. 扩 canonical MARKET 枚举(从 21 个 → 51 个,覆盖全球主要交易所)
2. 加 Bloomberg / Reuters / Jinmen 自用市场后缀 → canonical 的映射表
3. 加标题正则兜底 —— 当结构化字段空时扫 `(CODE.MARKET)` / `(CODE:MARKET)`

## 代码改动

### `backend/app/services/ticker_normalizer.py`

**扩 `_KNOWN_MARKETS`(21 → 51)。** 新增 30 个:

```
IN BR ES DK SG TH MY ID PH VN TR MX AR CL PE CO
SA AE EG ZA QA IL HU CZ PL BE PT IE GR RU
```

**新增 `_EXCHANGE_SUFFIX_MAP`(165 条映射)。** 按来源分类:

| 分类 | 样例 | 数量 |
|---|---|---:|
| Bloomberg 1 字母 | `.N→US`、`.T→JP`、`.S→CH`、`.L→GB`、`.J→ZA`、`.F→DE` | 7 |
| Bloomberg 2 字母 | `.PA→FR`、`.AS→NL`、`.MI→IT`、`.AX→AU`、`.CO→DK`、`.ST→SE`、`.KL→MY`、`.BK→TH`、`.IS→TR`、`.BO→IN`、`.NS→IN`、`.KQ→KS`、`.MX→MX`、`.TA→IL`、`.BU→HU` | 约 60 |
| Refinitiv 歧义后缀(按 Jinmen 语境) | `.SA→BR`(不是 Saudi)、`.SE→SA`(不是 Sweden;Sweden 用 `.ST`) | 2 |
| Jinmen 自造 3 字母 | `.JPN→JP`、`.BRA→BR`、`.IND→IN`、`.ESP→ES`、`.CHL→CL`、`.MEX→MX`、`.AUT→AT`、`.CAN→CA`、`.GBR→GB`、`.CHE→CH`、`.NLD→NL`、`.SWE→SE`、`.SAU→SA` 等 | 约 40 |
| canonical 自恒等映射 | `.SH→SH`、`.HK→HK` 等 | 51 |

**明确留空(避免误命中):**

- `.A`、`.P`、`.V`(在 "S.p.A.", "J.P.", "N.V." 这类公司后缀里高频出现)
- `.CA`(有歧义:既可能是 Cairo 也可能是 Canada;Canada 走冒号形式 `CODE:CA`)
- 纯数字市场后缀(不合理)

**改 `_parse_dotted()`:**

- 从接受 1–3 字母后缀扩到 1–4 字母(覆盖 `.JPN` / `.BRA` / `.CHL` 等)
- 代码部分允许内部点号(`TECK.B` 这种 B 股代码)
- 走 `_resolve_market_suffix()` 统一解析

**新增 `_parse_colon_suffix()`:** 解 `(ARX:CA)` / `(TECK.B:CA)` 冒号格式
—— 这是 AlphaPai roadshows 的特殊习惯,其他平台不这么写。

**新增 `extract_tickers_from_text(text)`:** 标题/自由文本里找 `(CODE.MARKET)`
或 `(CODE:MARKET)`,半角全角括号都认。只匹配带括号的形式,不匹配裸 `CODE.MARKET`
(那样会把 "S.p.A" 中的 ".A" 也匹出来)。

### `scripts/enrich_tickers.py`

1. Projection 增加 `title` / `title_cn` / `title_en`
2. 结构化 extractor 返回空时,依次扫这三个字段用 `extract_tickers_from_text` 兜底
3. 兜底命中时写 `_canonical_extract_source: "{source}_title"`(审计用,将来可回滚)
4. 用 `bulk_write(ordered=False)` 批量写入(每 500 条一批,速度 ~90 docs/s)
5. 新增 `--only-empty` CLI 选项:只处理当前 `_canonical_tickers: []` 的文档
   —— 加了新后缀映射后,用这个选项重跑比全量 `--incremental` 便宜多了

## 改动前基线(2026-04-24 运行前)

排除 `jinmen.oversea_reports`(下载未完)+ `alphapai.wechat_articles`(微信低信噪比)
后的 7 平台 20 个集合:

| 集合 | 总数 | 已打标 | 无标签 | 无标签率 |
|---|---:|---:|---:|---:|
| alphapai.comments | 12,688 | 10,347 | 2,341 | 18.5% |
| alphapai.roadshows | 48,794 | 33,517 | 15,277 | 31.3% |
| alphapai.reports | 51,593 | 3,292 | 48,301 | 93.6% |
| jinmen.meetings | 14,929 | 8,245 | 6,684 | 44.8% |
| jinmen.reports | 11,630 | 5,636 | 5,994 | 51.5% |
| jiuqian.forum | 2,748 | 2,010 | 738 | 26.9% |
| jiuqian.research | 68 | 7 | 61 | 89.7% |
| third-bridge.interviews | 148 | 55 | 93 | 62.8% |
| funda.posts | 361 | 353 | 8 | 2.2% |
| funda.earnings_reports | 1,079 | 1,079 | 0 | 0.0% |
| funda.earnings_transcripts | 3,355 | 3,355 | 0 | 0.0% |
| funda.sentiments | 10,846 | 10,548 | 298 | 2.7% |
| gangtise.summaries | 50,688 | 28,945 | 21,743 | 42.9% |
| gangtise.researches | 39,219 | 7,049 | 32,170 | 82.0% |
| gangtise.chief_opinions | 52,422 | 11,245 | 41,177 | 78.5% |
| acecamp.articles | 25,199 | 20,271 | 4,928 | 19.6% |
| alphaengine.summaries | 1,421 | 747 | 674 | 47.4% |
| alphaengine.china_reports | 1,652 | 1,072 | 580 | 35.1% |
| alphaengine.foreign_reports | 1,200 | 156 | 1,044 | 87.0% |
| alphaengine.news_items | 3,451 | 471 | 2,980 | 86.4% |
| **合计** | **333,491** | **148,400 (44.5%)** | **185,091 (55.5%)** | — |

此外 `jinmen.oversea_reports` 一个集合就有 **763,736 条无标签**(约占全库无标签的 80%),
正则扫描命中标题带 `(CODE.MARKET)` 格式的比例约 5.7%(~43,686 条)。

## 改动后结果(2026-04-25 `--only-empty` 全量跑完)

**总体:**

- 扫描总数:**971,648** 文档(22 个 collection,仅 `_canonical_tickers: []` 的空标文档)
- 新打标(`[]` → 非空):**68,730** 条(总命中率 7.1%)
- 其中来自标题兜底(`*_title` 来源):**40,600** 条 —— **占新增的 59%**,也就是说不上这波规则会少打近六成
- 实测速度:**~112 docs/s**(略高于预估;大部分 docs 是"扫描-无匹配-空数组"快速路径,少量走 bulk_write)
- 全量耗时:约 **2h25m**
- 索引:22 个 collection 全部建好 `_canonical_tickers` 单字段索引
- 异常:0 错误,进程 exit 0

**按集合(从脚本日志):**

| 集合 | 扫描 | 命中 | 命中率 | 其中 title 兜底 |
|---|---:|---:|---:|---:|
| alphapai.roadshows | 14,601 | 0 | 0% | 0 |
| alphapai.reports | 32,598 | 19 | 0% | 0 |
| alphapai.comments | 2,371 | 0 | 0% | 0 |
| alphapai.wechat_articles | 40,564 | 0 | 0% | 0 |
| jinmen.meetings | 6,682 | 0 | 0% | 0 |
| jinmen.reports | 5,517 | 14 | 0% | 14 |
| **jinmen.oversea_reports** | **761,735** | **37,352** | **5%** | **37,324** |
| meritco.forum | 738 | 2 | 0% | 1 |
| meritco.research | 61 | 0 | 0% | 0 |
| thirdbridge.interviews | 93 | 0 | 0% | 0 |
| funda.posts | 9 | 0 | 0% | 0 |
| funda.sentiments | 298 | 15 | 5% | 0 |
| acecamp.articles | 4,928 | 17 | 0% | 16 |
| **alphaengine.summaries** | 674 | **504** | **74%** | 0 |
| alphaengine.china_reports | 580 | 24 | 4% | 10 |
| **alphaengine.foreign_reports** | 1,044 | **519** | **49%** | 27 |
| alphaengine.news_items | 2,980 | 113 | 3% | 0 |
| gangtise.summaries | 21,803 | 4,420 | 20% | 71 |
| gangtise.researches | 32,547 | 4,227 | 12% | 1,288 |
| **gangtise.chief_opinions** | 41,776 | **21,504** | **51%** | 1,849 |
| semianalysis.semianalysis_posts | 49 | 0 | 0% | 0 |
| **合计** | **971,648** | **68,730** | **7.1%** | **40,600** |

**最大受益集合 `jinmen.oversea_reports`:** 37,352 命中里 **37,324 条(99.9%)** 靠标题正则抓到
—— 这些文档结构化字段里根本没 ticker,只有标题文本 `Kakaku.com Inc.(2371.JPN)` 这种。
新正则把它们从"永远打不上标"变成了"打上 5% 标"。

**未匹配 Top 30(跨 22 个 collection 聚合 `_unmatched_raw`):**

| 次数 | 原始字符串 | 说明 |
|---:|---|---|
| 97,144 | `null` | **上游脏数据** —— 字段值就是字符串 `"null"`,extractor 应提前过滤 |
| 3,212 | `821041.SWI` | 申万一级行业指数(非个股) |
| 3,212 | `821018.CI` | 中信一级行业指数(非个股) |
| 1,764 | `821015.CI` | 中信一级 |
| 1,764 | `821046.SWI` | 申万一级 |
| 1,736 | `821035.SWI` | 申万一级 |
| 1,734 | `821026.CI` | 中信一级 |
| 1,550 | `821048.SWI` | 申万一级 |
| 1,530 | `821022.CI` | 中信一级 |
| 1,300 | `821028.CI` | 中信一级 |
| 1,296 | `821055.SWI` | 申万一级 |
| 1,246 | `821045.SWI` | 申万一级 |
| 1,244 | `821014.CI` | 中信一级 |
| 1,222 | `821013.CI` | 中信一级 |
| 1,222 | `821036.SWI` | 申万一级 |
| 1,216 | `821043.SWI` | 申万一级 |
| 1,214 | `821010.CI` / `821025.CI` | 中信一级 |
| 1,212 | `821053.SWI` | 申万一级 |
| 1,086 | `821019.CI` / `821038.SWI` | 行业指数 |
| …下至 802 | 全部为 `821xxx.CI` / `821xxx.SWI` | 中信/申万一级行业指数,**全部非个股** |

**解读:** Top30 的结构极其有信号量:

1. **`null` 字面值 97k 次** —— 上游 JSON 把空值序列化成字符串 `"null"`(不是 JSON null),漏进了
   `_unmatched_raw`。修法:`extractor` 里 `if s in ("null", "", "NULL"): continue`。
2. **剩下 29 个全是 `821xxx.CI` / `821xxx.SWI`** —— 这是 `gangtise.chief_opinions` 的 `aflScr` 字段带出的
   **行业指数代码**,不是个股,不该进 ticker 系统。148,766 条未匹配里绝大部分就是这个。修法三选一:
   - (a) extractor 里 `re.match(r"\d{6}\.(CI|SWI)$", s)` 命中就丢 skip list,`_unmatched_raw` 不记
   - (b) 新开 `_canonical_industry_indices` 字段单独收,作为 category 元数据
   - (c) 扩 `aliases.json` 把 821001–821059 逐行注释成行业标签(信息量高,但得花人工)
3. **真正需要 alias 扩展的候选**(中英混合公司名、无后缀裸代码等)需要**除掉这两类伪未匹配后再做一次 Top N 聚合**
   才能看清 —— 原始 Top30 被 `null` + 行业指数占满,看不到真 alias candidate。

下一步建议 3 的工作量估:先做上面的修法 (a),然后重新聚合 Top 100,真正的公司名 alias 候选才会浮上来。

## 已知遗留问题

1. **`.AUT` 语义疑似上游错误** —— Jinmen 把 Nestle `NESN`、Anheuser-Busch `ABIN`
   打成 `.AUT`(Austria),但这两家分别是 Swiss / Belgian。目前仍按 Jinmen 的标
   注映射到 `AT`(Austria)。后续可以通过 alias 表单点覆盖:`{"NESN.AT": "NESN.CH"}`。

2. **`.CA` 歧义** —— Reuters 用 `.CA` 表示 Cairo(Egypt),老 Bloomberg 有时用 `.CA`
   表示 Canada。当前策略:不映射 `.CA` 到任何市场,让它落在 `_unmatched_raw` 里。
   AlphaPai roadshows 用冒号形式 `(ARX:CA)` → Canada,这部分走 `_parse_colon_suffix`。

3. **单字母 `.A`、`.P`、`.V` 故意不映射** —— 会被 "S.p.A"、"J.P."、"N.V." 这类
   公司法律形式后缀严重误匹配。代价:失掉一些真的 Canadian Venture(`.V`)/ AMEX
   (`.A`)的命中,但保证精度。

4. **仍然 55% 左右的文档保持 `[]` 是正确语义** —— 行业 / 宏观 / 策略 / 周报类文档
   本来就没有个股。别被"覆盖率没到 100%"的直觉误导成硬打标。

## 下一步建议

1. 跑完统计后,把未匹配 Top 30 的原始字符串过一遍 —— 多数是公司名(中英混合),
   可以扩 `aliases.json`(目前 323 行,扩到 1000 行级别能再捞一批)
2. Top 5 高无标签集合里(`alphapai.reports` / `gangtise.chief_opinions`
   / `gangtise.researches` / `gangtise.summaries` / `alphapai.roadshows`)剩下
   没被规则抓到的,主要是**英文研报里公司名作为主题词但无代码** —— 这才是
   真正需要 LLM NER 的战场,估计还有 10–15k 文档
3. `jinmen.oversea_reports` 下载完毕后,再跑一次 `--only-empty`,预计能新增
   约 44k 条(基于 5.7% 标题命中率)

## 审计:如何验证规则打标是否正确

```python
# 查看所有 title-fallback 命中的文档
for dbn, coll in [...]:
    docs = c[dbn][coll].find(
        {"_canonical_extract_source": {"$regex": "_title$"}},
        projection={"title": 1, "_canonical_tickers": 1}
    ).limit(20)
    for d in docs:
        print(d["title"], "→", d["_canonical_tickers"])
```

如果发现误打标(例如 "S.p.A" 里 `.A` 被当成 AMEX),可以:

```python
# 回滚特定来源
c[dbn][coll].update_many(
    {"_canonical_extract_source": f"{source}_title"},
    {"$set": {"_canonical_tickers": [], "_canonical_extract_source": source}}
)
```

然后修正 `_EXCHANGE_SUFFIX_MAP` 或 `extract_tickers_from_text` 的正则后重跑。
