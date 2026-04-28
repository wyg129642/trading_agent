"""In-scope IR-filings ticker registry — single source of truth.

41 portfolio holdings across 4 markets (US / HK / JP / KR). A-shares excluded
intentionally (user owns the call as of 2026-04-28). Each entry carries the
per-source identifier needed by the matching scraper (CIK for SEC, internal
HKEX stockId resolved at runtime, secCode for EDINET, corp_code for DART).

The canonical ticker (`<CODE>.<MKT>` form) is what gets stamped onto every
crawled doc's ``_canonical_tickers`` field via ``crawl/ticker_tag.py`` so that
downstream ``kb_search`` / ``kb_fetch_document`` filters by ticker just work.

Update this file when:
  - a new holding is added to the portfolio
  - DART corp_code or HKEX stockId changes (shouldn't happen after IPO, but
    re-listings or M&A can re-issue codes)
  - a new market is added to scope (A股 expansion → uncomment KR/JP-style entries)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class IrTicker:
    canonical: str                       # primary key: e.g. "INTC.US", "01347.HK"
    market: str                          # "US" | "HK" | "JP" | "KR"
    source: str                          # primary scraper: sec_edgar / hkex / edinet / tdnet / dart
    listing_code: str                    # exchange-native code: "INTC", "01347", "5801", "005930"
    name_en: str
    name_local: str = ""
    # Source-specific lookup keys
    cik: str = ""                        # SEC: 10-digit zero-padded
    hkex_stock_id: int = 0               # HKEX: internal `i` (resolved at runtime via activestock json)
    edinet_code: str = ""                # EDINET: E-prefix code (e.g. "E01332")
    sec_code: str = ""                   # EDINET: 5-char (4-digit + check), e.g. "58010"
    corp_code: str = ""                  # DART: 8-digit
    # FPI / IFRS flag for SEC tickers (changes form set: 20-F instead of 10-K, ifrs-full taxonomy)
    sec_fpi: bool = False
    # Aliases for ticker_normalizer rebuild prompts
    aliases: list[str] = field(default_factory=list)


# ---------- US (22) — SEC EDGAR ----------
US_TICKERS: list[IrTicker] = [
    IrTicker("GLW.US",   "US", "sec_edgar", "GLW",   "Corning Inc",                cik="0000024741", aliases=["康宁", "Corning"]),
    IrTicker("COHR.US",  "US", "sec_edgar", "COHR",  "Coherent Corp",              cik="0000820318", aliases=["高意"]),
    IrTicker("AXTI.US",  "US", "sec_edgar", "AXTI",  "AXT Inc",                    cik="0001051627", aliases=["AXT"]),
    IrTicker("INTC.US",  "US", "sec_edgar", "INTC",  "Intel Corp",                 cik="0000050863", aliases=["英特尔", "Intel"]),
    IrTicker("SNDK.US",  "US", "sec_edgar", "SNDK",  "Sandisk Corp",               cik="0002023554", aliases=["西部数据", "Sandisk", "Western Digital"]),
    IrTicker("MU.US",    "US", "sec_edgar", "MU",    "Micron Technology Inc",      cik="0000723125", aliases=["美光", "Micron"]),
    IrTicker("BE.US",    "US", "sec_edgar", "BE",    "Bloom Energy Corp",          cik="0001664703", aliases=["Bloom Energy"]),
    IrTicker("AAOI.US",  "US", "sec_edgar", "AAOI",  "Applied Optoelectronics",    cik="0001158114", aliases=["应用光电"]),
    IrTicker("TSM.US",   "US", "sec_edgar", "TSM",   "Taiwan Semiconductor Mfg",   cik="0001046179", aliases=["台积电", "TSMC"], sec_fpi=True),
    IrTicker("GOOGL.US", "US", "sec_edgar", "GOOGL", "Alphabet Inc",               cik="0001652044", aliases=["谷歌", "Alphabet", "Google"]),
    IrTicker("SGML.US",  "US", "sec_edgar", "SGML",  "Sigma Lithium Corp",         cik="0001848309", aliases=["Sigma Lithium"], sec_fpi=True),
    IrTicker("LITE.US",  "US", "sec_edgar", "LITE",  "Lumentum Holdings Inc",      cik="0001633978", aliases=["朗美通", "Lumentum"]),
    IrTicker("CELC.US",  "US", "sec_edgar", "CELC",  "Celcuity Inc",               cik="0001603454", aliases=["Celcuity"]),
    IrTicker("NEOV.US",  "US", "sec_edgar", "NEOV",  "NeoVolta Inc",               cik="0001748137", aliases=["NeoVolta"]),
    IrTicker("MRVL.US",  "US", "sec_edgar", "MRVL",  "Marvell Technology",         cik="0001835632", aliases=["美满电子", "Marvell"]),
    IrTicker("TSEM.US",  "US", "sec_edgar", "TSEM",  "Tower Semiconductor",        cik="0000928876", aliases=["高塔半导体", "Tower Semi"], sec_fpi=True),
    IrTicker("CIEN.US",  "US", "sec_edgar", "CIEN",  "Ciena Corp",                 cik="0000936395", aliases=["Ciena"]),
    IrTicker("NOK.US",   "US", "sec_edgar", "NOK",   "Nokia Corp",                 cik="0000924613", aliases=["诺基亚", "Nokia"], sec_fpi=True),
    IrTicker("UCTT.US",  "US", "sec_edgar", "UCTT",  "Ultra Clean Holdings",       cik="0001275014", aliases=["Ultra Clean"]),
    IrTicker("PSIX.US",  "US", "sec_edgar", "PSIX",  "Power Solutions Intl",       cik="0001137091", aliases=["Power Solutions"]),
    IrTicker("NBIS.US",  "US", "sec_edgar", "NBIS",  "Nebius Group N.V.",          cik="0001513845", aliases=["Nebius"], sec_fpi=True),
    IrTicker("CRCL.US",  "US", "sec_edgar", "CRCL",  "Circle Internet Group",      cik="0001876042", aliases=["Circle", "USDC"]),
]


# ---------- HK (14) — HKEXnews ----------
# hkex_stock_id stays 0 here; the scraper resolves it lazily via activestock_sehk_e.json
# at startup and caches the map locally. Avoids embedding a value that can drift.
HK_TICKERS: list[IrTicker] = [
    IrTicker("06869.HK", "HK", "hkex", "06869", "Yangtze Optical Fibre",          name_local="长飞光纤光缆"),
    IrTicker("03337.HK", "HK", "hkex", "03337", "Anton Oilfield Services",        name_local="安东油田服务"),
    IrTicker("06693.HK", "HK", "hkex", "06693", "Chifeng Jilong Gold Mining",     name_local="赤峰黄金"),
    IrTicker("02256.HK", "HK", "hkex", "02256", "Abbisko Cayman Ltd",             name_local="和誉医药"),
    IrTicker("02142.HK", "HK", "hkex", "02142", "Harbour BioMed",                 name_local="和铂医药"),
    IrTicker("01347.HK", "HK", "hkex", "01347", "Hua Hong Semiconductor",         name_local="华虹半导体"),
    IrTicker("02513.HK", "HK", "hkex", "02513", "Knowledge Atlas / Zhipu AI",     name_local="智谱"),
    IrTicker("03939.HK", "HK", "hkex", "03939", "Wanguo Gold Group",              name_local="万国黄金集团"),
    IrTicker("01164.HK", "HK", "hkex", "01164", "CGN Mining",                     name_local="中广核矿业"),
    IrTicker("01866.HK", "HK", "hkex", "01866", "China XLX Fertiliser",           name_local="中国心连心化肥"),
    IrTicker("03330.HK", "HK", "hkex", "03330", "Lingbao Gold Group",             name_local="灵宝黄金"),
    IrTicker("01477.HK", "HK", "hkex", "01477", "Ocumension Therapeutics",        name_local="欧康维视生物"),
    IrTicker("00100.HK", "HK", "hkex", "00100", "MiniMax (00100)",                name_local="MINIMAX-W"),
    IrTicker("02245.HK", "HK", "hkex", "02245", "Lygend Resources & Tech",        name_local="力勤资源"),
]


# ---------- JP (2) — TDnet (timely) + EDINET (statutory) ----------
JP_TICKERS: list[IrTicker] = [
    IrTicker("5801.JP", "JP", "edinet", "5801", "Furukawa Electric Co Ltd",
             name_local="古河電気工業", edinet_code="E01332", sec_code="58010",
             aliases=["古河电工", "古河电气"]),
    IrTicker("285A.JP", "JP", "edinet", "285A", "Kioxia Holdings Corp",
             name_local="キオクシアホールディングス", edinet_code="E35948", sec_code="285A0",
             aliases=["铠侠", "Kioxia"]),
]


# ---------- AU (1) — ASX (Markit Digital JSON) ----------
AU_TICKERS: list[IrTicker] = [
    IrTicker("SGQ.AU", "AU", "asx", "SGQ", "ST George Mining Ltd",
             aliases=["ST George Mining", "St George Mining", "SGM"]),
]


# ---------- KR (3) — DART ----------
# Samsung 005930 = ordinary; preferred 005935 has a separate corp_code that we ignore
# (filings for 005930 cover both share classes, per dart-fss community wisdom).
KR_TICKERS: list[IrTicker] = [
    # KR tickers use the existing repo convention: .KS for KOSPI / .KQ for KOSDAQ
    # (matches `aliases.json` + tushare `_basic` conventions). Avoid `.KR` to
    # not fork a new market suffix.
    IrTicker("005930.KS", "KR", "dart", "005930", "Samsung Electronics Co Ltd",
             name_local="삼성전자", corp_code="00126380",
             aliases=["三星电子", "Samsung"]),
    IrTicker("000660.KS", "KR", "dart", "000660", "SK Hynix Inc",
             name_local="SK하이닉스", corp_code="00164779",
             aliases=["SK海力士", "SK Hynix", "海力士"]),
    IrTicker("011930.KQ", "KR", "dart", "011930", "SHINSUNG E&G Co Ltd",
             name_local="신성이엔지", corp_code="",   # resolved via cached corpCode.xml
             aliases=["SHINSUNG E&G"]),
]


ALL_TICKERS: list[IrTicker] = US_TICKERS + HK_TICKERS + JP_TICKERS + KR_TICKERS + AU_TICKERS

BY_CANONICAL: dict[str, IrTicker] = {t.canonical: t for t in ALL_TICKERS}
BY_LISTING_CODE: dict[tuple[str, str], IrTicker] = {(t.market, t.listing_code): t for t in ALL_TICKERS}


def for_source(source: str) -> list[IrTicker]:
    """All tickers whose primary scraper is `source`. JP tickers appear under
    ``edinet`` (primary) but `tdnet` scraper also iterates them via this helper
    when called with ``source="tdnet"`` — handle that as a special case below."""
    if source == "tdnet":
        return JP_TICKERS                # TDnet pulls the same JP issuers as EDINET
    return [t for t in ALL_TICKERS if t.source == source]


def for_market(market: str) -> list[IrTicker]:
    return [t for t in ALL_TICKERS if t.market == market]


def by_listing_code(market: str, code: str) -> Optional[IrTicker]:
    return BY_LISTING_CODE.get((market, code))


__all__ = [
    "IrTicker", "ALL_TICKERS", "BY_CANONICAL", "BY_LISTING_CODE",
    "US_TICKERS", "HK_TICKERS", "JP_TICKERS", "KR_TICKERS", "AU_TICKERS",
    "for_source", "for_market", "by_listing_code",
]
