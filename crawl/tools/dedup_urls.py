#!/usr/bin/env python3
"""URL 去重 + 简单筛选工具.

用法:
  # 从 stdin
  pbpaste | python3 dedup_urls.py
  cat urls.txt | python3 dedup_urls.py

  # 从文件
  python3 dedup_urls.py urls.txt

  # 保留全部重复 (看频次)
  python3 dedup_urls.py urls.txt --show-freq

规则 (黑名单 path 子串):
  authorization/      用户鉴权/会员
  config/user/        用户配置
  point/user/         积分系统
  daily-login         每日登录
  menu/               菜单
  whitelist           白名单
  pending-count       待办数
  aiTask/completed/disconnected  心跳/长连
  permissions/approval           审批
  search/history                 搜索历史
  sentry.io                      埋点

修改 BLACKLIST 列表即可调整规则.
"""
import sys
import re
from urllib.parse import urlparse
from collections import Counter

BLACKLIST = [
    "authorization/",
    "config/user/",
    "point/user/",
    "daily-login",
    "menu/",
    "whitelist",
    "pending-count",
    "aitask/completed/disconnected",   # 心跳/长连, 注意全部小写匹配
    "aitask/completed",
    "permissions/approval",
    "search/history",
    "sentry.io",
]

# 只保留这些域 (空列表 = 不过滤域)
ALLOWED_HOSTS_PATTERN = []


def normalize(url: str) -> str:
    """把 URL 规约到 path (去 scheme/host/query), 用于去重"""
    u = urlparse(url.strip())
    return u.path


def is_useful(url: str) -> bool:
    u = urlparse(url.strip())
    if ALLOWED_HOSTS_PATTERN and not ALLOWED_HOSTS_PATTERN.search(u.netloc):
        return False
    lower = url.lower()
    for pat in BLACKLIST:
        if pat in lower:
            return False
    return True


def main():
    args = sys.argv[1:]
    show_freq = "--show-freq" in args
    args = [a for a in args if not a.startswith("--")]

    if args:
        text = open(args[0], encoding="utf-8").read()
    else:
        text = sys.stdin.read()

    lines = [ln.strip() for ln in text.splitlines() if ln.strip().startswith("http")]
    total = len(lines)

    # 先按完整 URL 计数 (含 query) 然后按 path 再去重
    path_counter = Counter()
    url_by_path = {}
    for url in lines:
        p = normalize(url)
        path_counter[p] += 1
        url_by_path.setdefault(p, url)

    useful = [(p, url_by_path[p], cnt) for p, cnt in path_counter.items()
              if is_useful(url_by_path[p])]
    filtered = [(p, url_by_path[p], cnt) for p, cnt in path_counter.items()
                if not is_useful(url_by_path[p])]

    # 按频次排序, 次数少的往前 (越像一次性业务调用)
    useful.sort(key=lambda x: (x[2], x[0]))

    print(f"# 输入 {total} 条, 去重 (按 path) 后 {len(path_counter)} 条, "
          f"有用 {len(useful)}, 过滤 {len(filtered)}")
    print()
    print("=" * 60)
    print("【有用接口】 (freq: 该 path 出现次数)")
    print("=" * 60)
    for path, url, cnt in useful:
        tag = "" if cnt == 1 else f" [x{cnt}]"
        if show_freq:
            print(f"  ({cnt}) {url}")
        else:
            print(f"  {url}")

    print()
    print("=" * 60)
    print("【已过滤】 (参考)")
    print("=" * 60)
    for path, url, cnt in sorted(filtered, key=lambda x: -x[2]):
        # 标记原因
        reason = next((p for p in BLACKLIST if p in url.lower()), "域名不符")
        tag = "" if cnt == 1 else f" [x{cnt}]"
        print(f"  {url}{tag}  ← {reason}")


if __name__ == "__main__":
    main()
