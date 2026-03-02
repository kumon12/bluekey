"""
Create trend_signals.json from recent Google News RSS results.

This adds "latest trend" signals (e.g., 로봇, 피지컬AI) that are merged by build_themes.py.
"""

from __future__ import annotations

import argparse
import json
import re
import warnings
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


HEADERS = {"User-Agent": "Mozilla/5.0"}

CORE_NEWS_KEYWORDS: dict[str, list[str]] = {
    "피지컬AI": ["피지컬 ai", "physical ai", "휴머노이드", "embodied ai"],
    "AI": ["ai", "인공지능", "생성형", "llm", "추론"],
    "로봇": ["로봇", "휴머노이드", "협동로봇", "자동화"],
    "자동차": ["자동차", "완성차", "전기차", "ev", "자율주행", "sdv"],
    "2차전지": ["2차전지", "이차전지", "배터리", "양극재", "음극재"],
    "반도체": ["반도체", "hbm", "파운드리", "메모리", "칩"],
    "HBM": ["hbm", "고대역폭메모리", "high bandwidth memory"],
    "전력인프라": ["전력", "변압기", "전력망", "송전", "배전"],
    "방산": ["방산", "국방", "미사일", "항공우주"],
    "바이오": ["바이오", "제약", "신약", "진단", "의료기기"],
    "IT": ["클라우드", "saas", "플랫폼", "데이터센터", "소프트웨어"],
}

NOISY_TITLE_PATTERNS = [
    "etf 시황",
    "테마시황",
    "테마추적",
    "증시요약",
    "마감시황",
    "급등종목",
    "관련주",
    "주도테마",
    "주식 초고수",
]


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: Path, data: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower().strip())


def normalize_match_text(text: str) -> str:
    return re.sub(r"[^0-9a-z가-힣]+", "", clean_text(text))


def stock_aliases(stock_name: str) -> list[str]:
    name = stock_name.strip()
    aliases = {name}
    if name.startswith("SK"):
        aliases.add(name.replace("SK", "에스케이", 1))
    if "하이닉스" in name:
        aliases.add("하이닉스")
    if name == "삼성전자":
        aliases.add("삼성")
    if name == "현대차":
        aliases.add("현대자동차")
    return sorted(x for x in aliases if x)


def is_noisy_market_title(title: str) -> bool:
    t = clean_text(title)
    return any(p in t for p in NOISY_TITLE_PATTERNS)


def fetch_naver_top_marketcap_codes(limit: int = 120) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    seen = set()
    for sosok in (0, 1):  # 0 KOSPI, 1 KOSDAQ
        for page in range(1, 51):
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, headers=HEADERS, timeout=20)
                res.raise_for_status()
                soup = BeautifulSoup(res.content.decode("euc-kr", "replace"), "html.parser")
                table = soup.select_one("table.type_2")
                if table is None:
                    break
                found = 0
                for a in table.select('a[href*="item/main.naver?code="]'):
                    href = a.get("href", "")
                    if "code=" not in href:
                        continue
                    code = href.split("code=")[-1].split("&")[0].strip()
                    name = a.get_text(strip=True)
                    if not code or not name or code in seen:
                        continue
                    seen.add(code)
                    rows.append((code, name))
                    found += 1
                    if len(rows) >= limit:
                        return rows
                if found == 0:
                    break
            except Exception:
                break
    return rows


def resolve_name_by_code(code: str) -> str:
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        res.raise_for_status()
        soup = BeautifulSoup(res.content.decode("utf-8", "replace"), "html.parser")
        el = soup.select_one(".wrap_company h2 a")
        if el:
            name = el.get_text(strip=True)
            if name:
                return name
    except Exception:
        pass
    return code


def recency_weight(pub_date_text: str, window_days: int) -> float:
    if not pub_date_text:
        return 0.6
    try:
        dt = parsedate_to_datetime(pub_date_text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        days = max(0.0, (now - dt).total_seconds() / 86400.0)
        if days > window_days:
            return 0.0
        # Linear decay with floor.
        return max(0.25, 1.0 - (days / max(1.0, float(window_days))))
    except Exception:
        return 0.6


def build_theme_keywords(theme_rules: dict[str, list[str]]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {k: list(v) for k, v in CORE_NEWS_KEYWORDS.items()}
    for theme, kws in theme_rules.items():
        keys = out.get(theme, [])
        keys.append(clean_text(theme))
        for kw in kws:
            k = clean_text(str(kw))
            if k:
                keys.append(k)
        out[theme] = sorted(set(keys))
    out.setdefault("로봇", []).extend(["robot"])
    out.setdefault("AI", []).extend(["ai 반도체"])
    for k in list(out.keys()):
        out[k] = sorted(set(clean_text(x) for x in out[k] if clean_text(x)))
    return out


def fetch_news_items(query: str, max_items: int) -> list[dict[str, str]]:
    q = quote_plus(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=ko&gl=KR&ceid=KR:ko"
    res = requests.get(url, headers=HEADERS, timeout=20)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")
    items = []
    for item in soup.find_all("item")[:max_items]:
        title = (item.title.text or "") if item.title else ""
        desc_raw = (item.description.text or "") if item.description else ""
        desc = BeautifulSoup(desc_raw, "html.parser").get_text(" ", strip=True)
        pub = (item.pubDate.text or "") if item.pubDate else ""
        items.append({"title": title, "description": desc, "pubDate": pub})
    return items


def fetch_news_items_for_stock(stock_name: str, max_items: int) -> list[dict[str, str]]:
    queries = [
        f"{stock_name} 주식 테마",
        f"{stock_name} 사업 확장 AI 로봇",
    ]
    merged: list[dict[str, str]] = []
    seen_titles = set()
    for q in queries:
        try:
            items = fetch_news_items(q, max_items=max_items)
        except Exception:
            continue
        for it in items:
            t = it.get("title", "").strip()
            if not t or t in seen_titles:
                continue
            seen_titles.add(t)
            merged.append(it)
    return merged


def score_themes_from_news(
    stock_name: str,
    theme_keywords: dict[str, list[str]],
    window_days: int,
    max_items: int,
) -> list[dict[str, Any]]:
    try:
        news = fetch_news_items_for_stock(stock_name=stock_name, max_items=max_items)
    except Exception:
        return []

    theme_score: dict[str, float] = {}
    evidence: dict[str, list[str]] = {}
    aliases = [normalize_match_text(x) for x in stock_aliases(stock_name)]
    for row in news:
        title = row.get("title", "")
        if is_noisy_market_title(title):
            continue

        text_raw = f"{title} {row.get('description', '')}"
        text = clean_text(text_raw)
        text_match = normalize_match_text(text_raw)
        if aliases and not any(a and a in text_match for a in aliases):
            continue

        w = recency_weight(row.get("pubDate", ""), window_days)
        if w <= 0:
            continue
        for theme, keys in theme_keywords.items():
            hits = [k for k in keys if k and k in text]
            if not hits:
                continue
            inc = 0.8 + (0.3 * min(len(hits), 3))
            theme_score[theme] = theme_score.get(theme, 0.0) + (inc * w)
            ev = evidence.setdefault(theme, [])
            ev.append(f"news:{row['title'][:80]}")

    out: list[dict[str, Any]] = []
    for theme, score in sorted(theme_score.items(), key=lambda x: x[1], reverse=True):
        if score < 0.9:
            continue
        out.append(
            {
                "name": theme,
                "score": round(score, 3),
                "evidence": sorted(set(evidence.get(theme, [])))[:5],
                "matched_text": "",
            }
        )
    return out[:5]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh trend_signals.json from recent news.")
    parser.add_argument("--rules", default="theme_rules.json", help="Theme rules JSON")
    parser.add_argument("--output", default="trend_signals.json", help="Output JSON")
    parser.add_argument("--top-n", type=int, default=120, help="Top market-cap stocks to scan")
    parser.add_argument("--window-days", type=int, default=120, help="Recency window for trend scoring")
    parser.add_argument("--max-items", type=int, default=12, help="Max RSS items per stock")
    parser.add_argument("--codes", default="", help="Optional comma-separated stock codes")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rules = load_json(Path(args.rules), {})
    if not isinstance(rules, dict) or not rules:
        raise RuntimeError("theme_rules.json is missing or invalid.")

    theme_keywords = build_theme_keywords(rules)

    if args.codes.strip():
        code_list = [c.strip() for c in args.codes.split(",") if c.strip()]
        universe = []
        name_map = dict(fetch_naver_top_marketcap_codes(limit=2000))
        for code in code_list:
            name = name_map.get(code) or resolve_name_by_code(code)
            universe.append((code, name))
    else:
        universe = fetch_naver_top_marketcap_codes(limit=args.top_n)

    signals: dict[str, list[dict[str, Any]]] = {}
    for idx, (code, name) in enumerate(universe, start=1):
        trend = score_themes_from_news(
            stock_name=name,
            theme_keywords=theme_keywords,
            window_days=args.window_days,
            max_items=args.max_items,
        )
        if trend:
            signals[code] = trend
        if idx % 20 == 0:
            print(f"Processed {idx}/{len(universe)}")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window_days": args.window_days,
        "signals": signals,
    }
    save_json(Path(args.output), output)
    print(f"Saved {args.output} with {len(signals)} stock signals.")


if __name__ == "__main__":
    main()
