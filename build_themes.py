"""
Build stock/theme data from KRX universe with aggressive classification.

Goals:
1) Cover all KOSPI/KOSDAQ listed stocks.
2) Assign at least 1 theme per stock, up to max_themes (default 5).
3) Prefer Naver short names by code when possible (e.g. 현대자동차 -> 현대차).
4) Keep explainable evidence in theme_details.json.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup


DEFAULT_THEME_RULES = {
    "반도체": ["반도체", "semiconductor", "메모리", "파운드리", "hbm", "칩", "패키징", "후공정"],
    "2차전지": ["2차전지", "이차전지", "배터리", "양극재", "음극재", "전해액", "분리막", "리튬", "니켈"],
    "로봇": ["로봇", "자동화", "스마트팩토리", "협동로봇", "감속기", "서보"],
    "AI": ["ai", "인공지능", "생성형", "llm", "gpu", "추론", "학습", "데이터센터"],
    "IT": ["it", "소프트웨어", "플랫폼", "클라우드", "saas", "erp", "솔루션"],
    "바이오": ["바이오", "제약", "신약", "항체", "진단", "의료기기", "백신", "cdmo"],
    "게임": ["게임", "mmorpg", "모바일게임", "콘솔", "퍼블리싱"],
    "콘텐츠": ["콘텐츠", "엔터", "미디어", "드라마", "영화", "음악", "광고"],
    "자동차": ["자동차", "완성차", "전기차", "자율주행", "차량", "부품", "모빌리티"],
    "방산": ["방산", "국방", "미사일", "항공우주", "레이더", "탄약", "전투기"],
    "조선": ["조선", "선박", "해양플랜트", "lng선", "탱커"],
    "원전": ["원전", "원자력", "smr", "소형모듈원전", "원자로"],
    "전력인프라": ["전력", "변압기", "송전", "배전", "전선", "전력기기", "전력망"],
    "통신": ["통신", "5g", "네트워크", "기지국", "usim"],
    "핀테크": ["핀테크", "결제", "pg", "뱅킹", "증권", "보험", "카드", "간편결제"],
    "유통소비": ["유통", "소매", "백화점", "편의점", "식품", "화장품", "의류", "리테일"],
    "건설인프라": ["건설", "토목", "플랜트", "인프라", "시공", "건자재"],
    "화학소재": ["화학", "정밀화학", "합성수지", "소재", "석유화학"],
    "철강금속": ["철강", "금속", "제강", "스테인리스", "비철금속"],
    "기계장비": ["기계", "장비", "공작기계", "유압", "산업기계"],
    "물류운송": ["물류", "운송", "해운", "항공운송", "택배", "철도"],
}


# Sector keyword based fallback to improve coverage.
SECTOR_THEME_KEYWORDS = {
    "반도체": ["반도체"],
    "2차전지": ["축전지", "전지", "배터리"],
    "로봇": ["로봇", "자동화", "산업용 로봇"],
    "AI": ["인공지능"],
    "IT": ["소프트웨어", "응용 소프트웨어", "시스템 소프트웨어", "정보서비스", "컴퓨터 프로그래밍"],
    "바이오": ["의약품", "의료", "바이오", "진단"],
    "게임": ["게임"],
    "콘텐츠": ["영화", "방송", "광고", "연예", "미디어"],
    "자동차": ["자동차", "트레일러", "자동차부품"],
    "방산": ["무기", "군수", "국방"],
    "조선": ["선박", "보트", "해양"],
    "원전": ["원자력"],
    "전력인프라": ["전기장비", "전력", "전선"],
    "통신": ["통신", "네트워크"],
    "핀테크": ["금융", "보험", "카드", "증권"],
    "유통소비": ["소매", "유통", "식품", "의류", "화장품"],
    "건설인프라": ["건설", "토목", "엔지니어링"],
    "화학소재": ["화학", "고무", "플라스틱", "합성수지"],
    "철강금속": ["철강", "금속", "주조", "압연"],
    "기계장비": ["기계", "장비", "공작기계"],
    "물류운송": ["운송", "창고", "물류", "해운", "항공"],
}


# Extra theme pack to cover renewable/energy transitions.
EXTRA_THEME_RULES = {
    "태양광": ["태양광", "태양전지", "태양전", "solar", "photovoltaic", "태양광모듈", "태양광발전"],
    "친환경에너지": ["신재생", "재생에너지", "그린에너지", "친환경 에너지", "탄소중립", "에너지솔루션"],
    "수소": ["수소", "암모니아", "연료전지", "수전해", "hydrogen"],
}


@dataclass
class StockRecord:
    code: str
    name: str
    market: str
    sector: str = ""
    products: str = ""
    tags: list[str] | None = None


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def merge_with_extra_rules(rules: dict[str, list[str]]) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for theme, kws in (rules or {}).items():
        if isinstance(kws, list):
            merged[str(theme)] = [str(x) for x in kws]
    for theme, kws in EXTRA_THEME_RULES.items():
        current = merged.setdefault(theme, [])
        for kw in kws:
            if kw not in current:
                current.append(kw)
    return merged


def apply_domain_enrichment(
    stock: "StockRecord",
    scores: list[dict[str, Any]],
    rules: dict[str, list[str]],
) -> list[dict[str, Any]]:
    text = clean_text(f"{stock.name} {stock.sector} {stock.products}")
    hints = [
        ("태양광", ["태양광", "태양전지", "태양전", "solar", "photovoltaic", "태양광발전"], 3.2),
        ("친환경에너지", ["신재생", "재생에너지", "친환경", "탄소중립", "그린에너지"], 2.4),
        ("수소", ["수소", "연료전지", "암모니아", "수전해", "hydrogen"], 2.2),
        ("전력인프라", ["전력", "전력망", "송전", "배전", "변압기", "ess"], 1.8),
    ]

    merged = {x["name"]: dict(x) for x in scores}
    for theme, keys, base_score in hints:
        if theme not in rules:
            continue
        hits = [k for k in keys if clean_text(k) in text]
        if not hits:
            continue
        if theme in merged:
            merged[theme]["score"] = round(float(merged[theme].get("score", 0.0)) + base_score, 3)
            evidence = merged[theme].get("evidence", [])
            if not isinstance(evidence, list):
                evidence = []
            evidence.extend([f"domain:{h}" for h in hits[:3]])
            merged[theme]["evidence"] = sorted(set(str(e) for e in evidence))
        else:
            merged[theme] = {
                "name": theme,
                "score": round(base_score, 3),
                "evidence": [f"domain:{h}" for h in hits[:3]],
                "matched_text": text[:280],
            }

    out = list(merged.values())
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def load_trend_signals(path: Path) -> dict[str, list[dict[str, Any]]]:
    """
    Accepted formats:
    1) {"signals": {"005380": [{"name":"로봇","score":2.0,...}]}}
    2) {"005380": [{"name":"로봇","score":2.0,...}]}
    """
    raw = load_json(path, {})
    if not isinstance(raw, dict):
        return {}
    if isinstance(raw.get("signals"), dict):
        body = raw.get("signals", {})
    else:
        body = raw

    out: dict[str, list[dict[str, Any]]] = {}
    for code, rows in body.items():
        if not isinstance(rows, list):
            continue
        cleaned: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name", "")).strip()
            if not name:
                continue
            try:
                score = float(row.get("score", 0.0))
            except Exception:
                score = 0.0
            evidence = row.get("evidence", [])
            if not isinstance(evidence, list):
                evidence = [str(evidence)]
            cleaned.append(
                {
                    "name": name,
                    "score": round(score, 3),
                    "evidence": [str(x) for x in evidence if str(x).strip()],
                    "matched_text": str(row.get("matched_text", ""))[:280],
                }
            )
        if cleaned:
            out[str(code).strip()] = cleaned
    return out


def merge_trend_scores(
    stock: StockRecord,
    base_scores: list[dict[str, Any]],
    trend_scores: list[dict[str, Any]],
    trend_weight: float,
) -> list[dict[str, Any]]:
    if not trend_scores:
        return base_scores

    merged: dict[str, dict[str, Any]] = {x["name"]: dict(x) for x in base_scores}
    base_names = {x["name"] for x in base_scores}
    sector_blob = clean_text(f"{stock.sector} {stock.products}")

    def is_allowed_new_theme(theme_name: str) -> bool:
        if theme_name in base_names:
            return True

        t = clean_text(theme_name)
        is_auto = "자동차" in sector_blob or "차량" in sector_blob
        is_semi = "반도체" in sector_blob or "메모리" in sector_blob

        if is_auto and t in {"ai", "로봇", "피지컬ai", "2차전지", "전력인프라", "자동차"}:
            return True
        if is_semi and t in {"반도체", "hbm", "ai", "it", "전력인프라"}:
            return True
        return False

    for signal in trend_scores:
        theme = signal["name"]
        if theme not in base_names and not is_allowed_new_theme(theme):
            continue
        boost = max(0.0, float(signal.get("score", 0.0))) * trend_weight
        # Keep HBM inside semiconductor theme, but let it contribute more strongly.
        if clean_text(theme) in {"반도체", "semiconductor"}:
            ev_text = " ".join(str(x) for x in (signal.get("evidence", []) or []))
            ev_norm = clean_text(ev_text)
            if ("hbm" in ev_norm) or ("고대역폭" in ev_norm):
                boost *= 1.25
        if boost <= 0:
            continue
        if theme in merged:
            merged[theme]["score"] = round(float(merged[theme].get("score", 0.0)) + boost, 3)
            evidence = merged[theme].get("evidence", [])
            if not isinstance(evidence, list):
                evidence = []
            evidence.extend(signal.get("evidence", []) or [])
            merged[theme]["evidence"] = sorted(set(str(e) for e in evidence))
        else:
            merged[theme] = {
                "name": theme,
                "score": round(boost, 3),
                "evidence": sorted(set(str(e) for e in (signal.get("evidence", []) or ["trend"]))),
                "matched_text": signal.get("matched_text", ""),
            }

    out = list(merged.values())
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def save_json(path: Path, data: Any) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower().strip())


def fetch_krx_market_list(market: str) -> pd.DataFrame:
    market_type = "stockMkt" if market == "KOSPI" else "kosdaqMkt"
    url = f"https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&marketType={market_type}"
    res = requests.get(url, timeout=20)
    res.raise_for_status()
    res.encoding = "euc-kr"
    soup = BeautifulSoup(res.text, "html.parser")

    table = soup.select_one("table")
    if table is None:
        raise RuntimeError(f"Failed to parse KRX list for {market}")

    rows = table.select("tr")
    if not rows:
        raise RuntimeError(f"No rows found in KRX list for {market}")

    headers = [th.get_text(strip=True) for th in rows[0].select("th")]
    records: list[dict[str, str]] = []
    for tr in rows[1:]:
        tds = tr.select("td")
        if not tds:
            continue
        values = [td.get_text(strip=True) for td in tds]
        if len(values) == len(headers):
            records.append(dict(zip(headers, values)))

    df = pd.DataFrame(records)
    df["시장"] = market
    return df


def fetch_naver_code_name_map() -> dict[str, str]:
    """
    Build code->short_name map from Naver market sum pages.
    This helps normalize KRX company name to traded name shown in app.
    """
    out: dict[str, str] = {}
    headers = {"User-Agent": "Mozilla/5.0"}
    for sosok in (0, 1):  # 0=KOSPI, 1=KOSDAQ
        for page in range(1, 41):
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, headers=headers, timeout=20)
                res.raise_for_status()
                soup = BeautifulSoup(res.content.decode("euc-kr", "replace"), "html.parser")
                table = soup.select_one("table.type_2")
                if table is None:
                    break

                found = 0
                for a in table.select('a[href*="item/main.naver?code="]'):
                    href = a.get("href", "")
                    name = a.get_text(strip=True)
                    if "code=" not in href or not name:
                        continue
                    code = href.split("code=")[-1].split("&")[0].strip()
                    if code:
                        out[code] = name
                        found += 1
                if found == 0:
                    break
            except Exception:
                break
    return out


def build_universe() -> list[StockRecord]:
    frames = [fetch_krx_market_list("KOSPI"), fetch_krx_market_list("KOSDAQ")]
    df = pd.concat(frames, ignore_index=True)
    naver_name_map = fetch_naver_code_name_map()

    name_col = "회사명"
    code_col = "종목코드"
    sector_col = "업종" if "업종" in df.columns else ""
    product_col = "주요제품" if "주요제품" in df.columns else ""

    universe: list[StockRecord] = []
    for _, row in df.iterrows():
        raw_code = str(row[code_col]).strip().upper()
        code = raw_code.zfill(6) if raw_code.isdigit() else raw_code
        base_name = str(row[name_col]).strip()
        name = naver_name_map.get(code, base_name)
        market = str(row["시장"]).strip()
        sector = str(row[sector_col]).strip() if sector_col else ""
        products = str(row[product_col]).strip() if product_col else ""
        if not name:
            continue
        universe.append(
            StockRecord(
                code=code,
                name=name,
                market=market,
                sector=sector,
                products=products,
                tags=[],
            )
        )
    return universe


def score_stock_themes(stock: StockRecord, rules: dict[str, list[str]]) -> list[dict[str, Any]]:
    name_text = clean_text(stock.name)
    sector_text = clean_text(stock.sector)
    products_text = clean_text(stock.products)
    tag_text = clean_text(" ".join(stock.tags or []))
    blob = " ".join([name_text, sector_text, products_text, tag_text])

    scored: list[dict[str, Any]] = []
    for theme, keywords in rules.items():
        score = 0.0
        evidence: list[str] = []
        for kw in keywords:
            key = clean_text(kw)
            if not key:
                continue
            if key in name_text:
                score += 2.0
                evidence.append(f"name:{kw}")
            if key in sector_text:
                score += 2.3
                evidence.append(f"sector:{kw}")
            if key in products_text:
                score += 1.7
                evidence.append(f"product:{kw}")
            if key in tag_text:
                score += 1.8
                evidence.append(f"tag:{kw}")
        if score > 0:
            scored.append(
                {
                    "name": theme,
                    "score": round(score, 2),
                    "evidence": sorted(set(evidence)),
                    "matched_text": blob[:280],
                }
            )

    # Sector keyword boost (aggressive)
    for theme, keys in SECTOR_THEME_KEYWORDS.items():
        if theme not in rules:
            continue
        for kw in keys:
            token = clean_text(kw)
            if token in sector_text:
                hit = next((s for s in scored if s["name"] == theme), None)
                if hit is None:
                    scored.append(
                        {
                            "name": theme,
                            "score": 2.4,
                            "evidence": [f"sector_hint:{kw}"],
                            "matched_text": blob[:280],
                        }
                    )
                else:
                    hit["score"] = round(hit["score"] + 1.4, 2)
                    hit["evidence"] = sorted(set(hit["evidence"] + [f"sector_hint:{kw}"]))
                break

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


def infer_fallback_themes(stock: StockRecord, rules: dict[str, list[str]]) -> list[dict[str, Any]]:
    sector_text = clean_text(stock.sector)
    product_text = clean_text(stock.products)
    blob = f"{sector_text} {product_text}"

    picks: list[dict[str, Any]] = []
    for theme, keys in SECTOR_THEME_KEYWORDS.items():
        if theme not in rules:
            continue
        if any(clean_text(k) in blob for k in keys):
            picks.append(
                {
                    "name": theme,
                    "score": 1.2,
                    "evidence": ["fallback:sector"],
                    "matched_text": blob[:280],
                }
            )

    if not picks:
        # Last-resort theme to guarantee at least 1 tag.
        theme_name = "기타"
        picks.append(
            {
                "name": theme_name,
                "score": 0.5,
                "evidence": ["fallback:default"],
                "matched_text": blob[:280],
            }
        )
    return picks


def apply_overrides(stock: StockRecord, themes: list[dict[str, Any]], overrides: dict[str, Any]) -> list[dict[str, Any]]:
    by_code = overrides.get("by_code", {})
    by_name = overrides.get("by_name", {})
    patch = by_code.get(stock.code) or by_name.get(stock.name)
    if not patch:
        return themes

    current = {t["name"]: t for t in themes}
    for remove_theme in patch.get("remove", []):
        current.pop(remove_theme, None)

    for add_theme in patch.get("add", []):
        if add_theme not in current:
            current[add_theme] = {
                "name": add_theme,
                "score": 99.0,
                "evidence": ["override:add"],
                "matched_text": "",
            }

    if patch.get("set"):
        forced = {}
        for i, theme in enumerate(patch["set"]):
            forced[theme] = {
                "name": theme,
                "score": 100.0 - i,
                "evidence": ["override:set"],
                "matched_text": "",
            }
        current = forced

    out = list(current.values())
    out.sort(key=lambda x: x["score"], reverse=True)
    return out


def classify_all(
    universe: list[StockRecord],
    rules: dict[str, list[str]],
    overrides: dict[str, Any],
    trend_signals: dict[str, list[dict[str, Any]]],
    trend_weight: float,
    max_themes: int,
    min_score: float,
) -> tuple[dict[str, list[str]], dict[str, list[dict[str, str]]], dict[str, Any]]:
    stock_to_themes: dict[str, list[str]] = {}
    theme_to_members: dict[str, list[dict[str, str]]] = defaultdict(list)
    detailed: dict[str, Any] = {}

    for stock in universe:
        scored = score_stock_themes(stock, rules)
        scored = apply_domain_enrichment(stock, scored, rules)
        # Give higher trend influence to diversified names (multi-theme or holdings).
        diversified = (len(scored) >= 2) or ("홀딩스" in stock.name) or ("지주" in stock.name)
        tw = trend_weight * (1.25 if diversified else 1.0)
        scored = merge_trend_scores(stock, scored, trend_signals.get(stock.code, []), tw)
        scored = apply_overrides(stock, scored, overrides)

        selected = [s for s in scored if s["score"] >= min_score][:max_themes]
        if not selected and scored:
            selected = scored[:1]
        if not selected:
            selected = infer_fallback_themes(stock, rules)[:max_themes]

        # Deduplicate and trim to max_themes.
        unique: dict[str, dict[str, Any]] = {}
        for item in selected:
            if item["name"] not in unique:
                unique[item["name"]] = item
        selected = list(unique.values())[:max_themes]

        selected_names = [s["name"] for s in selected]
        stock_to_themes[stock.name] = selected_names
        detailed[stock.code] = {
            "code": stock.code,
            "name": stock.name,
            "market": stock.market,
            "sector": stock.sector,
            "products": stock.products,
            "trend_applied": bool(trend_signals.get(stock.code)),
            "themes": selected,
        }
        for theme in selected_names:
            theme_to_members[theme].append({"code": stock.code, "name": stock.name, "market": stock.market})

    for theme in list(theme_to_members.keys()):
        theme_to_members[theme] = sorted(theme_to_members[theme], key=lambda x: (x["market"], x["name"]))
    return stock_to_themes, dict(theme_to_members), detailed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local stock theme mapping.")
    parser.add_argument("--rules", default="theme_rules.json", help="Theme rules JSON path")
    parser.add_argument("--overrides", default="theme_overrides.json", help="Manual overrides JSON path")
    parser.add_argument("--trend-signals", default="trend_signals.json", help="Trend signals JSON path")
    parser.add_argument("--trend-weight", type=float, default=1.0, help="Trend signal weight multiplier")
    parser.add_argument("--max-themes", type=int, default=5, help="Max themes per stock")
    parser.add_argument("--min-score", type=float, default=0.9, help="Min score threshold")
    parser.add_argument("--details", default="theme_details.json", help="Detailed output path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rules = load_json(Path(args.rules), DEFAULT_THEME_RULES)
    rules = merge_with_extra_rules(rules)
    overrides = load_json(Path(args.overrides), {"by_code": {}, "by_name": {}})
    trend_signals = load_trend_signals(Path(args.trend_signals))

    print("Loading KOSPI/KOSDAQ universe from KRX...")
    universe = build_universe()
    print(f"Universe size: {len(universe)}")
    print(f"Trend signals: {len(trend_signals)} stocks ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")

    stock_themes, theme_map, detailed = classify_all(
        universe=universe,
        rules=rules,
        overrides=overrides,
        trend_signals=trend_signals,
        trend_weight=args.trend_weight,
        max_themes=args.max_themes,
        min_score=args.min_score,
    )

    save_json(Path("stock_themes.json"), stock_themes)
    save_json(Path("theme_map.json"), theme_map)
    save_json(Path(args.details), detailed)

    covered = sum(1 for v in stock_themes.values() if v)
    print(f"Classified stocks: {covered}/{len(stock_themes)}")
    print(f"Themes with members: {len(theme_map)}")
    print("Saved: stock_themes.json, theme_map.json, theme_details.json")


if __name__ == "__main__":
    main()
