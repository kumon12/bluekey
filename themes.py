import json
from pathlib import Path
from typing import Any


BUILTIN_BY_NAME = {
    "삼성전자": ["반도체", "AI", "IT"],
    "SK하이닉스": ["반도체", "AI"],
    "LG에너지솔루션": ["2차전지", "자동차"],
    "현대차": ["자동차", "IT"],
    "NAVER": ["AI", "IT"],
    "카카오": ["IT", "콘텐츠"],
}


_cache: dict[str, Any] | None = None


def _safe_load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _theme_names(value: Any) -> list[str]:
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict) and isinstance(item.get("name"), str):
                out.append(item["name"])
        return out
    return []


def _build_maps_from_stock_themes(data: Any) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    by_name: dict[str, list[str]] = {}
    by_code: dict[str, list[str]] = {}

    if not isinstance(data, dict):
        return by_name, by_code

    for key, value in data.items():
        # Old format: { "종목명": ["테마1", ...] }
        if isinstance(value, list):
            by_name[str(key)] = _theme_names(value)
            continue

        # New detailed format: { "005930": { "name":"삼성전자", "themes":[...] } }
        if isinstance(value, dict):
            code = str(key)
            name = str(value.get("name", "")).strip()
            themes = _theme_names(value.get("themes", []))
            if name:
                by_name[name] = themes
            by_code[code] = themes

    return by_name, by_code


def _build_members_from_theme_map(data: Any) -> dict[str, list[dict[str, str]]]:
    members: dict[str, list[dict[str, str]]] = {}
    if not isinstance(data, dict):
        return members

    for theme, value in data.items():
        theme_name = str(theme)
        rows: list[dict[str, str]] = []

        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    code = str(item.get("code", ""))
                    name = str(item.get("name", ""))
                    market = str(item.get("market", ""))
                    if name or code:
                        rows.append({"code": code, "name": name, "market": market})
                elif isinstance(item, str):
                    rows.append({"code": "", "name": item, "market": ""})

        if rows:
            members[theme_name] = rows

    return members


def _load() -> dict[str, Any]:
    global _cache
    if _cache is not None:
        return _cache

    base = Path(__file__).resolve().parent
    stock_themes_raw = _safe_load_json(base / "stock_themes.json")
    theme_map_raw = _safe_load_json(base / "theme_map.json")

    by_name, by_code = _build_maps_from_stock_themes(stock_themes_raw)
    if not by_name:
        by_name = dict(BUILTIN_BY_NAME)

    members = _build_members_from_theme_map(theme_map_raw)

    # Backfill members from by_name if theme_map is missing/old format.
    if not members:
        members = {}
        for stock_name, themes in by_name.items():
            for theme in themes:
                members.setdefault(theme, []).append(
                    {"code": "", "name": stock_name, "market": ""}
                )

    _cache = {
        "by_name": by_name,
        "by_code": by_code,
        "members": members,
    }
    return _cache


def get_theme(stock_name: str, max_themes: int = 3) -> str:
    themes = get_theme_list(stock_name)[:max_themes]
    return ", ".join(themes) if themes else "-"


def get_theme_list(stock_name: str) -> list[str]:
    data = _load()
    return data["by_name"].get(stock_name, [])


def get_theme_list_by_code(stock_code: str) -> list[str]:
    data = _load()
    return data["by_code"].get(stock_code, [])


def get_theme_members(theme_name: str) -> list[dict[str, str]]:
    data = _load()
    return data["members"].get(theme_name, [])


def get_all_themes() -> dict[str, list[str]]:
    data = _load()
    return data["by_name"]
