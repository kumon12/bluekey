import json
import os
import threading
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

TOKEN_URL = '/oauth2/token'
DEFAULT_BASE_URL = 'https://api.kiwoom.com'
DEFAULT_HEADERS = {
    'Content-Type': 'application/json;charset=UTF-8',
    'User-Agent': 'Mozilla/5.0',
}
DEFAULT_TOP_STOCKS_URL = '/api/dostk/rkinfo'
DEFAULT_TOP_STOCKS_API_ID = 'ka10032'
DEFAULT_TOP_STOCKS_BODY = {
    'mrkt_tp': '000',
    'mang_stk_incls': '0',
    'stex_tp': '1',
}

_CODE_KEYS = ['code', 'stk_cd', 'stock_code', 'shrn_iscd', 'isu_cd', 'item_code', 'jongmok_code']
_NAME_KEYS = ['name', 'stk_nm', 'stock_name', 'prdt_name', 'isu_nm', 'item_name', 'jongmok_name']
_PRICE_KEYS = ['price', 'cur_prc', 'cur_price', 'current_price', 'stck_prpr', 'now_prc']
_RATE_KEYS = ['rate', 'flu_rt', 'prdy_ctrt', 'updn_rate', 'chg_rt']
_AMOUNT_KEYS = ['amount', 'trde_amt', 'acc_trde_amt', 'deal_amount', 'trade_amount', 'acml_tr_pbmn', 'trde_prica']
_VOLUME_KEYS = ['volume', 'trde_qty', 'acc_trde_qty', 'deal_qty', 'trade_volume', 'acml_vol', 'now_trde_qty']
_MARKET_CAP_KEYS = ['market_cap', 'mkt_cap', 'mrkt_tot_amt', 'tot_mrkt_cap']

_token_cache: dict[str, Any] = {'token': None, 'expires_at': 0.0}
_token_lock = threading.Lock()
_env_loaded = False


class KiwoomConfigurationError(RuntimeError):
    pass


class KiwoomRequestError(RuntimeError):
    pass


def load_kiwoom_env() -> None:
    global _env_loaded
    if _env_loaded:
        return

    base_dir = Path(__file__).resolve().parent
    candidates = [
        base_dir / 'kiwoom.env',
        base_dir / '.env',
        base_dir.parent / 'kiwoom.env',
        base_dir.parent / '.env',
    ]
    for env_path in candidates:
        if env_path.exists():
            load_dotenv(env_path, override=False)

    _env_loaded = True


def _get_env(name: str, default: str = '') -> str:
    load_kiwoom_env()
    return os.getenv(name, default).strip()


def _parse_json_env(name: str, default: Any) -> Any:
    raw = _get_env(name)
    if not raw:
        return default
    return json.loads(raw)


def _get_endpoint_config(url_env: str, api_id_env: str, body_env: str) -> tuple[str, str, Any]:
    if url_env == 'KIWOOM_TOP_STOCKS_URL':
        endpoint = _get_env(url_env, DEFAULT_TOP_STOCKS_URL)
        api_id = _get_env(api_id_env, DEFAULT_TOP_STOCKS_API_ID)
        body = _parse_json_env(body_env, DEFAULT_TOP_STOCKS_BODY)
        return endpoint, api_id, body

    endpoint = _get_env(url_env)
    api_id = _get_env(api_id_env)
    body = _parse_json_env(body_env, {})
    return endpoint, api_id, body


def _coerce_int(value: Any) -> int:
    text = str(value or '').strip().replace(',', '')
    if not text:
        return 0
    sign = -1 if text.startswith('-') else 1
    text = text.lstrip('+-')
    if text.isdigit():
        return sign * int(text)
    try:
        return int(float(text) * sign)
    except ValueError:
        return 0


def _coerce_float(value: Any) -> float:
    text = str(value or '').strip().replace('%', '').replace(',', '')
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _pick_value(payload: dict[str, Any], candidates: list[str]) -> Any:
    for key in candidates:
        if key in payload and payload[key] not in (None, ''):
            return payload[key]
    return None


def _first_table(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list) and value and all(isinstance(row, dict) for row in value):
                return value
        for value in payload.values():
            if isinstance(value, dict):
                nested = _first_table(value)
                if nested:
                    return nested
    return []


def _first_row(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        table = _first_table(payload)
        if table:
            return table[0]
        return payload
    if isinstance(payload, list):
        for row in payload:
            if isinstance(row, dict):
                return row
    return {}


def _render_template(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, str):
        return value.format(**context)
    if isinstance(value, list):
        return [_render_template(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _render_template(item, context) for key, item in value.items()}
    return value


def get_status() -> tuple[bool, str]:
    missing = []
    if not _get_env('KIWOOM_APPKEY'):
        missing.append('KIWOOM_APPKEY')
    if not _get_env('KIWOOM_SECRETKEY'):
        missing.append('KIWOOM_SECRETKEY')
    if missing:
        return False, 'Missing env: ' + ', '.join(missing)
    snapshot_ready = bool(_get_env('KIWOOM_SNAPSHOT_URL') and _get_env('KIWOOM_SNAPSHOT_API_ID'))
    if snapshot_ready:
        return True, 'Kiwoom top list + snapshot configured'
    return True, 'Kiwoom top list configured. Snapshot falls back to Naver.'


def _require_config() -> None:
    ok, message = get_status()
    if not ok:
        raise KiwoomConfigurationError(message)


def _require_snapshot_config() -> None:
    if not _get_env('KIWOOM_SNAPSHOT_URL') or not _get_env('KIWOOM_SNAPSHOT_API_ID'):
        raise KiwoomConfigurationError('Kiwoom snapshot config missing. Falling back to Naver snapshots.')


def _base_url() -> str:
    return _get_env('KIWOOM_BASE_URL', DEFAULT_BASE_URL).rstrip('/')


def _request_token() -> str:
    _require_config()
    with _token_lock:
        now = time.time()
        cached_token = _token_cache.get('token')
        expires_at = float(_token_cache.get('expires_at') or 0.0)
        if cached_token and expires_at - now > 60:
            return cached_token

        body = {
            'grant_type': 'client_credentials',
            'appkey': _get_env('KIWOOM_APPKEY'),
            'secretkey': _get_env('KIWOOM_SECRETKEY'),
        }
        response = requests.post(_base_url() + TOKEN_URL, headers=DEFAULT_HEADERS, json=body, timeout=15)
        response.raise_for_status()
        payload = response.json()
        token = payload.get('token')
        if not token:
            raise KiwoomRequestError(f'Kiwoom token response missing token: {payload}')

        expires_dt = str(payload.get('expires_dt', ''))
        expires_at = now + 60 * 50
        if len(expires_dt) == 14 and expires_dt.isdigit():
            try:
                expires_at = time.mktime(time.strptime(expires_dt, '%Y%m%d%H%M%S'))
            except ValueError:
                pass

        _token_cache['token'] = token
        _token_cache['expires_at'] = expires_at
        return token


def _request_api(url_env: str, api_id_env: str, body_env: str, context: dict[str, str] | None = None, method_env: str | None = None) -> Any:
    _require_config()
    context = context or {}
    token = _request_token()
    endpoint, api_id, body_template = _get_endpoint_config(url_env, api_id_env, body_env)
    if not endpoint or not api_id:
        raise KiwoomConfigurationError(f'Missing Kiwoom endpoint config: {url_env}, {api_id_env}')

    body = _render_template(body_template, context)
    method_name = _get_env(method_env or '', 'POST') if method_env else 'POST'
    method_name = (method_name or 'POST').upper()
    headers = {
        **DEFAULT_HEADERS,
        'authorization': f'Bearer {token}',
        'api-id': api_id,
        'cont-yn': 'N',
        'next-key': '',
    }
    url = endpoint if endpoint.startswith('http') else _base_url() + endpoint
    request_fn = requests.get if method_name == 'GET' else requests.post
    kwargs: dict[str, Any] = {'headers': headers, 'timeout': 15}
    if method_name == 'GET':
        kwargs['params'] = body
    else:
        kwargs['json'] = body
    response = request_fn(url, **kwargs)
    response.raise_for_status()
    return response.json()


def _normalize_stock_row(row: dict[str, Any]) -> dict[str, Any]:
    code = str(_pick_value(row, _CODE_KEYS) or '').strip()
    name = str(_pick_value(row, _NAME_KEYS) or '').strip()
    price = abs(_coerce_int(_pick_value(row, _PRICE_KEYS)))
    rate = _coerce_float(_pick_value(row, _RATE_KEYS))
    amount = abs(_coerce_int(_pick_value(row, _AMOUNT_KEYS)))
    volume = abs(_coerce_int(_pick_value(row, _VOLUME_KEYS)))
    market_cap = abs(_coerce_int(_pick_value(row, _MARKET_CAP_KEYS)))
    return {
        'code': code,
        'name': name,
        'price': price,
        'price_str': f'{price:,}' if price else '0',
        'current_price': f'{price:,}' if price else '0',
        'rate': rate,
        'rate_str': f'{rate:+.2f}%',
        'amount': amount,
        'amount_str': f'{amount:,}',
        'volume': volume,
        'market_cap': market_cap,
        'market_cap_str': f'{market_cap:,}' if market_cap else '0',
    }


def _enrich_market_caps(stocks: list[dict[str, Any]]) -> None:
    missing_codes = [stock['code'] for stock in stocks if stock.get('code') and not stock.get('market_cap')]
    if not missing_codes:
        return

    try:
        from scraper import get_stock_snapshots as get_naver_snapshots

        snapshots = get_naver_snapshots(missing_codes)
        for stock in stocks:
            snapshot = snapshots.get(stock.get('code', ''))
            if not snapshot:
                continue
            market_cap = abs(_coerce_int(snapshot.get('market_cap')))
            if market_cap:
                stock['market_cap'] = market_cap
                stock['market_cap_str'] = f'{market_cap:,}'
    except Exception:
        pass


def get_top_stocks(limit: int = 30, sort_by: str = 'amount') -> list[dict[str, Any]]:
    if sort_by != 'amount':
        raise KiwoomConfigurationError('Kiwoom source currently supports sort_by="amount" only.')
    payload = _request_api(
        'KIWOOM_TOP_STOCKS_URL',
        'KIWOOM_TOP_STOCKS_API_ID',
        'KIWOOM_TOP_STOCKS_BODY',
        method_env='KIWOOM_TOP_STOCKS_METHOD',
    )
    rows = _first_table(payload)
    stocks = []
    for row in rows:
        stock = _normalize_stock_row(row)
        if stock['code'] and stock['name']:
            stocks.append(stock)
    _enrich_market_caps(stocks)
    stocks.sort(key=lambda item: item['amount'], reverse=True)
    for index, stock in enumerate(stocks[:limit], start=1):
        stock['rank'] = index
    return stocks[:limit]


def get_stock_snapshots(codes: list[str]) -> dict[str, dict[str, Any]]:
    _require_snapshot_config()
    snapshots: dict[str, dict[str, Any]] = {}
    for code in dict.fromkeys(code for code in codes if code):
        payload = _request_api(
            'KIWOOM_SNAPSHOT_URL',
            'KIWOOM_SNAPSHOT_API_ID',
            'KIWOOM_SNAPSHOT_BODY',
            context={'code': code},
            method_env='KIWOOM_SNAPSHOT_METHOD',
        )
        row = _first_row(payload)
        stock = _normalize_stock_row(row)
        if not stock['code']:
            stock['code'] = code
        if stock['code']:
            snapshots[stock['code']] = stock
    return snapshots
