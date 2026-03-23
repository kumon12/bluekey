import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from scraper import get_market_indices, get_stock_info, get_stock_snapshots, get_top_stocks
from themes import get_theme, get_theme_list, get_theme_members

st.set_page_config(page_title='Blue Key Project', layout='wide')

ETF_KEYWORDS = [
    'KODEX',
    'TIGER',
    'KBSTAR',
    'KOSEF',
    'ACE',
    'SOL',
    'ARIRANG',
    'HANARO',
    'ETN',
    '레버리지',
    '인버스',
    '선물',
]

TOP_COLUMNS = ['거래대금 순위', '종목명', '테마', '현재가', '등락률', '거래대금 (백만)', '시가총액 (억)']
DETAIL_COLUMNS = ['종목명', '현재가', '등락률', '거래대금 (백만)', '시가총액 (억)']
RATE_COL = '등락률'
PRICE_COL = '현재가'
AMOUNT_COL = '거래대금 (백만)'
MCAP_COL = '시가총액 (억)'
NAME_COL = '종목명'


@st.cache_data(ttl=20, show_spinner=False)
def load_market_indices():
    return get_market_indices()


@st.cache_data(ttl=20, show_spinner=False)
def load_top_stocks_cached(limit, sort_by):
    return get_top_stocks(limit=limit, sort_by=sort_by)


@st.cache_data(ttl=20, show_spinner=False)
def load_stock_snapshots_cached(codes):
    return get_stock_snapshots(list(codes))


def style_rate(value):
    try:
        numeric = float(str(value).replace('%', '').replace(',', '').strip())
    except Exception:
        return ''
    if numeric > 0:
        return 'color: #d32f2f; font-weight: bold;'
    if numeric < 0:
        return 'color: #1976d2; font-weight: bold;'
    return ''


def build_stock_link(code, name):
    return f'https://finance.naver.com/item/main.naver?code={code}&name={urllib.parse.quote(name)}'


def normalize_int(value, default=0):
    try:
        return int(str(value).replace(',', ''))
    except Exception:
        return default


def prepare_quote_lookup(raw_stocks):
    quote_lookup = {}
    preload_sources = list(raw_stocks) + list(load_top_stocks_cached(100, 'volume'))
    for stock in preload_sources:
        code = stock.get('code')
        if not code or code in quote_lookup:
            continue
        quote_lookup[code] = {
            'price': normalize_int(stock.get('price', 0)),
            'rate': stock.get('rate', 0.0),
            'amount': normalize_int(stock.get('amount', 0)),
            'market_cap': normalize_int(stock.get('market_cap', 0)),
        }
    return quote_lookup


col_header, col_indices = st.columns([2.5, 1.5])

with col_header:
    st.title('Blue Key Project')

with col_indices:
    indices = load_market_indices()
    if indices:
        index_col1, index_col2 = st.columns(2)

        kospi = indices.get('KOSPI', {})
        index_col1.metric(
            'KOSPI',
            kospi.get('value', 'N/A'),
            delta=kospi.get('rate', ''),
            delta_color='normal' if kospi.get('direction', 'up') == 'up' else 'inverse',
        )

        kosdaq = indices.get('KOSDAQ', {})
        index_col2.metric(
            'KOSDAQ',
            kosdaq.get('value', 'N/A'),
            delta=kosdaq.get('rate', ''),
            delta_color='normal' if kosdaq.get('direction', 'up') == 'up' else 'inverse',
        )
    else:
        st.error('지수 정보를 불러오지 못했습니다.')

with st.sidebar:
    st.header('Settings')
    refresh_rate = st.slider('Refresh Rate (seconds)', 5, 60, 10, key='refresh_slider')
    auto_refresh = st.checkbox('Auto Refresh', value=False, key='auto_refresh_check')

    st.markdown('---')
    st.subheader('Filter (Top List)')
    exclude_etf = st.checkbox('Exclude ETF/ETN', value=True)
    use_rate_filter = st.checkbox('Enable Rate Filter', value=True)
    rate_threshold = st.slider(
        'Min Rate (%)',
        -30.0,
        30.0,
        4.0,
        step=0.5,
        disabled=not use_rate_filter,
        help='Show stocks with rate >= threshold',
    )
    display_count = st.slider(
        'Top N Rank',
        10,
        100,
        30,
        step=10,
        help='Show stocks from Top 1 to N by trading value',
    )


tab1, tab2 = st.tabs(['Top Trading Value', 'Search Stock'])

with tab1:
    info_col1, info_col2 = st.columns(2)
    with info_col1:
        kst = timezone(timedelta(hours=9))
        current_time = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
        st.caption(f'Data fetched at: {current_time} (KST)')
        if use_rate_filter:
            st.subheader(f'거래대금 상위 Top {display_count} / 등락률 {rate_threshold}% 이상')
            st.caption(f'Filter: Rate >= {rate_threshold}%')
        else:
            st.subheader(f'거래대금 상위 Top {display_count}')
            st.caption('Filter: Rate Off')

    with info_col2:
        if exclude_etf:
            st.caption('ETF/ETN: Excluded')

    raw_stocks = load_top_stocks_cached(display_count, 'amount')

    filtered_stocks = []
    for idx, stock in enumerate(raw_stocks):
        stock = dict(stock)
        stock['original_rank'] = idx + 1

        stock_rate = pd.to_numeric(pd.Series([stock.get('rate', 0.0)]), errors='coerce').fillna(0.0).iloc[0]
        if use_rate_filter and stock_rate < rate_threshold:
            continue

        if exclude_etf and any(keyword in stock.get('name', '') for keyword in ETF_KEYWORDS):
            continue

        stock['rate'] = float(stock_rate)
        filtered_stocks.append(stock)

    if filtered_stocks:
        df = pd.DataFrame(filtered_stocks)
        df['DisplayRank'] = range(1, len(df) + 1)
        df['Link'] = df.apply(lambda row: build_stock_link(row.get('code', ''), row['name']), axis=1)

        if 'market_cap' not in df.columns:
            df['market_cap'] = 0

        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(int)
        df['rate'] = pd.to_numeric(df['rate'], errors='coerce').fillna(0.0)
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0).astype(int)
        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce').fillna(0).astype(int)
        df['theme'] = df['name'].apply(get_theme)

        display_df = df[['DisplayRank', 'Link', 'theme', 'price', 'rate', 'amount', 'market_cap']].copy()
        display_df.columns = TOP_COLUMNS

        st.dataframe(
            display_df.style.map(style_rate, subset=[RATE_COL]).format(
                {
                    PRICE_COL: '{:,.0f}',
                    RATE_COL: '{:+.2f}%',
                    AMOUNT_COL: '{:,.0f}',
                    MCAP_COL: '{:,.0f}',
                }
            ),
            column_config={
                NAME_COL: st.column_config.LinkColumn(NAME_COL, display_text='name=(.*)', width='medium'),
                PRICE_COL: st.column_config.NumberColumn(PRICE_COL),
                RATE_COL: st.column_config.NumberColumn(RATE_COL),
                AMOUNT_COL: st.column_config.NumberColumn(AMOUNT_COL),
                MCAP_COL: st.column_config.NumberColumn(MCAP_COL),
            },
            hide_index=True,
            use_container_width=True,
            height=(len(display_df) + 1) * 35 + 3,
        )

        st.markdown('---')
        st.subheader('테마별 상세 종목 리스트')
        st.caption('현재 상위 종목들이 포함된 테마의 전체 구성 종목을 표시합니다.')

        active_themes = set()
        for stock in filtered_stocks:
            active_themes.update(get_theme_list(stock['name']))

        quote_lookup = prepare_quote_lookup(raw_stocks)
        theme_members_map = {}
        member_codes = set()

        for theme in sorted(active_themes):
            members = get_theme_members(theme)
            if not members:
                continue
            theme_members_map[theme] = members
            for member in members:
                code = member.get('code', '')
                if code:
                    member_codes.add(code)

        missing_codes = tuple(sorted(code for code in member_codes if code not in quote_lookup))
        if missing_codes:
            quote_lookup.update(load_stock_snapshots_cached(missing_codes))

        if theme_members_map:
            for theme in sorted(theme_members_map):
                members = theme_members_map[theme]
                with st.expander(f'테마 {theme} 관련 전체 종목', expanded=True):
                    tdf = pd.DataFrame(members)
                    if 'code' not in tdf.columns:
                        tdf['code'] = ''
                    if 'name' not in tdf.columns:
                        tdf['name'] = ''

                    tdf['price'] = tdf['code'].apply(lambda c: quote_lookup.get(c, {}).get('price', 0))
                    tdf['rate'] = tdf['code'].apply(lambda c: quote_lookup.get(c, {}).get('rate', 0.0))
                    tdf['amount'] = tdf['code'].apply(lambda c: quote_lookup.get(c, {}).get('amount', 0))
                    tdf['market_cap'] = tdf['code'].apply(lambda c: quote_lookup.get(c, {}).get('market_cap', 0))

                    tdf['price'] = pd.to_numeric(tdf['price'], errors='coerce').fillna(0).astype(int)
                    tdf['rate'] = pd.to_numeric(tdf['rate'], errors='coerce').fillna(0.0)
                    tdf['amount'] = pd.to_numeric(tdf['amount'], errors='coerce').fillna(0).astype(int)
                    tdf['market_cap'] = pd.to_numeric(tdf['market_cap'], errors='coerce').fillna(0).astype(int)

                    tdf = tdf[~((tdf['rate'] < 20.0) & (tdf['market_cap'] < 500))]
                    tdf = tdf.sort_values(by=['rate', 'amount'], ascending=[False, False], kind='stable')

                    if tdf.empty:
                        st.info('조건에 맞는 종목이 없습니다.')
                        continue

                    tdf['Link'] = tdf.apply(lambda row: build_stock_link(row.get('code', ''), row['name']), axis=1)
                    tdf_display = tdf[['Link', 'price', 'rate', 'amount', 'market_cap']].copy()
                    tdf_display.columns = DETAIL_COLUMNS

                    st.dataframe(
                        tdf_display.style.map(style_rate, subset=[RATE_COL]).format(
                            {
                                PRICE_COL: '{:,}',
                                RATE_COL: '{:+.2f}%',
                                AMOUNT_COL: '{:,}',
                                MCAP_COL: '{:,}',
                            }
                        ),
                        column_config={
                            NAME_COL: st.column_config.LinkColumn(NAME_COL, display_text='name=(.*)'),
                            PRICE_COL: st.column_config.NumberColumn(PRICE_COL),
                            RATE_COL: st.column_config.NumberColumn(RATE_COL),
                            AMOUNT_COL: st.column_config.NumberColumn(AMOUNT_COL),
                            MCAP_COL: st.column_config.NumberColumn(MCAP_COL),
                        },
                        hide_index=True,
                        use_container_width=True,
                    )
        else:
            st.info('테마 정보가 없습니다.')
    else:
        st.warning('No stocks match the criteria.')

with tab2:
    st.subheader('Individual Stock Search')
    stock_code = st.text_input('Enter Stock Code (KRX)', value='005930')
    if stock_code:
        info = get_stock_info(stock_code)
        if info:
            metric_col1, metric_col2 = st.columns(2)
            metric_col1.metric(label=info['name'], value=f"{info['current_price']} KRW", delta=f"{info['rate']}%")
            metric_col2.write(f"**Volume:** {info['volume']}")
            st.write(f"**Code:** {info['code']}")
        else:
            st.error(f'Failed to fetch info for code: {stock_code}')

if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()

st.markdown('---')
st.caption('Data source: Naver Finance')
