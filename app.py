import streamlit as st
import pandas as pd
import time
from scraper import get_stock_info, get_market_indices, get_top_stocks
from data_processor import clean_price, clean_rate
from themes import get_theme

st.set_page_config(page_title="Blue Key Project", layout="wide")

def style_rate(v):
    """Style function for Rate column (+ Red, - Blue, Bold)."""
    if v > 0:
        return 'color: #d32f2f; font-weight: bold;' # Red
    elif v < 0:
        return 'color: #1976d2; font-weight: bold;' # Blue
    return ''

# Header and Indices Layout
col_header, col_indices = st.columns([2.5, 1.5])

with col_header:
    st.title("📈 Blue Key Project")

with col_indices:
    indices = get_market_indices()
    if indices:
        i_c1, i_c2 = st.columns(2)
        
        # KOSPI
        kospi = indices.get("KOSPI", {})
        kospi_val = kospi.get("value", "N/A") if isinstance(kospi, dict) else kospi
        kospi_rate = kospi.get("rate", "") if isinstance(kospi, dict) else ""
        kospi_dir = kospi.get("direction", "up") if isinstance(kospi, dict) else "up"
        delta_color = "normal" if kospi_dir == "up" else "inverse"
        i_c1.metric("KOSPI", kospi_val, delta=kospi_rate, delta_color=delta_color)
        
        # KOSDAQ
        kosdaq = indices.get("KOSDAQ", {})
        kosdaq_val = kosdaq.get("value", "N/A") if isinstance(kosdaq, dict) else kosdaq
        kosdaq_rate = kosdaq.get("rate", "") if isinstance(kosdaq, dict) else ""
        kosdaq_dir = kosdaq.get("direction", "up") if isinstance(kosdaq, dict) else "up"
        delta_color_kd = "normal" if kosdaq_dir == "up" else "inverse"
        i_c2.metric("KOSDAQ", kosdaq_val, delta=kosdaq_rate, delta_color=delta_color_kd)
    else:
        st.error("Index Error")

# Sidebar for settings
with st.sidebar:
    st.header("Settings")
    # Move stock_code input to inside the tab or keep it global? 
    # Let's keep it in sidebar but only relevant for Search tab.
    refresh_rate = st.slider("Refresh Rate (seconds)", 5, 60, 10, key='refresh_slider')
    auto_refresh = st.checkbox("Auto Refresh", value=False, key='auto_refresh_check')
    
    st.markdown("---")
    st.subheader("Filter (Top List)")
    exclude_etf = st.checkbox("Exclude ETF/ETN", value=True)
    use_rate_filter = st.checkbox("Enable **Rate** Filter", value=True)
    rate_threshold = st.slider(
        "Min Rate (%)", 
        -30.0, 30.0, 4.0, 
        step=0.5, 
        disabled=not use_rate_filter,
        help="Show stocks with Rate >= Threshold"
    )
    display_count = st.slider(
        "Top N Rank", 
        10, 100, 30, 
        step=10,
        help="Show stocks from Top 1 to N by trading value"
    )

# Main area - Tabs
# Swap order: Top Trading Value first
tab1, tab2 = st.tabs(["💰 Top Trading Value", "🔍 Search Stock"])

with tab1:
    t1_col1, t1_col2 = st.columns(2)
    with t1_col1:
        if use_rate_filter:
            st.subheader(f"거래대금 Top {display_count} 중 {rate_threshold}% 상승 종목")
            st.caption(f"Filter: Rate >= {rate_threshold}%")
        else:
            st.subheader(f"거래대금 Top {display_count} 종목 (전체)")
            st.caption("Filter: Rate Off")
            
    with t1_col2:
        if exclude_etf:
            st.caption("ETF/ETN: Excluded")
    
    # Fetch data (Sorting logic handled in scraper)
    # Fetch up to display_count stocks by trading value
    raw_stocks = get_top_stocks(limit=display_count, sort_by='amount')
    
    filtered_stocks = []
    etf_keywords = ['KODEX', 'TIGER', 'KBSTAR', 'KOSEF', 'ACE', 'SOL', 'ARIRANG', 'HANARO', 'ETN', '인버스', '레버리지', '선물']
    
    # Filter from top N by trading value (not stopping early)
    for idx, stock in enumerate(raw_stocks):
        # Store original rank
        stock['original_rank'] = idx + 1
        
        # Filter by rate
        if use_rate_filter:
            if stock['rate'] < rate_threshold:
                continue
            
        # Filter by ETF
        if exclude_etf:
            is_etf = any(k in stock['name'] for k in etf_keywords)
            if is_etf:
                continue
                
        filtered_stocks.append(stock)
            
    if filtered_stocks:
        # Re-rank for display
        df = pd.DataFrame(filtered_stocks)
        df['DisplayRank'] = range(1, len(df) + 1)
        
        # Create Link URL with Name encoded
        # https://finance.naver.com/item/main.naver?code={code}&name={name}
        import urllib.parse
        
        def make_url(row):
            code = row.get('code', '')
            name = urllib.parse.quote(row['name'])
            return f"https://finance.naver.com/item/main.naver?code={code}&name={name}"
        
        df['Link'] = df.apply(make_url, axis=1)
        
        # Select columns
        # Added 'market_cap' -> 'Market Cap'
        # Display Columns: Rank, Name, Price, Rate, Value (M), Market Cap, Volume
        
        # Ensure market_cap exists (fill NA if missing, e.g. from quant)
        if 'market_cap' not in df.columns:
            df['market_cap'] = 0 # Numeric default
        
        # Add theme column
        df['theme'] = df['name'].apply(get_theme)
            
        # Select columns (Numeric + Theme)
        display_df = df[['DisplayRank', 'Link', 'theme', 'price', 'rate', 'amount', 'market_cap']].copy()
        # Rename columns to Korean
        display_df.columns = ['거래대금 순위', '종목명', '테마', '현재가', '등락률', '거래대금 (백만)', '시가총액 (억)']
        
        # Display DataFrame with Styler and LinkColumn/NumberColumn
        st.dataframe(
            display_df.style.map(style_rate, subset=['등락률']).format({
                "현재가": "{:,.0f}",
                "거래대금 (백만)": "{:,.0f}",
                "시가총액 (억)": "{:,.0f}",
                "등락률": "{:+.2f}%" # Optional: add + sign for Rate?
            }),
            column_config={
                "종목명": st.column_config.LinkColumn(
                    "종목명",
                    display_text="name=(.*)", # Extract 'name' param from URL
                    width="medium"
                ),
                "현재가": st.column_config.NumberColumn("현재가"),
                "등락률": st.column_config.NumberColumn("등락률"),
                "거래대금 (백만)": st.column_config.NumberColumn("거래대금 (백만)"),
                "시가총액 (억)": st.column_config.NumberColumn("시가총액 (억)")
            },
            hide_index=True,
            use_container_width=True,
            height=(len(display_df) + 1) * 35 + 3  # Row height ~35px + header
        )
        
        # Theme-based grouping section
        st.markdown("---")
        st.subheader("📌 테마별 상세 종목 리스트")
        st.caption("현재 상위 종목들이 포함된 테마의 모든 구성 종목과 실시간 데이터를 표시합니다.")
        
        # Collect themes from displayed stocks
        from themes import get_theme_list
        from scraper import get_theme_details
        import json
        import os
        
        # Load theme_map.json
        theme_map = {}
        map_path = os.path.join(os.path.dirname(__file__), 'theme_map.json')
        if os.path.exists(map_path):
            with open(map_path, 'r', encoding='utf-8') as f:
                theme_map = json.load(f)
        
        # Identify themes from filtered stocks
        active_themes = set()
        for stock in filtered_stocks:
            active_themes.update(get_theme_list(stock['name']))
            
        if active_themes:
            for theme in sorted(list(active_themes)):
                theme_no = theme_map.get(theme)
                if not theme_no:
                    continue
                    
                with st.expander(f"📂 {theme} 관련 전체 종목", expanded=True):
                    theme_data = get_theme_details(theme_no)
                    if theme_data:
                        tdf = pd.DataFrame(theme_data)
                        # Link generation
                        tdf['Link'] = tdf.apply(lambda r: f"https://finance.naver.com/item/main.naver?code={r['code']}&name={urllib.parse.quote(r['name'])}", axis=1)
                        
                        # Display
                        tdf_display = tdf[['Link', 'price', 'rate', 'amount']].copy()
                        tdf_display.columns = ['종목명', '현재가', '등락률', '거래대금 (백만)']
                        
                        st.dataframe(
                            tdf_display.style.map(style_rate, subset=['등락률']).format({
                                "현재가": "{:,}",
                                "거래대금 (백만)": "{:,}",
                                "등락률": "{:+.2f}%"
                            }),
                            column_config={
                                "종목명": st.column_config.LinkColumn("종목명", display_text="name=(.*)"),
                                "현재가": st.column_config.NumberColumn("현재가"),
                                "등락률": st.column_config.NumberColumn("등락률"),
                                "거래대금 (백만)": st.column_config.NumberColumn("거래대금 (백만)")
                            },
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.info("데이터를 불러올 수 없습니다.")
        else:
            st.info("테마 정보가 없습니다.")
            
    else:
        st.warning("No stocks match the criteria.")

with tab2:
    st.subheader("Individual Stock Search")
    stock_code = st.text_input("Enter Stock Code (KRX)", value="005930")
    if stock_code:
        info = get_stock_info(stock_code)
        if info:
            # Clean data for valid metric display
            current_price = info['current_price']
            rate = info['rate']
            volume = info['volume']
            
            # Metric
            m_col1, m_col2 = st.columns(2)
            m_col1.metric(label=info['name'], value=f"{current_price} KRW", delta=f"{rate}%")
            m_col2.write(f"**Volume:** {volume}")
            st.write(f"**Code:** {info['code']}")
        else:
            st.error(f"Failed to fetch info for code: {stock_code}")

# Auto refresh logic
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()

st.markdown("---")
st.caption("Data source: Naver Finance")
