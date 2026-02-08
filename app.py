import streamlit as st
import pandas as pd
import time
from scraper import get_stock_info, get_market_indices
from data_processor import clean_price, clean_rate

st.set_page_config(page_title="Stock Info Aggregator", layout="wide")

st.title("📈 Stock Info Aggregator")

# Sidebar for settings
with st.sidebar:
    st.header("Settings")
    stock_code = st.text_input("Enter Stock Code (KRX)", value="005930")
    refresh_rate = st.slider("Refresh Rate (seconds)", 5, 60, 10)
    auto_refresh = st.checkbox("Auto Refresh", value=False)

# Main area
col1, col2 = st.columns(2)

with col1:
    st.subheader("Market Indices")
    indices = get_market_indices()
    if indices:
        ic1, ic2 = st.columns(2)
        ic1.metric("KOSPI", indices.get("KOSPI", "N/A"))
        ic2.metric("KOSDAQ", indices.get("KOSDAQ", "N/A"))
    else:
        st.error("Failed to fetch market indices")

with col2:
    st.subheader("Stock Details")
    if stock_code:
        info = get_stock_info(stock_code)
        if info:
            # Clean data for valid metric display
            current_price = info['current_price']
            rate = info['rate']
            volume = info['volume']
            
            # Metric
            st.metric(label=info['name'], value=f"{current_price} KRW", delta=f"{rate}%")
            st.write(f"**Volume:** {volume}")
            st.write(f"**Code:** {info['code']}")
            
        else:
            st.error(f"Failed to fetch info for code: {stock_code}")

# Auto refresh logic
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()

st.markdown("---")
st.caption("Data source: Naver Finance")
