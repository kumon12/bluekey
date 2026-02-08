# Blue Key Project 📈

Stock info aggregator with a focus on trading value, market trends, and theme-based grouping.

## Features
- **Top Trading Value:** Monitor high-volume stocks with customizable gain filters.
- **Theme Grouping:** Instantly see all stocks related to specific market themes.
- **Market Indices:** Real-time KOSPI/KOSDAQ tracking with 5-day trend sparklines.
- **Real-time Data:** Fetches latest price, rate behavior, and trading amount from Naver Finance.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Initialize Theme Data (First time only):**
    ```bash
    python build_themes.py
    ```

3.  **Run the App:**
    ```bash
    streamlit run app.py
    ```

## Project Structure
- `app.py`: Main Streamlit interface.
- `scraper.py`: Web scraping logic for real-time data.
- `build_themes.py`: Utility to crawl and map stock themes.
- `themes.py`: Theme lookup utilities.
- `index_history.py`: Index history and chart generation.
- `data_processor.py`: Data cleaning and formatting.
