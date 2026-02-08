"""
Build stock themes dictionary from Naver Finance theme pages.
This script scrapes all theme pages and builds a mapping of stock names to themes.
Run this periodically to update the themes data.
"""
import requests
from bs4 import BeautifulSoup
import json
import time
import sys

sys.stdout.reconfigure(encoding='utf-8')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
BASE_URL = "https://finance.naver.com"

def get_all_themes():
    """Get list of all themes from Naver Finance."""
    themes = []
    url = f"{BASE_URL}/sise/theme.naver"
    
    try:
        res = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(res.content.decode('euc-kr', 'replace'), 'html.parser')
        
        # Find theme links
        theme_table = soup.select('table.type_1 tr')
        
        for row in theme_table:
            links = row.select('td a')
            for link in links:
                href = link.get('href', '')
                if 'no=' in href:
                    theme_name = link.text.strip()
                    theme_no = href.split('no=')[-1].split('&')[0]
                    if theme_name and theme_no:
                        themes.append({
                            'name': theme_name,
                            'no': theme_no
                        })
    except Exception as e:
        print(f"Error fetching theme list: {e}")
    
    return themes

def get_theme_stocks(theme_no, theme_name):
    """Get all stocks belonging to a theme."""
    stocks = []
    page = 1
    
    while True:
        url = f"{BASE_URL}/sise/sise_group_detail.naver?type=theme&no={theme_no}&page={page}"
        try:
            res = requests.get(url, headers=HEADERS)
            soup = BeautifulSoup(res.content.decode('euc-kr', 'replace'), 'html.parser')
            
            # Find stock table (type_5)
            table = soup.select_one('table.type_5')
            if not table:
                break
            
            # Find all stock links (href contains /item/main.naver)
            rows = table.select('tr')
            found_stocks = False
            
            for row in rows:
                # Stock name is in the first link to /item/main.naver
                link = row.select_one('a[href*="/item/main.naver"]')
                if link:
                    stock_name = link.text.strip()
                    # Remove asterisk and extra whitespace
                    stock_name = stock_name.replace('*', '').strip()
                    if stock_name and stock_name not in stocks:
                        stocks.append(stock_name)
                        found_stocks = True
            
            if not found_stocks:
                break
                
            # Check for next page
            paging = soup.select('td.pgRR a')
            if paging and page < 10:  # Max 10 pages per theme
                page += 1
                time.sleep(0.1)  # Be nice to server
            else:
                break
                
        except Exception as e:
            print(f"Error fetching theme {theme_name} page {page}: {e}")
            break
    
    return stocks

def build_theme_dictionary():
    """Build the complete stock-to-themes dictionary and theme-to-no mapping."""
    print("🔍 Fetching theme list from Naver Finance...")
    themes = get_all_themes()
    print(f"📊 Found {len(themes)} themes")
    
    stock_themes = {}  # stock_name -> [theme1, theme2, ...]
    theme_map = {}     # theme_name -> theme_no
    
    for i, theme in enumerate(themes):
        print(f"[{i+1}/{len(themes)}] Processing: {theme['name']}")
        theme_map[theme['name']] = theme['no']
        stocks = get_theme_stocks(theme['no'], theme['name'])
        
        for stock in stocks:
            if stock not in stock_themes:
                stock_themes[stock] = []
            if theme['name'] not in stock_themes[stock]:
                stock_themes[stock].append(theme['name'])
        
        time.sleep(0.1)  # Rate limiting
    
    return stock_themes, theme_map

def save_json(data, filename):
    """Save data to JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Saved to {filename}")

if __name__ == "__main__":
    print("=" * 50)
    print("🏷️  Building Stock Themes Dictionary")
    print("=" * 50)
    
    stock_themes, theme_map = build_theme_dictionary()
    save_json(stock_themes, 'stock_themes.json')
    save_json(theme_map, 'theme_map.json')
    
    # Show some stats
    total_stocks = len(stock_themes)
    total_themes = len(theme_map)
    avg_themes = sum(len(t) for t in stock_themes.values()) / total_stocks if total_stocks else 0
    
    print("\n📈 Summary:")
    print(f"   Total stocks with themes: {total_stocks}")
    print(f"   Total themes found: {total_themes}")
    print(f"   Average themes per stock: {avg_themes:.1f}")
    
    # Sample output
    print("\n🔎 Sample entries:")
    sample_stocks = list(stock_themes.items())[:5]
    for name, themes in sample_stocks:
        print(f"   {name}: {', '.join(themes[:3])}")
    
    print("\n✅ Done! Run 'streamlit run app.py' to see themes in action.")
