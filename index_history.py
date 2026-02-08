"""
Fetch historical index data from Naver Finance for sparkline charts.
Supports fetching multiple pages for longer time periods (3 months+).
"""
import requests
from bs4 import BeautifulSoup

def get_index_history(index_code="KOSPI", days=60):
    """
    Fetch recent index values for sparkline chart.
    Returns list of floats representing closing values (oldest first).
    
    Args:
        index_code: "KOSPI" or "KOSDAQ"
        days: Number of trading days to fetch (default 60 for ~3 months)
    """
    code_map = {
        "KOSPI": "0001",
        "KOSDAQ": "1001"
    }
    
    code = code_map.get(index_code, "0001")
    base_url = f"https://finance.naver.com/sise/sise_index_day.naver?code={code}"
    
    values = []
    seen_dates = set()
    page = 1
    max_pages = 10  # Safety limit
    
    try:
        while len(values) < days and page <= max_pages:
            url = f"{base_url}&page={page}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(res.content.decode('euc-kr', 'replace'), 'html.parser')
            
            table = soup.select_one('table.type_1')
            if not table:
                break
            
            rows = table.select('tr')
            found_any = False
            
            for row in rows:
                cols = row.select('td')
                if len(cols) >= 2:
                    date_text = cols[0].text.strip()
                    close_text = cols[1].text.strip().replace(',', '')
                    
                    # Skip empty or duplicate dates
                    if not date_text or date_text in seen_dates:
                        continue
                    if not close_text or not close_text.replace('.', '').isdigit():
                        continue
                        
                    value = float(close_text)
                    if value > 0:
                        seen_dates.add(date_text)
                        values.append(value)
                        found_any = True
                        if len(values) >= days:
                            break
            
            if not found_any:
                break
            
            page += 1
        
        # Reverse so oldest is first (for chart)
        return list(reversed(values))
        
    except Exception as e:
        print(f"Error fetching {index_code} history: {e}")
        return []

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    kospi = get_index_history("KOSPI", 60)
    kosdaq = get_index_history("KOSDAQ", 60)
    print(f"KOSPI: {len(kospi)} days, range: {min(kospi):.2f} - {max(kospi):.2f}")
    print(f"KOSDAQ: {len(kosdaq)} days, range: {min(kosdaq):.2f} - {max(kosdaq):.2f}")
