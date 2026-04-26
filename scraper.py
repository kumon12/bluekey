import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

DEFAULT_HEADERS = {'User-Agent': 'Mozilla/5.0'}


def _parse_int(text):
    text = (text or "").strip().replace(",", "")
    return int(text) if text.isdigit() else 0


def _parse_rate(text):
    raw = (text or "").strip().replace("%", "").replace(",", "")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _extract_market_cap(text):
    match = re.search(r"시가총액\(억\)\s*([\d,]+)", text or "")
    if not match:
        return 0
    return _parse_int(match.group(1))


def _parse_amount_millions(text):
    text = (text or "").strip().replace(",", "")
    if not text or text == "-":
        return 0
    if text.isdigit():
        return int(text)
    try:
        return int(float(text))
    except ValueError:
        return 0


def _extract_stock_snapshot(soup, code):
    name_tag = soup.select_one('.wrap_company h2 a')
    price_tag = soup.select_one('.no_today .blind')
    if not name_tag or not price_tag:
        return None

    name = name_tag.text.strip()
    price = _parse_int(price_tag.text)

    exday_blinds = soup.select('.no_exday .blind')
    rate = _parse_rate(exday_blinds[1].text if len(exday_blinds) >= 2 else "0")

    volume = 0
    amount = 0
    try:
        rows = soup.select('.no_info tr')
        if len(rows) >= 2:
            first_row = rows[0].select('td')
            second_row = rows[1].select('td')
            volume_tag = first_row[2].select_one('.blind') if len(first_row) >= 3 else None
            amount_tag = second_row[2].select_one('.blind') if len(second_row) >= 3 else None
            volume = _parse_int(volume_tag.text if volume_tag else "0")
            amount = _parse_int(amount_tag.text if amount_tag else "0")
    except IndexError:
        pass

    if amount == 0 and price and volume:
        amount = (price * volume) // 1000000

    market_cap = _extract_market_cap(soup.get_text(" ", strip=True))

    return {
        "name": name,
        "code": code,
        "price": price,
        "current_price": f"{price:,}" if price else "0",
        "rate": rate,
        "volume": volume,
        "amount": amount,
        "market_cap": market_cap,
    }

def get_stock_info(code):
    """
    Fetches stock information from Naver Finance given a stock code.
    Args:
        code (str): The stock code (e.g., '005930' for Samsung Electronics).
    Returns:
        dict: A dictionary containing stock details, or None if failed.
    """
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        soup = BeautifulSoup(response.content.decode('utf-8', 'replace'), 'html.parser')
        snapshot = _extract_stock_snapshot(soup, code)
        if not snapshot:
            return None
        return {
            "name": snapshot["name"],
            "code": code,
            "current_price": snapshot["current_price"],
            "rate": snapshot["rate"],
            "volume": f"{snapshot['volume']:,}" if snapshot["volume"] else "0",
            "amount": snapshot["amount"],
        }
    except Exception as e:
        print(f"Error fetching data for {code}: {e}")
        return None


def get_stock_snapshots(codes):
    """
    Fetch detailed quote data for arbitrary stock codes.
    Returns: {code: {"name","code","price","rate","amount","volume"}}
    """
    snapshots = {}
    unique_codes = [code for code in dict.fromkeys(codes) if code]

    def fetch_one(code):
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content.decode('utf-8', 'replace'), 'html.parser')
        return _extract_stock_snapshot(soup, code)

    max_workers = min(12, max(1, len(unique_codes)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetch_one, code): code for code in unique_codes}
        for future in as_completed(future_map):
            code = future_map[future]
            try:
                snapshot = future.result()
                if snapshot:
                    snapshots[code] = snapshot
            except Exception as e:
                print(f"Error fetching snapshot for {code}: {e}")

    return snapshots

def get_market_indices():
    """
    Fetches KOSPI/KOSDAQ indices with rate of change.
    Returns: dict with 'KOSPI' and 'KOSDAQ' sub-dicts containing 'value', 'change', 'rate'
    """
    url = "https://finance.naver.com/sise/"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(response.content.decode('utf-8', 'replace'), 'html.parser')

        # KOSPI
        kospi_value = soup.select_one('#KOSPI_now').text.strip()
        kospi_change_el = soup.select_one('#KOSPI_change')
        kospi_change = kospi_change_el.text.strip() if kospi_change_el else "0"
        # Extract rate from the change area
        kospi_rate_el = soup.select_one('#KOSPI_quant')
        kospi_rate = kospi_rate_el.text.strip() if kospi_rate_el else "0%"
        
        # Check if up or down
        kospi_up = soup.select_one('.kospi_area .num') 
        kospi_direction = "up" if kospi_up and 'up' in str(kospi_up.get('class', [])) else "down"
        
        # KOSDAQ
        kosdaq_value = soup.select_one('#KOSDAQ_now').text.strip()
        kosdaq_change_el = soup.select_one('#KOSDAQ_change')
        kosdaq_change = kosdaq_change_el.text.strip() if kosdaq_change_el else "0"
        kosdaq_rate_el = soup.select_one('#KOSDAQ_quant')
        kosdaq_rate = kosdaq_rate_el.text.strip() if kosdaq_rate_el else "0%"
        
        kosdaq_up = soup.select_one('.kosdaq_area .num')
        kosdaq_direction = "up" if kosdaq_up and 'up' in str(kosdaq_up.get('class', [])) else "down"
        
        return {
            "KOSPI": {
                "value": kospi_value,
                "change": kospi_change,
                "rate": kospi_rate,
                "direction": kospi_direction
            },
            "KOSDAQ": {
                "value": kosdaq_value,
                "change": kosdaq_change,
                "rate": kosdaq_rate,
                "direction": kosdaq_direction
            }
        }
    except Exception as e:
        print(f"Error fetching market indices: {e}")
        return None

def get_top_stocks(limit=30, sort_by="volume"):
    """
    Fetches top stocks from Naver Finance.
    Args:
        limit (int): Number of stocks to return.
        sort_by (str): 'volume' (Top 100 Volume) or 'amount' (Top Trading Value via Market Sum sorted).
    Returns:
        list: List of dictionaries.
    """
    if sort_by == 'amount':
        # Trading-value mode needs a wider candidate set than just one page.
        # Merge a few pages of market-cap leaders and high-volume leaders, then sort by amount.
        url_specs = [
            ("https://finance.naver.com/sise/sise_market_sum.naver?sosok=0", 3),
            ("https://finance.naver.com/sise/sise_market_sum.naver?sosok=1", 3),
            ("https://finance.naver.com/sise/sise_quant.naver", 6),
        ]

        seen_codes = set()
        all_stocks = []

        for base_url, pages in url_specs:
            try:
                for page in range(1, pages + 1):
                    page_url = f"{base_url}&page={page}" if "?" in base_url else f"{base_url}?page={page}"
                    response = requests.get(page_url, headers=DEFAULT_HEADERS, timeout=10)
                    soup = BeautifulSoup(response.content.decode('euc-kr', 'replace'), 'html.parser')
                    table = soup.select_one('table.type_2')
                    if not table:
                        continue

                    rows = table.select('tr')
                    for row in rows:
                        cols = row.select('td')
                        if not (len(cols) > 5 and cols[0].text.strip().isdigit()):
                            continue

                        try:
                            name_tag = cols[1].select_one('a')
                            if not name_tag:
                                continue

                            name = name_tag.text.strip()
                            href = name_tag['href']
                            stock_code = href.split('=')[-1]
                            if stock_code in seen_codes:
                                continue

                            is_quant = 'quant' in base_url
                            price_str = cols[2].text.strip()
                            rate_str = cols[4].text.strip()
                            price = _parse_int(price_str)
                            rate_val = _parse_rate(rate_str)

                            market_cap_str = "-"
                            if is_quant:
                                volume = _parse_int(cols[5].text.strip())
                                amount = _parse_amount_millions(cols[6].text.strip())
                                if len(cols) > 9:
                                    market_cap_str = cols[9].text.strip()
                            else:
                                volume = _parse_int(cols[9].text.strip())
                                amount = (price * volume) // 1000000
                                market_cap_str = cols[6].text.strip()

                            market_cap_val = _parse_int(market_cap_str) if market_cap_str != "-" else 0
                            item = {
                                "code": stock_code,
                                "name": name,
                                "price": price,
                                "price_str": price_str,
                                "rate_str": rate_str,
                                "rate": rate_val,
                                "volume": volume,
                                "amount": amount,
                                "amount_str": f"{amount:,}",
                                "market_cap": market_cap_val,
                                "market_cap_str": market_cap_str,
                            }
                            all_stocks.append(item)
                            seen_codes.add(stock_code)
                        except (ValueError, IndexError):
                            continue
            except Exception as e:
                print(f"Error fetching {base_url}: {e}")

        all_stocks.sort(key=lambda x: x['amount'], reverse=True)

        final_stocks = []
        for i, stock in enumerate(all_stocks[:limit]):
            stock['rank'] = i + 1
            final_stocks.append(stock)

        return final_stocks

    else:
        # Use sise_quant for Top Volume
        url = "https://finance.naver.com/sise/sise_quant.naver"
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=10)
            soup = BeautifulSoup(response.content.decode('euc-kr', 'replace'), 'html.parser')
            
            table = soup.select_one('table.type_2')
            if not table:
                return []
                
            stocks = []
            rows = table.select('tr')
            
            generated_rank = 1
            for row in rows:
                cols = row.select('td')
                # Valid data row check
                if not (len(cols) > 5 and cols[0].text.strip().isdigit()):
                    continue
                    
                try:
                    # Use generated rank instead of static 'N' column
                    rank = generated_rank
                    name = cols[1].text.strip()
                    price_str = cols[2].text.strip()
                    rate_str = cols[4].text.strip().strip()
                    vol_str = cols[5].text.strip()
                    amt_str = cols[6].text.strip() # explicit amount
                    
                    volume = int(vol_str.replace(',', ''))
                    amount = int(amt_str.replace(',', '')) if amt_str else 0

                    rate_val = 0.0
                    clean_rate = rate_str.replace('%', '').strip()
                    if clean_rate:
                        rate_val = float(clean_rate)
                    
                    item = {
                        "rank": rank,
                        "name": name,
                        "price": price_str,
                        "rate_str": rate_str,
                        "rate": rate_val,
                        "volume": volume,
                        "amount": amount,
                        "amount_str": amt_str
                    }
                    stocks.append(item)
                    generated_rank += 1
                except (ValueError, IndexError) as e:
                    # print(f"Skipping row: {e}")
                    continue
                    
            return stocks[:limit]
            
        except Exception as e:
            print(f"Error fetching top stocks: {e}")
            return []
def get_theme_details(theme_no):
    """
    Fetches all stocks belonging to a theme with their real-time data.
    Args:
        theme_no (str): Naver Finance theme ID.
    Returns:
        list: List of dicts containing stock data.
    """
    url = f"https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={theme_no}"
    stocks = []
    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(response.content.decode('euc-kr', 'replace'), 'html.parser')
        
        table = soup.select_one('table.type_5')
        if not table:
            return []
            
        rows = table.select('tr')
        for row in rows:
            cols = row.select('td')
            if len(cols) >= 9:
                link = cols[0].select_one('a')
                if not link: continue
                
                name = link.text.strip().replace('*', '').strip()
                code = link.get('href', '').split('code=')[-1].split('&')[0]
                
                price = cols[2].text.strip().replace(',', '')
                rate = cols[4].text.strip().replace('%', '').replace('+', '')
                amount = cols[8].text.strip().replace(',', '')
                
                try:
                    stocks.append({
                        "name": name,
                        "code": code,
                        "price": int(price) if price else 0,
                        "rate": float(rate) if rate else 0.0,
                        "amount": int(amount) if amount else 0
                    })
                except ValueError:
                    continue
                    
        return stocks
    except Exception as e:
        print(f"Error fetching theme details for {theme_no}: {e}")
        return []

