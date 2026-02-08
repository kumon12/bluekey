import requests
from bs4 import BeautifulSoup

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
        # Naver Finance uses UTF-8 encoding
        soup = BeautifulSoup(response.content.decode('utf-8', 'replace'), 'html.parser')

        # Basic Info
        name = soup.select_one('.wrap_company h2 a').text
        price = soup.select_one('.no_today .blind').text
        
        # Change rate (includes amount and percentage)
        # .no_exday .blind returns [change_amount, change_rate]
        exday_blinds = soup.select('.no_exday .blind')
        if len(exday_blinds) >= 2:
            rate = exday_blinds[1].text
        else:
            rate = "0.0"

        # Volume
        # Found in table with class 'no_info', first row, third column
        try:
            volume_tag = soup.select('.no_info tr')[0].select('td')[2].select_one('.blind')
            volume = volume_tag.text if volume_tag else "0"
        except IndexError:
            volume = "0"

        return {
            "name": name,
            "code": code,
            "current_price": price,
            "rate": rate,
            "volume": volume
        }
    except Exception as e:
        print(f"Error fetching data for {code}: {e}")
        return None

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
        # Strategy: Merge 'Top Market Cap' (to get giants) and 'Top Volume' (to get active mid-caps)
        # sise_market_sum returns Top Market Cap (we fetch KOSPI & KOSDAQ)
        # sise_quant returns Top Volume (we fetch Top 100)
        # Then we calculate Amount for all, deduplicate, and sort.
        
        urls = [
            "https://finance.naver.com/sise/sise_market_sum.naver?sosok=0", # KOSPI Top 50 Market Cap
            "https://finance.naver.com/sise/sise_market_sum.naver?sosok=1", # KOSDAQ Top 50 Market Cap
            "https://finance.naver.com/sise/sise_quant.naver"              # Top 100 Volume
        ]
        
        seen_names = set()
        all_stocks = []
        
        for url in urls:
            try:
                response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                soup = BeautifulSoup(response.content.decode('euc-kr', 'replace'), 'html.parser')
                table = soup.select_one('table.type_2')
                if table:
                    rows = table.select('tr')
                    for row in rows:
                        cols = row.select('td')
                        if not (len(cols) > 5 and cols[0].text.strip().isdigit()):
                            continue
                        
                        try:
                            # Name is key for deduplication
                            name_tag = cols[1].select_one('a')
                            if not name_tag:
                                continue
                            name = name_tag.text.strip()
                            
                            # Extract Code from href
                            href = name_tag['href']
                            stock_code = href.split('=')[-1]
                            
                            if name in seen_names:
                                continue
                            
                            # Parse data
                            # Columns differ slightly but Name(1), Price(2), Change(3), Rate(4) are usually consistent
                            # Volume index varies: 
                            # sise_market_sum: Vol is index 9, Market Cap is index 6
                            # sise_quant: Vol is index 5, No Market Cap
                            
                            is_quant = 'quant' in url
                            
                            price_str = cols[2].text.strip()
                            rate_str = cols[4].text.strip().strip()
                            
                            market_cap_str = "-"
                            
                            if is_quant:
                                vol_str = cols[5].text.strip()
                                # quant Market Cap is likely index 9
                                if len(cols) > 9:
                                    market_cap_str = cols[9].text.strip()
                            else:
                                vol_str = cols[9].text.strip() # Market Sum Volume Index
                                market_cap_str = cols[6].text.strip() # Market Sum Cap Index
                            
                            price = int(price_str.replace(',', ''))
                            volume = int(vol_str.replace(',', ''))
                            amount = (price * volume) // 1000000 # Millions
                            amt_str = f"{amount:,}"
                            
                            # Parse Market Cap to int
                            market_cap_val = 0
                            try:
                                if market_cap_str != "-":
                                    market_cap_val = int(market_cap_str.replace(',', ''))
                            except:
                                pass
                            
                            rate_val = 0.0
                            clean_rate = rate_str.replace('%', '').strip()
                            if clean_rate:
                                rate_val = float(clean_rate)
                                
                            item = {
                                "code": stock_code,
                                "name": name,
                                "price": price, # Return Int
                                "price_str": price_str, # Keep formatted if needed, but we used 'price' in app
                                "rate_str": rate_str,
                                "rate": rate_val,
                                "volume": volume,
                                "amount": amount,
                                "amount_str": amt_str,
                                "market_cap": market_cap_val, # Return Int
                                "market_cap_str": market_cap_str 
                            }
                            all_stocks.append(item)
                            seen_names.add(name)
                        except (ValueError, IndexError):
                            continue
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                
        # Sort merged list by Amount descending
        all_stocks.sort(key=lambda x: x['amount'], reverse=True)
        
        # Assign Ranks
        final_stocks = []
        for i, stock in enumerate(all_stocks[:limit]):
            stock['rank'] = i + 1
            final_stocks.append(stock)
            
        return final_stocks

    else:
        # Use sise_quant for Top Volume
        url = "https://finance.naver.com/sise/sise_quant.naver"
        try:
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
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
