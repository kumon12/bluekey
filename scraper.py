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
    Fetches KOSPI/KOSDAQ indices.
    """
    url = "https://finance.naver.com/sise/"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        # Naver Finance uses UTF-8 encoding
        soup = BeautifulSoup(response.content.decode('utf-8', 'replace'), 'html.parser')

        kospi = soup.select_one('#KOSPI_now').text
        kosdaq = soup.select_one('#KOSDAQ_now').text
        
        return {"KOSPI": kospi, "KOSDAQ": kosdaq}
    except Exception as e:
        print(f"Error fetching market indices: {e}")
        return None
