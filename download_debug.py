import requests

url = "https://finance.naver.com/item/main.naver?code=005930"
try:
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    response.raise_for_status()
    # Save as utf-8 for easy reading
    content = response.content.decode('euc-kr', 'replace')
    with open("debug.html", "w", encoding="utf-8") as f:
        f.write(content)
    print("Download successful")
except Exception as e:
    print(f"Error: {e}")
