import requests

url = "https://finance.naver.com/item/main.naver?code=005930"
headers = {'User-Agent': 'Mozilla/5.0'}

try:
    response = requests.get(url, headers=headers)
    content = response.content
    
    print(f"Original Encoding from headers: {response.encoding}")
    
    encodings_to_try = ['utf-8', 'euc-kr', 'cp949']
    
    found = False
    for enc in encodings_to_try:
        try:
            decoded = content.decode(enc)
            if "삼성전자" in decoded:
                print(f"SUCCESS: Found '삼성전자' using encoding: {enc}")
                found = True
                break
            else:
                print(f"FAILED: '삼성전자' not found using encoding: {enc}")
        except UnicodeDecodeError:
            print(f"ERROR: Could not decode using {enc}")
            
    if not found:
        print("CRITICAL: Could not find '삼성전자' with any common encoding.")

except Exception as e:
    print(f"Error: {e}")
