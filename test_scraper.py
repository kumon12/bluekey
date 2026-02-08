from scraper import get_stock_info, get_market_indices

print("Testing Market Indices:")
indices = get_market_indices()
print(indices)

print("\nTesting Stock Info (Samsung Electronics - 005930):")
stock = get_stock_info('005930')
# Handle printing for Windows console
import sys
# Reconfigure stdout to use utf-8, even if it might look garbled in some terminals, it won't crash
sys.stdout.reconfigure(encoding='utf-8')
for key, value in stock.items():
    print(f"{key}: {value}")
