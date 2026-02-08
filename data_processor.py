
def clean_price(price_str):
    """
    Converts price string (e.g., '70,000') to integer.
    """
    if not price_str or price_str == "N/A":
        return 0
    return int(price_str.replace(',', ''))

def clean_rate(rate_str):
    """
    Converts rate string (e.g., '1.54') to float.
    """
    if not rate_str:
        return 0.0
    return float(rate_str.replace('+', '').replace('-', '').replace('%', ''))
