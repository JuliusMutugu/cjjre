import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def test_ticker_data(ticker, test_date_str):
    """Test if we can get data for a specific ticker and date"""
    print(f"\n🔍 Testing {ticker} around {test_date_str}")
    
    # Convert date format
    test_date = datetime.strptime(test_date_str, '%Y%m%d')
    start_date = test_date - timedelta(days=5)
    end_date = test_date + timedelta(days=5)
    
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if not data.empty:
            print(f"✅ SUCCESS: Got {len(data)} days of data")
            print(f"   Date range: {data.index[0].strftime('%Y-%m-%d')} to {data.index[-1].strftime('%Y-%m-%d')}")
            if 'Close' in data.columns:
                print(f"   Sample prices: {data['Close'].iloc[0]:.2f} to {data['Close'].iloc[-1]:.2f}")
            elif ('Close', ticker) in data.columns:
                print(f"   Sample prices: {data[('Close', ticker)].iloc[0]:.2f} to {data[('Close', ticker)].iloc[-1]:.2f}")
            return True
        else:
            print("❌ FAILED: No data returned")
            return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

# Test problematic tickers from the batch results
test_cases = [
    ("ORCL", "20010508"),  # Oracle - should definitely work
    ("ORCL", "20060519"),  # Oracle - recent date from batch
    ("HGR", "20071207"),   # HGR - mixed results
    ("AEPI", "20140207"),  # AEPI - delisted, expected to fail
    ("SUNW", "20050819"),  # Sun Microsystems - might be delisted
    ("IYW", "20140207"),   # IYW ETF - should always work
]

print("🎯 Testing Yahoo Finance API for problematic tickers...")
success_count = 0
total_tests = len(test_cases)

for ticker, date in test_cases:
    if test_ticker_data(ticker, date):
        success_count += 1

print(f"\n📊 Summary: {success_count}/{total_tests} tests successful ({success_count/total_tests*100:.1f}%)")

# Test the current column structure for IYW
print(f"\n🔍 Testing IYW column structure...")
try:
    data = yf.download("IYW", start="2014-02-05", end="2014-02-15", progress=False)
    if not data.empty:
        print(f"✅ IYW columns: {list(data.columns)}")
        print(f"   Data shape: {data.shape}")
    else:
        print("❌ No IYW data")
except Exception as e:
    print(f"❌ IYW error: {e}")
