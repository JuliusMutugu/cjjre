import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

print("🔍 DEBUGGING YAHOO FINANCE DATA STRUCTURE")
print("=" * 50)

# Test a few well-known tickers
test_tickers = ['ORCL', 'MSFT', 'AAPL', 'IYW']
test_date = datetime(2020, 6, 1)

for ticker in test_tickers:
    print(f"\n📊 Testing {ticker}...")
    try:
        start_date = test_date - timedelta(days=10)
        end_date = test_date + timedelta(days=10)
        
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        
        if not data.empty:
            print(f"   ✅ Downloaded {len(data)} rows")
            print(f"   📋 Columns: {list(data.columns)}")
            print(f"   📋 Column levels: {data.columns.nlevels}")
            print(f"   📋 Index type: {type(data.index)}")
            
            # Show sample data
            if len(data) > 0:
                sample_date = data.index[0]
                print(f"   📅 Sample date: {sample_date}")
                print(f"   💰 First row data:")
                for col in data.columns:
                    value = data.iloc[0][col]
                    print(f"      {col}: {value}")
                
                # Try to get Adj Close specifically
                if 'Adj Close' in data.columns:
                    adj_close = data.loc[sample_date, 'Adj Close']
                    print(f"   ✅ Adj Close value: {adj_close}")
                else:
                    print(f"   ❌ No 'Adj Close' column found")
                    if data.columns.nlevels > 1:
                        print(f"   🔍 Multi-level columns detected")
                        try:
                            adj_close_data = data.xs('Adj Close', level=1, axis=1)
                            if not adj_close_data.empty:
                                print(f"   ✅ Found Adj Close in level 1: {adj_close_data.iloc[0, 0]}")
                            else:
                                print(f"   ❌ No Adj Close in level 1")
                        except:
                            print(f"   ❌ Error accessing level 1")
        else:
            print(f"   ❌ No data returned")
            
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")

print(f"\n🎯 Now testing the data structure that ACTUALLY works...")

# Test what actually works
try:
    print(f"\n📡 Testing ORCL download...")
    orcl_data = yf.download('ORCL', start='2020-06-01', end='2020-06-10', progress=False)
    
    if not orcl_data.empty:
        print(f"✅ ORCL data downloaded successfully!")
        print(f"📊 Shape: {orcl_data.shape}")
        print(f"📋 Columns: {list(orcl_data.columns)}")
        
        # Show actual data
        print(f"\n📈 First few rows:")
        print(orcl_data.head())
        
        # Test price extraction
        test_date = orcl_data.index[0]
        print(f"\n💰 Testing price extraction for {test_date}:")
        
        # Method 1: Direct column access
        try:
            if 'Adj Close' in orcl_data.columns:
                price1 = orcl_data.loc[test_date, 'Adj Close']
                print(f"   Method 1 (direct): {price1}")
            else:
                print(f"   Method 1: No 'Adj Close' column")
        except Exception as e:
            print(f"   Method 1 error: {e}")
        
        # Method 2: iloc access
        try:
            price2 = orcl_data.iloc[0]['Adj Close'] if 'Adj Close' in orcl_data.columns else None
            print(f"   Method 2 (iloc): {price2}")
        except Exception as e:
            print(f"   Method 2 error: {e}")
        
        # Method 3: Get any price column that exists
        try:
            available_price_cols = [col for col in orcl_data.columns if 'Close' in str(col)]
            print(f"   Available price columns: {available_price_cols}")
            
            if available_price_cols:
                price3 = orcl_data.loc[test_date, available_price_cols[0]]
                print(f"   Method 3 (first Close col): {price3}")
        except Exception as e:
            print(f"   Method 3 error: {e}")
            
except Exception as e:
    print(f"❌ ORCL test failed: {e}")

print(f"\n🎯 CONCLUSION:")
print(f"We need to identify the WORKING method and use that in our main script.")
