import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time

def test_single_calculation():
    """Test calculating one specific return value"""
    
    # Use ORCL which we know works
    ticker = "ORCL"
    event_date = datetime(2006, 5, 19)  # From our test data
    buy_delay = 1
    sell_delay = 30
    
    print(f"🎯 Testing single calculation: {ticker} on {event_date.strftime('%Y-%m-%d')}")
    print(f"   Strategy: Buy +{buy_delay} days, Sell +{sell_delay} days")
    
    try:
        # Calculate dates (skip weekends)
        buy_date = event_date + timedelta(days=buy_delay)
        while buy_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            buy_date += timedelta(days=1)
            
        sell_date = event_date + timedelta(days=sell_delay)
        while sell_date.weekday() >= 5:
            sell_date += timedelta(days=1)
        
        print(f"   Buy date: {buy_date.strftime('%Y-%m-%d')}")
        print(f"   Sell date: {sell_date.strftime('%Y-%m-%d')}")
        
        # Get buy price
        print(f"\n📈 Getting buy price...")
        buy_start = buy_date - timedelta(days=3)
        buy_end = buy_date + timedelta(days=3)
        
        print(f"   Downloading {ticker} from {buy_start.strftime('%Y-%m-%d')} to {buy_end.strftime('%Y-%m-%d')}")
        buy_data = yf.download(ticker, start=buy_start, end=buy_end, progress=False)
        
        if buy_data.empty:
            print(f"   ❌ No buy data")
            return None
            
        print(f"   ✅ Got {len(buy_data)} days of buy data")
        
        # Find buy price
        buy_target = pd.Timestamp(buy_date.date())
        if buy_target in buy_data.index:
            close_col = [col for col in buy_data.columns if 'Close' in str(col)][0]
            buy_price = float(buy_data.loc[buy_target, close_col])
            print(f"   💰 Buy price: ${buy_price:.2f}")
        else:
            print(f"   ❌ Buy date not found in data")
            return None
            
        # Get sell price
        print(f"\n📉 Getting sell price...")
        sell_start = sell_date - timedelta(days=3)
        sell_end = sell_date + timedelta(days=3)
        
        print(f"   Downloading {ticker} from {sell_start.strftime('%Y-%m-%d')} to {sell_end.strftime('%Y-%m-%d')}")
        sell_data = yf.download(ticker, start=sell_start, end=sell_end, progress=False)
        
        if sell_data.empty:
            print(f"   ❌ No sell data")
            return None
            
        print(f"   ✅ Got {len(sell_data)} days of sell data")
        
        # Find sell price
        sell_target = pd.Timestamp(sell_date.date())
        if sell_target in sell_data.index:
            close_col = [col for col in sell_data.columns if 'Close' in str(col)][0]
            sell_price = float(sell_data.loc[sell_target, close_col])
            print(f"   💰 Sell price: ${sell_price:.2f}")
        else:
            print(f"   ❌ Sell date not found in data")
            return None
            
        # Calculate return
        return_value = (sell_price - buy_price) / buy_price
        print(f"\n🎉 SUCCESS: Return = {return_value:.6f} ({return_value*100:.2f}%)")
        return return_value
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Run the test
result = test_single_calculation()
if result is not None:
    print(f"✅ Test successful! Can proceed with batch processing.")
else:
    print(f"❌ Test failed. Need to fix connectivity issues.")
