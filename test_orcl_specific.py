import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def test_orcl_specific():
    """Test ORCL specifically with the exact same logic as production script"""
    
    # Test case from our batch: ORCL on 2001-05-08
    event_date = datetime.strptime("20010508", "%Y%m%d")
    ticker = "ORCL"
    buy_delay = 1
    sell_delay = 30
    
    print(f"🔍 Testing {ticker} calculation for event {event_date.strftime('%Y-%m-%d')}")
    print(f"   Buy delay: {buy_delay}, Sell delay: {sell_delay}")
    
    # Calculate buy and sell dates  
    buy_date = event_date + timedelta(days=buy_delay)
    sell_date = event_date + timedelta(days=sell_delay)
    
    print(f"   Buy date: {buy_date.strftime('%Y-%m-%d')}")
    print(f"   Sell date: {sell_date.strftime('%Y-%m-%d')}")
    
    # Test getting buy price
    print(f"\n📈 Getting buy price for {buy_date.strftime('%Y-%m-%d')}...")
    buy_price = get_price_on_date(buy_date, ticker)
    if buy_price:
        print(f"   ✅ Buy price: ${buy_price:.2f}")
    else:
        print(f"   ❌ Could not get buy price")
        
    # Test getting sell price
    print(f"\n📉 Getting sell price for {sell_date.strftime('%Y-%m-%d')}...")
    sell_price = get_price_on_date(sell_date, ticker)
    if sell_price:
        print(f"   ✅ Sell price: ${sell_price:.2f}")
    else:
        print(f"   ❌ Could not get sell price")
        
    # Calculate return if both prices available
    if buy_price and sell_price:
        return_value = (sell_price - buy_price) / buy_price
        print(f"\n💰 Calculated return: {return_value:.6f} ({return_value*100:.2f}%)")
        return return_value
    else:
        print(f"\n❌ Cannot calculate return - missing price data")
        return None

def get_price_on_date(date, ticker):
    """Get price for ticker on specific date - same logic as production"""
    try:
        # Create date range (get a few days to ensure we have data)
        start_date = date - timedelta(days=5)
        end_date = date + timedelta(days=5)
        
        print(f"      Downloading {ticker} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        
        if data.empty:
            print(f"      ❌ No data returned for {ticker}")
            return None
        
        print(f"      ✅ Got {len(data)} days of data")
        print(f"      📊 Columns: {list(data.columns)}")
        print(f"      📅 Date range: {data.index[0].strftime('%Y-%m-%d')} to {data.index[-1].strftime('%Y-%m-%d')}")
        
        # Find the exact date or the closest available date
        target_date = pd.Timestamp(date.date())
        
        if target_date in data.index:
            print(f"      🎯 Found exact date {target_date.strftime('%Y-%m-%d')}")
            # Access multi-level columns correctly: ('Close', 'TICKER')
            close_cols = [col for col in data.columns if 'Close' in str(col)]
            if close_cols:
                print(f"      📈 Using close column: {close_cols[0]}")
                price = data.loc[target_date, close_cols[0]]
                return float(price)
            else:
                print(f"      ❌ No Close column found")
                return None
        else:
            print(f"      🔍 Target date {target_date.strftime('%Y-%m-%d')} not found, finding closest...")
            # Find closest date
            available_dates = data.index
            if len(available_dates) > 0:
                closest_date = min(available_dates, key=lambda x: abs(x - target_date))
                print(f"      🎯 Using closest date: {closest_date.strftime('%Y-%m-%d')}")
                
                close_cols = [col for col in data.columns if 'Close' in str(col)]
                if close_cols:
                    print(f"      📈 Using close column: {close_cols[0]}")
                    price = data.loc[closest_date, close_cols[0]]
                    return float(price)
                else:
                    print(f"      ❌ No Close column found")
                    return None
            
            print(f"      ❌ No available dates")
            return None
                
    except Exception as e:
        print(f"      ❌ Error getting price for {ticker} on {date}: {e}")
        return None

# Run the test
test_orcl_specific()
