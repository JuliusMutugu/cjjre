import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
import warnings
warnings.filterwarnings('ignore')

print("🎯 TESTING WITH RECENT EVENTS - COMPANIES THAT DEFINITELY EXIST")
print("=" * 65)

# Parameters for strategies
BUY_DELAYS = [1, 7, 14, 28]
SELL_DELAYS = [30, 60]
IYW_TICKER = 'IYW'

# Load the event data
input_csv = 'Testing Data for Upwork -- with Tickers -- R.csv'
df = pd.read_csv(input_csv)

print(f"📊 Loading {len(df)} events for processing...")

# Convert date column
df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

# Filter to recent events (2020 onwards) where companies definitely exist
recent_df = df[df['date'] >= '2020-01-01'].head(50).copy()
print(f"🧪 Processing {len(recent_df)} recent events (2020+) with companies that definitely exist...")

# Show sample of what we're testing
print("\n📋 Sample events we're testing:")
for i, row in recent_df.head(10).iterrows():
    print(f"   {row['ticker']} on {row['date'].strftime('%Y-%m-%d')}")

# Data cache
data_cache = {}

def get_price_on_date(ticker, target_date):
    """Get price for a specific date"""
    if ticker not in data_cache:
        # Download data on demand
        try:
            start_date = target_date - timedelta(days=40)
            end_date = target_date + timedelta(days=70)
            print(f"📡 Downloading {ticker} for {target_date.strftime('%Y-%m-%d')}...")
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            
            if not data.empty:
                # Check if we have the right columns
                if len(data.columns) > 0:
                    # Handle both single ticker and multi-ticker downloads
                    if 'Adj Close' in data.columns:
                        data_cache[ticker] = data
                        print(f"     ✅ Downloaded {len(data)} days of data for {ticker}")
                    elif len(data.columns) > 1 and data.columns.nlevels > 1:
                        # Multi-level columns case
                        adj_close_data = data.xs('Adj Close', level=1, axis=1)
                        if not adj_close_data.empty:
                            # Create simplified DataFrame
                            simple_data = pd.DataFrame({'Adj Close': adj_close_data.iloc[:, 0]})
                            data_cache[ticker] = simple_data
                            print(f"     ✅ Downloaded {len(simple_data)} days of data for {ticker}")
                        else:
                            data_cache[ticker] = None
                            print(f"     ❌ No Adj Close data for {ticker}")
                            return None
                    else:
                        data_cache[ticker] = None
                        print(f"     ❌ Unexpected data structure for {ticker}")
                        return None
                else:
                    data_cache[ticker] = None
                    print(f"     ❌ Empty data for {ticker}")
                    return None
            else:
                data_cache[ticker] = None
                print(f"     ❌ No data downloaded for {ticker}")
                return None
        except Exception as e:
            data_cache[ticker] = None
            print(f"     ❌ Error downloading {ticker}: {str(e)[:100]}")
            return None
    
    data = data_cache[ticker]
    if data is None:
        return None
        
    # Try exact date first
    if target_date in data.index:
        return float(data.loc[target_date, 'Adj Close'])
    
    # Find next available trading day within 7 days
    for i in range(1, 8):
        next_date = target_date + timedelta(days=i)
        if next_date in data.index:
            return float(data.loc[next_date, 'Adj Close'])
    
    return None

# Process the recent data
print(f"\n🔄 Processing {len(recent_df)} recent events and filling ALL strategy columns...")

filled_count = 0
for idx, row in recent_df.iterrows():
    event_date = row['date']
    ticker = row['ticker']
    
    print(f"   📈 Processing event {idx+1}: {ticker} on {event_date.strftime('%Y-%m-%d')}")
    
    row_filled = 0
    
    # Process ALL 16 strategy combinations
    for buy_delay in BUY_DELAYS:
        buy_date = event_date + timedelta(days=buy_delay)
        
        for sell_delay in SELL_DELAYS:
            sell_date = buy_date + timedelta(days=sell_delay - 1)
            
            # STOCK trade
            buy_price = get_price_on_date(ticker, buy_date)
            sell_price = get_price_on_date(ticker, sell_date)
            
            if buy_price is not None and sell_price is not None:
                trade_return = (sell_price - buy_price) / buy_price
                
                # Fill correct column based on strategy
                if buy_delay == 1 and sell_delay == 30:
                    recent_df.at[idx, 'Return B1S30'] = trade_return
                elif buy_delay == 7 and sell_delay == 30:
                    recent_df.at[idx, 'B7S30'] = trade_return
                elif buy_delay == 14 and sell_delay == 30:
                    recent_df.at[idx, 'B14S30'] = trade_return
                elif buy_delay == 28 and sell_delay == 30:
                    recent_df.at[idx, 'B28S30'] = trade_return
                elif buy_delay == 1 and sell_delay == 60:
                    recent_df.at[idx, 'B1S60'] = trade_return
                elif buy_delay == 7 and sell_delay == 60:
                    recent_df.at[idx, 'B7S60'] = trade_return
                elif buy_delay == 14 and sell_delay == 60:
                    recent_df.at[idx, 'B14S60'] = trade_return
                elif buy_delay == 28 and sell_delay == 60:
                    recent_df.at[idx, 'B28S60'] = trade_return
                
                filled_count += 1
                row_filled += 1
                print(f"       ✅ Stock B{buy_delay}S{sell_delay}: {trade_return:.4f}")
            
            # IYW trade
            buy_price_iyw = get_price_on_date(IYW_TICKER, buy_date)
            sell_price_iyw = get_price_on_date(IYW_TICKER, sell_date)
            
            if buy_price_iyw is not None and sell_price_iyw is not None:
                trade_return_iyw = (sell_price_iyw - buy_price_iyw) / buy_price_iyw
                
                # Fill correct IYW column
                if buy_delay == 1 and sell_delay == 30:
                    recent_df.at[idx, 'IYW B1S30'] = trade_return_iyw
                elif buy_delay == 7 and sell_delay == 30:
                    recent_df.at[idx, 'B7S30.1'] = trade_return_iyw
                elif buy_delay == 14 and sell_delay == 30:
                    recent_df.at[idx, 'B14S30.1'] = trade_return_iyw
                elif buy_delay == 28 and sell_delay == 30:
                    recent_df.at[idx, 'B28S30.1'] = trade_return_iyw
                elif buy_delay == 1 and sell_delay == 60:
                    recent_df.at[idx, 'B1S60.1'] = trade_return_iyw
                elif buy_delay == 7 and sell_delay == 60:
                    recent_df.at[idx, 'B7S60.1'] = trade_return_iyw
                elif buy_delay == 14 and sell_delay == 60:
                    recent_df.at[idx, 'B14S60.1'] = trade_return_iyw
                elif buy_delay == 28 and sell_delay == 60:
                    recent_df.at[idx, 'B28S60.1'] = trade_return_iyw
                
                filled_count += 1
                row_filled += 1
                print(f"       ✅ IYW B{buy_delay}S{sell_delay}: {trade_return_iyw:.4f}")
    
    print(f"     🎯 TOTAL FILLED for {ticker}: {row_filled}/16 values")

# Save the result
output_file = 'RECENT_EVENTS_FILLED_TEST.csv'
recent_df.to_csv(output_file, index=False)

print(f"\n✅ RECENT EVENTS TEST COMPLETED!")
print(f"📁 Output file: {output_file}")
print(f"📊 Total values filled: {filled_count}")

# Show fill rates
strategy_columns = ['Return B1S30', 'B7S30', 'B14S30', 'B28S30', 'B1S60', 'B7S60', 'B14S60', 'B28S60',
                   'IYW B1S30', 'B7S30.1', 'B14S30.1', 'B28S30.1', 'B1S60.1', 'B7S60.1', 'B14S60.1', 'B28S60.1']

print(f"\n📈 FILL RATES FOR RECENT EVENTS:")
for col in strategy_columns:
    if col in recent_df.columns:
        filled = recent_df[col].notna().sum()
        rate = filled / len(recent_df) * 100
        print(f"   {col}: {filled}/{len(recent_df)} ({rate:.1f}%)")

if filled_count > 0:
    print(f"\n🎉 SUCCESS! The approach WORKS with real Yahoo Finance data!")
    print(f"🔄 Now we can scale this to process all {len(df):,} events in batches.")
else:
    print(f"\n❌ STILL NO DATA - Need to investigate Yahoo Finance API issues")
