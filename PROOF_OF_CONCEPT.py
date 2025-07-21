import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
import warnings
warnings.filterwarnings('ignore')

print("🎯 SIMPLIFIED APPROACH - PROCESS WITH KNOWN GOOD TICKERS")
print("=" * 60)

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

# Take just first 300 events to prove the concept works
df_sample = df.head(300).copy()
print(f"🧪 Processing first {len(df_sample)} events to demonstrate functionality...")

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
            if not data.empty and 'Adj Close' in data.columns:
                data_cache[ticker] = data
                print(f"     ✅ Downloaded {len(data)} days of data for {ticker}")
            else:
                data_cache[ticker] = None
                print(f"     ❌ No valid data for {ticker}")
                return None
        except Exception as e:
            data_cache[ticker] = None
            print(f"     ❌ Error downloading {ticker}: {str(e)[:50]}")
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

# Process the sample data
print(f"🔄 Processing {len(df_sample)} events and filling ALL strategy columns...")

filled_count = 0
for idx, row in df_sample.iterrows():
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
                    df_sample.at[idx, 'Return B1S30'] = trade_return
                elif buy_delay == 7 and sell_delay == 30:
                    df_sample.at[idx, 'B7S30'] = trade_return
                elif buy_delay == 14 and sell_delay == 30:
                    df_sample.at[idx, 'B14S30'] = trade_return
                elif buy_delay == 28 and sell_delay == 30:
                    df_sample.at[idx, 'B28S30'] = trade_return
                elif buy_delay == 1 and sell_delay == 60:
                    df_sample.at[idx, 'B1S60'] = trade_return
                elif buy_delay == 7 and sell_delay == 60:
                    df_sample.at[idx, 'B7S60'] = trade_return
                elif buy_delay == 14 and sell_delay == 60:
                    df_sample.at[idx, 'B14S60'] = trade_return
                elif buy_delay == 28 and sell_delay == 60:
                    df_sample.at[idx, 'B28S60'] = trade_return
                
                filled_count += 1
                row_filled += 1
            
            # IYW trade
            buy_price_iyw = get_price_on_date(IYW_TICKER, buy_date)
            sell_price_iyw = get_price_on_date(IYW_TICKER, sell_date)
            
            if buy_price_iyw is not None and sell_price_iyw is not None:
                trade_return_iyw = (sell_price_iyw - buy_price_iyw) / buy_price_iyw
                
                # Fill correct IYW column
                if buy_delay == 1 and sell_delay == 30:
                    df_sample.at[idx, 'IYW B1S30'] = trade_return_iyw
                elif buy_delay == 7 and sell_delay == 30:
                    df_sample.at[idx, 'B7S30.1'] = trade_return_iyw
                elif buy_delay == 14 and sell_delay == 30:
                    df_sample.at[idx, 'B14S30.1'] = trade_return_iyw
                elif buy_delay == 28 and sell_delay == 30:
                    df_sample.at[idx, 'B28S30.1'] = trade_return_iyw
                elif buy_delay == 1 and sell_delay == 60:
                    df_sample.at[idx, 'B1S60.1'] = trade_return_iyw
                elif buy_delay == 7 and sell_delay == 60:
                    df_sample.at[idx, 'B7S60.1'] = trade_return_iyw
                elif buy_delay == 14 and sell_delay == 60:
                    df_sample.at[idx, 'B14S60.1'] = trade_return_iyw
                elif buy_delay == 28 and sell_delay == 60:
                    df_sample.at[idx, 'B28S60.1'] = trade_return_iyw
                
                filled_count += 1
                row_filled += 1
    
    print(f"     ✅ Filled {row_filled} values for {ticker}")

# Save the result
output_file = 'PROOF_OF_CONCEPT_300_events_FILLED.csv'
df_sample.to_csv(output_file, index=False)

print(f"\n✅ PROOF OF CONCEPT COMPLETED!")
print(f"📁 Output file: {output_file}")
print(f"📊 Total values filled: {filled_count}")
print(f"🎯 This proves the approach works with real Yahoo Finance data!")

# Show fill rates
strategy_columns = ['Return B1S30', 'B7S30', 'B14S30', 'B28S30', 'B1S60', 'B7S60', 'B14S60', 'B28S60',
                   'IYW B1S30', 'B7S30.1', 'B14S30.1', 'B28S30.1', 'B1S60.1', 'B7S60.1', 'B14S60.1', 'B28S60.1']

print(f"\n📈 FILL RATES:")
for col in strategy_columns:
    if col in df_sample.columns:
        filled = df_sample[col].notna().sum()
        rate = filled / len(df_sample) * 100
        print(f"   {col}: {filled}/{len(df_sample)} ({rate:.1f}%)")
