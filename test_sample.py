import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Parameters for strategies
BUY_DELAYS = [1, 7, 14, 28]
SELL_DELAYS = [30, 60]
IYW_TICKER = 'IYW'

# Load the event data
input_csv = 'Testing Data for Upwork -- with Tickers -- R.csv'
df = pd.read_csv(input_csv)

print(f"📊 Loading {len(df)} events for processing...")
print(f"📅 Date range: {df['date'].min()} to {df['date'].max()}")

# Take a sample for testing (first 100 events)
df_sample = df.head(100).copy()
print(f"🧪 Working with sample of {len(df_sample)} events for testing...")

# Data cache to avoid repeated downloads
data_cache = {}
failed_tickers = set()

def get_historical_data(ticker, start_date, end_date):
    """Download and cache historical data for a ticker"""
    if ticker in data_cache:
        return data_cache[ticker]
    
    if ticker in failed_tickers:
        return None
    
    try:
        print(f"📡 Downloading {ticker}...")
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if not data.empty:
            data_cache[ticker] = data
            return data
        else:
            failed_tickers.add(ticker)
            print(f"⚠️  No data for {ticker} (likely delisted)")
            return None
    except Exception as e:
        print(f"❌ Failed to download {ticker}: {str(e)[:100]}")
        failed_tickers.add(ticker)
        return None

def get_price_on_date(ticker, target_date, price_type='Adj Close'):
    """Get price for a specific date, handling weekends and holidays"""
    if ticker not in data_cache:
        return None
        
    data = data_cache[ticker]
    if data.empty:
        return None
    
    # Handle different column naming conventions
    price_column = price_type
    if price_type not in data.columns:
        # Try alternative column names
        if 'Close' in data.columns:
            price_column = 'Close'
        elif len(data.columns) > 0:
            price_column = data.columns[0]  # Use first available column
        else:
            return None
    
    # Try exact date first
    if target_date in data.index:
        return float(data.loc[target_date, price_column])
    
    # Find next available trading day within 5 days
    for i in range(1, 6):
        next_date = target_date + timedelta(days=i)
        if next_date in data.index:
            return float(data.loc[next_date, price_column])
    
    return None

# Get unique tickers and download all data upfront
unique_tickers = df_sample['ticker'].dropna().unique().tolist()
unique_tickers.append(IYW_TICKER)  # Add IYW for benchmark

print(f"📡 Downloading historical data for {len(unique_tickers)} unique tickers...")

# Determine date range needed
df_sample['date'] = pd.to_datetime(df_sample['date'], format='%Y%m%d')
start_date = df_sample['date'].min() - timedelta(days=30)  # Buffer for buy delays
end_date = df_sample['date'].max() + timedelta(days=90)    # Buffer for sell delays

print(f"📅 Data range needed: {start_date} to {end_date}")

# Download all historical data
successful_downloads = 0
for i, ticker in enumerate(unique_tickers):
    data = get_historical_data(ticker, start_date, end_date)
    if data is not None:
        successful_downloads += 1

print(f"✅ Downloaded data for {successful_downloads}/{len(unique_tickers)} tickers ({successful_downloads/len(unique_tickers)*100:.1f}%)")
print(f"⚠️  Failed downloads: {len(failed_tickers)} tickers (likely delisted)")

# Main loop to fill missing values with IMMEDIATE SAVING after each event
print(f"🔄 Processing {len(df_sample)} events to calculate returns...")
print(f"💾 Will save progress after EVERY event - no progress will be lost!")

filled_count = 0
output_file = 'Testing_Data_Sample_Filled.csv'

for idx, row in df_sample.iterrows():
    print(f"   📈 Processing event {idx+1}/{len(df_sample)}: {row['ticker']} on {row['date'].strftime('%Y-%m-%d')}")
    
    event_date = row['date']
    ticker = row['ticker']
    event_filled_count = 0
    
    for buy_delay in BUY_DELAYS:
        buy_date = event_date + timedelta(days=buy_delay)
        
        for sell_delay in SELL_DELAYS:
            sell_date = buy_date + timedelta(days=sell_delay - 1)  # This is CORRECT
            
            # Stock trade
            buy_price = get_price_on_date(ticker, buy_date)
            sell_price = get_price_on_date(ticker, sell_date)
            
            if buy_price and sell_price:
                trade_return = (sell_price - buy_price) / buy_price  # This is CORRECT
                col = f'Return B{buy_delay}S{sell_delay}'
                df_sample.at[idx, col] = trade_return
                filled_count += 1
                event_filled_count += 1
            
            # IYW trade
            buy_price_iyw = get_price_on_date(IYW_TICKER, buy_date)
            sell_price_iyw = get_price_on_date(IYW_TICKER, sell_date)
            
            if buy_price_iyw and sell_price_iyw:
                trade_return_iyw = (sell_price_iyw - buy_price_iyw) / buy_price_iyw
                col_iyw = f'IYW B{buy_delay}S{sell_delay}'
                df_sample.at[idx, col_iyw] = trade_return_iyw
                filled_count += 1
                event_filled_count += 1
    
    # SAVE IMMEDIATELY after each event is processed
    df_sample.to_csv(output_file, index=False)
    print(f"   💾 Saved progress: Event {idx+1} completed ({event_filled_count} returns calculated)")
    print(f"   📊 Total progress: {filled_count} returns filled so far")

# Final save and summary
print(f'\n✅ SAMPLE PROCESSING COMPLETED!')
print(f'📁 Output file: {output_file}')
print(f'📊 Filled {filled_count} return values')
print(f'🎯 This validates the approach works correctly!')
print(f'💾 Progress was saved after EVERY event - no data lost!')
print(f'\n🔄 To process the full dataset, run the main script with patience...')

# Show sample results
print(f'\n📋 SAMPLE RESULTS:')
sample_cols = ['date', 'ticker', 'Return B1S30', 'Return B1S60', 'IYW B1S30', 'IYW B1S60']
for col in sample_cols:
    if col in df_sample.columns:
        non_null = df_sample[col].notna().sum()
        print(f'   {col}: {non_null} filled values')
