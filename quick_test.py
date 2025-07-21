import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np

# Load just first 5 events for QUICK testing
input_csv = 'Testing Data for Upwork -- with Tickers -- R.csv'
df = pd.read_csv(input_csv)
df_mini = df.head(5).copy()  # Just 5 events for super quick test

print(f"🚀 QUICK TEST: Processing {len(df_mini)} events with IMMEDIATE SAVING")

# Convert dates
df_mini['date'] = pd.to_datetime(df_mini['date'], format='%Y%m%d')

# Parameters
BUY_DELAYS = [1, 7, 14, 28]
SELL_DELAYS = [30, 60]
IYW_TICKER = 'IYW'

# Use mock data for speed (you can replace with real data later)
def get_mock_price(ticker, date):
    """Returns mock price for demonstration - replace with real data for production"""
    return 100.0 + hash(f"{ticker}{date}") % 50  # Mock price between 100-150

output_file = 'Quick_Test_Results.csv'
filled_count = 0

print(f"💾 Will save after EVERY event - even if interrupted after event 2!")

for idx, row in df_mini.iterrows():
    print(f"\n📈 Processing event {idx+1}/{len(df_mini)}: {row['ticker']} on {row['date'].strftime('%Y-%m-%d')}")
    
    event_date = row['date']
    ticker = row['ticker']
    event_returns = 0
    
    for buy_delay in BUY_DELAYS:
        buy_date = event_date + timedelta(days=buy_delay)
        
        for sell_delay in SELL_DELAYS:
            sell_date = buy_date + timedelta(days=sell_delay - 1)
            
            # Mock calculations (replace with real Yahoo Finance data)
            buy_price = get_mock_price(ticker, buy_date)
            sell_price = get_mock_price(ticker, sell_date)
            
            # Stock return
            trade_return = (sell_price - buy_price) / buy_price
            col = f'Return B{buy_delay}S{sell_delay}'
            df_mini.at[idx, col] = trade_return
            filled_count += 1
            event_returns += 1
            
            # IYW return
            buy_price_iyw = get_mock_price(IYW_TICKER, buy_date)
            sell_price_iyw = get_mock_price(IYW_TICKER, sell_date)
            trade_return_iyw = (sell_price_iyw - buy_price_iyw) / buy_price_iyw
            col_iyw = f'IYW B{buy_delay}S{sell_delay}'
            df_mini.at[idx, col_iyw] = trade_return_iyw
            filled_count += 1
            event_returns += 1
    
    # 🔥 SAVE IMMEDIATELY after each event
    df_mini.to_csv(output_file, index=False)
    print(f"   ✅ Event {idx+1} completed: {event_returns} returns calculated")
    print(f"   💾 SAVED to {output_file} - Total progress: {filled_count} returns")
    print(f"   🛡️  Safe to interrupt - no progress lost!")

print(f"\n🎉 DEMONSTRATION COMPLETE!")
print(f"📁 File: {output_file}")
print(f"📊 Shows how script saves after EVERY event")
print(f"🔄 Now apply this pattern to real Yahoo Finance data!")

# Show what was saved
print(f"\n📋 SAMPLE OF SAVED DATA:")
saved_df = pd.read_csv(output_file)
print(saved_df[['date', 'ticker', 'Return B1S30', 'IYW B1S30']].head())
