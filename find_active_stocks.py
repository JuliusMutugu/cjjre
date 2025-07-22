import pandas as pd

# Find some active stocks in the dataset
print("🔍 Looking for active stocks in the dataset...")

df = pd.read_csv("Testing Data for Upwork -- with Tickers -- R.csv")

# Get unique tickers and their date ranges
ticker_info = df.groupby('ticker').agg({
    'date': ['min', 'max', 'count']
}).round()

ticker_info.columns = ['earliest_date', 'latest_date', 'event_count']
ticker_info = ticker_info.sort_values('latest_date', ascending=False)

print(f"📊 Top 15 tickers by latest date (most likely to be active):")
print(ticker_info.head(15))

# Look for some modern active stocks
modern_tickers = ticker_info[ticker_info['latest_date'] >= 20200000].head(10)
print(f"\n🎯 Modern active tickers (2020+):")
print(modern_tickers)

# Also check for well-known active stocks
known_active = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
active_in_dataset = []
for ticker in known_active:
    if ticker in df['ticker'].values:
        ticker_data = df[df['ticker'] == ticker]
        latest = ticker_data['date'].max()
        count = len(ticker_data)
        active_in_dataset.append((ticker, latest, count))
        
if active_in_dataset:
    print(f"\n🚀 Known active stocks found in dataset:")
    for ticker, latest, count in active_in_dataset:
        print(f"   {ticker}: Latest date {latest}, {count} events")
else:
    print(f"\n❌ No well-known active stocks found in dataset")
    
# Find some rows with more recent dates for testing
recent_rows = df[df['date'] >= 20150000].head(20)
print(f"\n📅 Sample recent events (2015+) for testing:")
print(recent_rows[['permno', 'date', 'ticker']].to_string(index=False))
