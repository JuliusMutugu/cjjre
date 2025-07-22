import pandas as pd

# Test the column mapping logic
print("🔍 Testing column mapping...")

# Load first few rows to check column names
df = pd.read_csv("Testing Data for Upwork -- with Tickers -- R.csv", nrows=5)
print(f"📊 CSV columns: {list(df.columns)}")

# Test the column mapping
buy_delays = [1, 7, 14, 28]
sell_delays = [30, 60]
assets = ['Stock', 'IYW']

column_mapping = {}
for buy_delay in buy_delays:
    for sell_delay in sell_delays:
        # Stock columns - only B1S30 has "Return" prefix
        if buy_delay == 1 and sell_delay == 30:
            stock_col = f"Return B1S{sell_delay}"
        elif buy_delay == 1:
            stock_col = f"B1S{sell_delay}"
        else:
            stock_col = f"B{buy_delay}S{sell_delay}"
        column_mapping[(buy_delay, sell_delay, 'Stock')] = stock_col
        
        # IYW columns - only B1S30 has "IYW" prefix
        if buy_delay == 1 and sell_delay == 30:
            iyw_col = f"IYW B1S{sell_delay}"
        elif buy_delay == 1:
            iyw_col = f"B1S{sell_delay}.1"
        else:
            iyw_col = f"B{buy_delay}S{sell_delay}.1"
        column_mapping[(buy_delay, sell_delay, 'IYW')] = iyw_col

print(f"\n📋 Generated column mappings:")
for strategy, col_name in column_mapping.items():
    buy_delay, sell_delay, asset = strategy
    exists = col_name in df.columns
    status = "✅" if exists else "❌"
    print(f"   {status} {strategy} -> '{col_name}' (exists: {exists})")

print(f"\n🎯 Missing columns:")
for strategy, col_name in column_mapping.items():
    if col_name not in df.columns:
        print(f"   ❌ {strategy} -> '{col_name}' NOT FOUND")
