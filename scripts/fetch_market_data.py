"""Standalone script to fetch NSE stock data and store as parquet."""
import os
import json
from datetime import datetime, timezone
import yfinance as yf
import pandas as pd


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
CONFIG_PATH = os.path.join(ROOT_DIR, 'index_constituents.json')
DATA_DIR = os.path.join(ROOT_DIR, 'market_data')


def get_stock_indices_map():
    """Load index constituents and build stock-to-indices mapping."""
    with open(CONFIG_PATH) as f:
        index_data = json.load(f)

    stock_indices = {}
    for index_name, stocks in index_data.items():
        print(f"{index_name}: {len(stocks)} stocks")
        for symbol in stocks:
            ticker = f"{symbol}.NS"
            if ticker not in stock_indices:
                stock_indices[ticker] = []
            stock_indices[ticker].append(index_name)

    print(f"\nTotal unique stocks: {len(stock_indices)}")
    return stock_indices


def fetch_stock_data(stock_indices):
    """Fetch hourly price data for all stocks."""
    tickers = list(stock_indices.keys())
    print(f"Fetching data for {len(tickers)} stocks...")

    all_records = []
    fetch_time = datetime.now(timezone.utc).isoformat()

    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_str = ' '.join(batch)
        try:
            data = yf.download(batch_str, period='1d', interval='1h', group_by='ticker', threads=True)
            for ticker in batch:
                try:
                    if len(batch) == 1:
                        hist = data
                    else:
                        hist = data[ticker]
                    if hist.empty or hist.dropna(how='all').empty:
                        continue
                    for ts, row in hist.iterrows():
                        if row.dropna().empty:
                            continue
                        all_records.append({
                            'Datetime': str(ts),
                            'ticker': ticker,
                            'Open': row.get('Open'),
                            'High': row.get('High'),
                            'Low': row.get('Low'),
                            'Close': row.get('Close'),
                            'Volume': row.get('Volume'),
                            'indices': json.dumps(stock_indices[ticker]),
                            'fetch_time': fetch_time,
                        })
                except Exception as e:
                    print(f"Error processing {ticker}: {e}")
        except Exception as e:
            print(f"Error downloading batch starting at {i}: {e}")

    print(f"Fetched {len(all_records)} total records")
    return all_records


def store_to_parquet(records):
    """Store records in a daily parquet file."""
    if not records:
        print("No records to store.")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    file_path = os.path.join(DATA_DIR, f'{today}.parquet')

    new_data = pd.DataFrame(records)

    if os.path.exists(file_path):
        existing = pd.read_parquet(file_path)
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined.drop_duplicates(
            subset=['ticker', 'Datetime'], keep='last', inplace=True
        )
        combined.to_parquet(file_path, index=False)
        print(f"Appended to {file_path} — total {len(combined)} rows")
    else:
        new_data.to_parquet(file_path, index=False)
        print(f"Created {file_path} with {len(new_data)} rows")


if __name__ == '__main__':
    stock_map = get_stock_indices_map()
    records = fetch_stock_data(stock_map)
    store_to_parquet(records)
