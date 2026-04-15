from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os
import json


DATA_DIR = os.path.join(os.path.dirname(__file__), 'market_data')
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'index_constituents.json')


def get_index_constituents():
    """Step 1: Load index constituents and build stock-to-indices mapping."""
    with open(CONFIG_PATH) as f:
        index_data = json.load(f)

    # Build reverse map: ticker -> list of indices it belongs to
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


def fetch_stock_data(**context):
    """Step 2: Fetch hourly price data for all stocks."""
    import yfinance as yf

    stock_indices = context['ti'].xcom_pull(task_ids='get_index_constituents')
    if not stock_indices:
        print("No stock list available.")
        return []

    tickers = list(stock_indices.keys())
    print(f"Fetching data for {len(tickers)} stocks...")

    all_records = []
    fetch_time = datetime.utcnow().isoformat()

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


def store_to_parquet(**context):
    """Step 3: Store the data in a daily parquet file."""
    import pandas as pd

    records = context['ti'].xcom_pull(task_ids='fetch_stock_data')
    if not records:
        print("No records to store.")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    today = datetime.utcnow().strftime('%Y-%m-%d')
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


dag = DAG(
    'market_data',
    start_date=datetime(2025, 1, 1),
    schedule='0 * * * *',
    catchup=False,
)

get_constituents_task = PythonOperator(
    task_id='get_index_constituents',
    python_callable=get_index_constituents,
    dag=dag,
)

fetch_data_task = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_stock_data,
    dag=dag,
)

store_data_task = PythonOperator(
    task_id='store_to_parquet',
    python_callable=store_to_parquet,
    dag=dag,
)

get_constituents_task >> fetch_data_task >> store_data_task
