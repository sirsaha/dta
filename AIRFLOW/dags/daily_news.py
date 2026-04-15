from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os


def fetch_daily_news():
    """Fetch top news headlines from the last 24 hours using NewsAPI."""
    response = requests.get(
        'https://newsapi.org/v2/top-headlines',
        params={
            'category': 'general',
            'pageSize': 20,
            'apiKey': Variable.get('news'),
        },
    )
    data = response.json()

    if data.get('status') != 'ok':
        raise Exception(f"NewsAPI error: {data.get('message', 'Unknown error')}")

    articles = data.get('articles', [])
    print(f"Found {len(articles)} articles\n")
    for i, article in enumerate(articles, 1):
        print(f"{i}. [{article.get('source', {}).get('name')}] {article['title']}")

    return articles


def write_news_to_file(**context):
    """Write fetched news to a file. If no news, write 'nothing to catch up'."""
    articles = context['ti'].xcom_pull(task_ids='fetch_daily_news')
    file_path = os.path.join(os.path.dirname(__file__), 'news.txt')

    with open(file_path, 'w') as f:
        if not articles:
            f.write('nothing to catch up')
        else:
            f.write(f"News from {datetime.utcnow().strftime('%Y-%m-%d')}\n")
            f.write('=' * 40 + '\n\n')
            for i, article in enumerate(articles, 1):
                source = article.get('source', {}).get('name', 'Unknown')
                title = article.get('title', 'No title')
                url = article.get('url', '')
                published = article.get('publishedAt', '')
                f.write(f"{i}. [{source}] {title}\n")
                f.write(f"   {url}\n")
                f.write(f"   Published: {published}\n\n")

    print(f"News written to {file_path}")


dag = DAG(
    'daily_news',
    start_date=datetime(2025, 1, 1),
    schedule='0 9 * * *',
    catchup=False,
)

fetch_news_task = PythonOperator(
    task_id='fetch_daily_news',
    python_callable=fetch_daily_news,
    dag=dag,
)

write_news_task = PythonOperator(
    task_id='write_news_to_file',
    python_callable=write_news_to_file,
    dag=dag,
)

fetch_news_task >> write_news_task
