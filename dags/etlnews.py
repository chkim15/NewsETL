from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json
from datetime import datetime


POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'news_api'
NEWS_API_KEY = 'b186c29213b84ba8a7f59a9ab20bd818'
# TOPIC = 'technology'
# COMPANIES = ['OpenAI','Anthropic','Stripe','Snowflake','Perplexity','Databricks','CrewAI','DataDog','Revolut','Intuit','Square','Addepar','Sapphire','Uber','Lyft','Scale AI','DoorDash']  
COMPANIES = ['openai','anthropic','stripe','snowflake','perplexity','databricks','crewai','datadog','revolut','intuit','square','addepar','sapphire','uber','lyft','scale ai','doordash','amazon','apple','nike']
# COMPANIES = ['crewai']

default_args={
    'owner': 'airflow',
    'start_date': days_ago(1)
}

## DAG
with DAG(dag_id='news_etl_pipeline',
         default_args=default_args,
         schedule_interval='0 22 * * 0',  # Schedule to run daily at 10 PM
         catchup=True) as dags:
    
    # @task()
    # def extract_top_headlines():
    #     """Extract Top Tech Headlines Daily"""
    #     http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
    #     endpoint = f'/v2/top-headlines?country=us&category=technology&apiKey={NEWS_API_KEY}'
    #     response = http_hook.run(endpoint)

    #     if response.status_code == 200:
    #         headlines_data = response.json()
    #         return {'headlines': headlines_data.get('articles', [])}
    #     else:
    #         raise Exception(f'Failed to fetch top headlines: {response.status_code}')
    
    @task()
    def extract_news_data(company):
        """Extract News data from the News API"""

        # Use HTTP Hook to get connection details from Airflow connection
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # News API endpoint
        # Example: https://newsapi.org/v2/everything?q=technology&apiKey=b186c29213b84ba8a7f59a9ab20bd818
        endpoint = f'/v2/everything?q={company}&language=en&searchIn=title&apiKey={NEWS_API_KEY}'  # Replace with your API key if not stored in connection
        
        # Make the request via the HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            news_data = response.json()
            return {'company': company, 'articles': news_data.get('articles', [])}
        else:
            raise Exception(f'Failed to fetch news data for {company}: {response.status_code}')

    @task()    
    def transform_news_data(company_news_data):
        """Transform news data for a specific company"""
        company = company_news_data['company']
        articles = company_news_data['articles']
        transformed_data = []

        for article in articles:
            if article.get('title') == '[Removed]':
                continue

            transformed_data.append({
                'company': company,
                'title': article.get('title'),
                'description': article.get('description'),
                'content': article.get('content'),
                'author': article.get('author'),
                'published_at': datetime.strptime(article['publishedAt'], "%Y-%m-%dT%H:%M:%SZ"),
                'source_name': article['source'].get('name'),
                'url': article.get('url')
            })

        return transformed_data
    
    @task()
    def load_news_data(transformed_data):
        """Load transformed news data into PostgreSQL"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_news (
                company TEXT,
                title TEXT,
                description TEXT,
                content TEXT,
                author TEXT,
                published_at TIMESTAMPTZ,
                source_name TEXT,
                url TEXT UNIQUE,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Insert each article into the table
        for article in transformed_data:
            cursor.execute("""
                INSERT INTO company_news (company, title, description, content, author, published_at, source_name, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
            """, (
                article['company'],
                article['title'],
                article['description'],
                article['content'],
                article['author'],
                article['published_at'],
                article['source_name'],
                article['url']
            ))
        conn.commit()
        cursor.close()
        conn.close()

    # DAG Workflow - ETL Pipeline
    for company in COMPANIES:
        company_news_data = extract_news_data(company)  
        transformed_data = transform_news_data(company_news_data)
        load_news_data(transformed_data)