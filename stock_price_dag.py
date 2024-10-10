from airflow import DAG                                                                         
from airflow.decorators import task, dag                                                        
from airflow.models import Variable                                                         
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook                           
from datetime import datetime, timedelta
import requests                                                                                

default_args = {
    'owner': 'akshith',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_snowflake_conn():
    """Return a connection to Snowflake using SnowflakeHook."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

@dag(
    dag_id='stock_price_etl',
    default_args=default_args,
    start_date=datetime(2024, 10, 10), 
    catchup=False,
    schedule_interval=None,
    tags=['ETL', 'stocks']
)
def stock_price_etl_dag():

    @task
    def get_alpha_vantage_api_key():
        """Fetch Alpha Vantage API key from Airflow Variables."""
        return Variable.get('alpha_vantage_api')

    @task
    def extract_stock_data(symbol, api_key):
        """Fetch the last 90 days of stock prices for the specified symbol."""
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={api_key}'
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            result = []
            for day, values in list(data['Time Series (Daily)'].items())[:90]:
                values['date'] = day
                values['symbol'] = symbol
                result.append(values)
            return result
        else:
            raise Exception(f"Error: {response.status_code}")

    @task
    def create_table():
        """Create a table in Snowflake if it doesn't already exist."""
        conn = get_snowflake_conn() 
        cursor = conn.cursor()

        try:
            cursor.execute("USE DATABASE dev")
            cursor.execute("USE SCHEMA dev.raw_data")
            cursor.execute("""
                CREATE OR REPLACE TABLE stock_prices (
                    symbol VARCHAR(5),
                    date DATE,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume INT,
                    PRIMARY KEY (symbol, date)
                )
            """)
        finally:
            cursor.close()
            conn.close()  

    @task
    def load_data(stock_list):
        """Load stock data into Snowflake."""
        conn = get_snowflake_conn()  
        cursor = conn.cursor()

        try:
            cursor.execute("USE DATABASE dev")
            cursor.execute("USE SCHEMA dev.raw_data")

            insert_stmt = """
            MERGE INTO stock_prices AS target
            USING (
                SELECT
                    %s AS symbol,
                    %s AS date,
                    %s AS open,
                    %s AS high,
                    %s AS low,
                    %s AS close,
                    %s AS volume
            ) AS source
            ON target.symbol = source.symbol AND target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET
                    target.open = source.open,
                    target.high = source.high,
                    target.low = source.low,
                    target.close = source.close,
                    target.volume = source.volume
            WHEN NOT MATCHED THEN
                INSERT (symbol, date, open, high, low, close, volume)
                VALUES (source.symbol, source.date, source.open, source.high, source.low, source.close, source.volume)
            """
            for stock_info in stock_list:
                cursor.execute(insert_stmt, (
                    stock_info['symbol'],
                    stock_info['date'],
                    stock_info['1. open'],
                    stock_info['2. high'],
                    stock_info['3. low'],
                    stock_info['4. close'],
                    stock_info['5. volume']
                ))
        finally:
            cursor.close()
            conn.close()  

    api_key = get_alpha_vantage_api_key()
    stock_symbol = "AAPL"
    stock_data = extract_stock_data(stock_symbol, api_key)

    create_table() >> load_data(stock_data)  

stock_price_etl_dag = stock_price_etl_dag()
