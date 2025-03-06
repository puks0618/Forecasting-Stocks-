from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime
import yfinance as yf

# Constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
SYMBOLS = ['AAPL', 'NVDA']   

# Function to get Snowflake cursor
def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn().cursor()

# Task to extract data using yfinance
@task
def extract_data(symbol: str):
    """Extract data for a single symbol."""
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="180d").reset_index()
    df['Symbol'] = symbol
    return df

# Task to transform data
@task
def transform_data(hist_df):
    """Transform data for a single symbol."""
    records = []
    symbol = hist_df['Symbol'].iloc[0]
    for _, row in hist_df.iterrows():
        records.append((
            symbol,
            row['Date'].date().isoformat(),
            row['Open'],
            row['High'],
            row['Low'],
            row['Close']
        ))
    return records
# Task to load data into Snowflake
@task
def load_data(transformed_data1, transformed_data2):
    """Load all symbols' data."""
    # Combine both symbol data
    all_records = transformed_data1 + transformed_data2
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute("TRUNCATE TABLE DEV.RAW.STOCK_PRICE")
        cursor.executemany(
            """INSERT INTO DEV.RAW.STOCK_PRICE 
            (SYMBOL, "DATE", OPEN, HIGH, LOW, CLOSE)
            VALUES (%s, %s, %s, %s, %s, %s)""",
            all_records
        )
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        cursor.close()

# Define the DAG
with DAG(
    dag_id="etl_stock_price",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    # Extract tasks
    extract_aapl = extract_data.override(task_id='extract_aapl')('AAPL')
    extract_nvda = extract_data.override(task_id='extract_nvda')('NVDA')
    
    # Transform tasks
    transform_aapl = transform_data.override(task_id='transform_aapl')(extract_aapl)
    transform_nvda = transform_data.override(task_id='transform_nvda')(extract_nvda)
    
    # Load task combining both transformed datasets
    load_task = load_data(transform_aapl, transform_nvda)