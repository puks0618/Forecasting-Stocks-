from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# Hardcoded Constants
SNOWFLAKE_CONN_ID = "snowflake_conn"  # Hardcoded connection ID
FORECASTING_PERIODS = 7  # Hardcoded forecasting periods

def get_snowflake_cursor():
    """Get Snowflake cursor with error handling"""
    try:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        return hook.get_conn().cursor()
    except Exception as e:
        print(f"Snowflake connection failed: {str(e)}")
        raise

@task
def train_model():
    """Train forecasting model with transaction support"""
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("BEGIN")
        
        # Create training view
        cursor.execute("""
            CREATE OR REPLACE VIEW DEV.ADHOC.MARKET_DATA_VIEW AS
            SELECT DATE, CLOSE, SYMBOL
            FROM DEV.RAW.STOCK_PRICE
        """)
        
        # Create forecast model
        cursor.execute("""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST DEV.ANALYTICS.PREDICT_STOCK_PRICE (
                INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'DEV.ADHOC.MARKET_DATA_VIEW'),
                SERIES_COLNAME => 'SYMBOL',
                TIMESTAMP_COLNAME => 'DATE',
                TARGET_COLNAME => 'CLOSE',
                CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
            )
        """)
        
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Model training failed: {str(e)}")
        raise
    finally:
        cursor.close()

@task
def generate_forecast():
    """Generate predictions with transaction support"""
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("BEGIN")
        
        # Generate forecasts
        cursor.execute(f"""
            CALL DEV.ANALYTICS.PREDICT_STOCK_PRICE!FORECAST(
                FORECASTING_PERIODS => {FORECASTING_PERIODS},
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            )
        """)
        cursor.execute("SELECT LAST_QUERY_ID()")  # Explicitly get query ID
        forecast_query_id = cursor.fetchone()[0]
        
        # Create final table with corrected comment syntax
        cursor.execute(f"""
            CREATE OR REPLACE TABLE DEV.ANALYTICS.MARKET_DATA_FINAL AS
            SELECT 
                SYMBOL, 
                DATE, 
                CLOSE AS ACTUAL, 
                NULL AS FORECAST, 
                NULL AS LOWER_BOUND, 
                NULL AS UPPER_BOUND
            FROM DEV.RAW.STOCK_PRICE
            WHERE DATE < CURRENT_DATE()  -- Exclude future dates
            UNION ALL
            SELECT 
                REPLACE(SERIES::STRING, '"', '') AS SYMBOL, 
                TS::DATE AS DATE, 
                NULL AS ACTUAL, 
                FORECAST::FLOAT, 
                LOWER_BOUND::FLOAT, 
                UPPER_BOUND::FLOAT
            FROM TABLE(RESULT_SCAN('{forecast_query_id}'))
        """)
        
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Forecast generation failed: {str(e)}")
        raise
    finally:
        cursor.close()


with DAG(
    dag_id="ml_forecast",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    train_model() >> generate_forecast()