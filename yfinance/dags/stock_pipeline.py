from datetime import datetime, timedelta
import sys
import pandas as pd
import pymongo
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import time

# Import conditional untuk kompatibilitas
try:
    import yfinance as yf
except ImportError:
    yf = None
    
logger = logging.getLogger(__name__)

def connect_mongo():
    try:
        client = pymongo.MongoClient(
            "mongodb://admin:password@mongo_db:27017/",
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000
        )
        client.server_info()
        db = client["stock_db"]
        logger.info("Successfully connected to MongoDB")
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

def get_tickers(**context):
    try:
        file_path = "/opt/airflow/tickers.xlsx"
        logger.info(f"Reading Excel file from: {file_path}")
        
        df = pd.read_excel(file_path)
        logger.info(f"Excel columns: {df.columns.tolist()}")
        
        if 'Ticker' not in df.columns:
            raise ValueError(f"Column 'Ticker' not found. Available columns: {df.columns.tolist()}")
        
        tickers = df['Ticker'].dropna().unique().tolist()
        logger.info(f"Found {len(tickers)} tickers: {tickers}")
        
        context['ti'].xcom_push(key='tickers', value=tickers)
        return f"Successfully extracted {len(tickers)} tickers"
        
    except Exception as e:
        logger.error(f"Error in get_tickers: {str(e)}")
        raise

def fetch_and_save_data(period_name, period_value, **context):
    if yf is None:
        raise ImportError("yfinance module is not available")
        
    ti = context['ti']
    tickers = ti.xcom_pull(task_ids='get_tickers', key='tickers')
    
    if not tickers:
        raise ValueError("No tickers received from get_tickers task")
    
    logger.info(f"Processing {len(tickers)} tickers for period {period_name}")
    success_count = 0
    error_count = 0
    
    try:
        db = connect_mongo()
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise
    
    for symbol in tickers:
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logger.info(f"Fetching data for {symbol} ({period_name}) - Attempt {retry_count + 1}")
                
                # Create Ticker object
                ticker = yf.Ticker(symbol)
                
                # Get historical data with specific method
                hist = ticker.history(period=period_value)
                
                if not hist.empty:
                    hist.reset_index(inplace=True)
                    
                    # Convert datetime objects properly
                    for col in hist.columns:
                        if 'date' in col.lower() or hist[col].dtype == 'datetime64[ns]':
                            hist[col] = hist[col].astype(str)
                    
                    data = hist.to_dict(orient='records')
                    
                    # Add metadata
                    for record in data:
                        record['_symbol'] = symbol
                        record['_period'] = period_name
                        record['_created_at'] = datetime.now().isoformat()
                    
                    collection_name = f"{symbol.lower()}_{period_name}_stock"
                    collection = db[collection_name]
                    
                    # Clear existing data
                    collection.delete_many({'_symbol': symbol, '_period': period_name})
                    
                    # Insert new data
                    collection.insert_many(data)
                    
                    logger.info(f"Successfully saved {len(data)} records for {symbol} ({period_name})")
                    success_count += 1
                    break
                    
                else:
                    logger.warning(f"No data found for {symbol} ({period_name})")
                    break
                    
            except Exception as e:
                retry_count += 1
                logger.error(f"Error fetching {symbol} ({period_name}) - Attempt {retry_count}: {str(e)}")
                
                if retry_count < max_retries:
                    time.sleep(5)
                else:
                    error_count += 1
                    logger.error(f"Failed to fetch {symbol} after {max_retries} attempts")
        
        time.sleep(1)  # Rate limiting
    
    summary = f"Period {period_name}: {success_count} successful, {error_count} failed out of {len(tickers)} tickers"
    logger.info(summary)
    return summary

def verify_database(**context):
    try:
        db = connect_mongo()
        collections = db.list_collection_names()
        logger.info(f"Collections in database: {collections}")
        
        summary = {}
        for collection_name in collections:
            count = db[collection_name].count_documents({})
            summary[collection_name] = count
            logger.info(f"Collection {collection_name}: {count} documents")
        
        return summary
    except Exception as e:
        logger.error(f"Error verifying database: {str(e)}")
        raise

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Define periods
PERIODS = {
    'daily': '1d',
    'weekly': '1wk', 
    'monthly': '1mo',
    'yearly_1y': '1y',
    'yearly_3y': '3y',
    'yearly_5y': '5y'
}

# Create DAG
dag = DAG(
    dag_id='stock_data_from_excel',
    default_args=default_args,
    description='Fetch stock data from Excel with various periods',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
)

# Tasks
get_tickers_task = PythonOperator(
    task_id='get_tickers',
    python_callable=get_tickers,
    provide_context=True,
    dag=dag
)

fetch_tasks = []
for period_name, period_value in PERIODS.items():
    task = PythonOperator(
        task_id=f'fetch_all_stocks_{period_name}',
        python_callable=fetch_and_save_data,
        op_kwargs={'period_name': period_name, 'period_value': period_value},
        provide_context=True,
        dag=dag
    )
    fetch_tasks.append(task)

verify_task = PythonOperator(
    task_id='verify_database',
    python_callable=verify_database,
    provide_context=True,
    dag=dag
)

# Set dependencies
get_tickers_task >> fetch_tasks >> verify_task