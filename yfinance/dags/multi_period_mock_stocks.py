from datetime import datetime, timedelta
import pandas as pd
import pymongo
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import random

logger = logging.getLogger(__name__)

def get_tickers(**context):
    """Read tickers from Excel file"""
    try:
        file_path = "/opt/airflow/tickers.xlsx"
        logger.info(f"Reading Excel file from: {file_path}")
        
        df = pd.read_excel(file_path)
        logger.info(f"Excel columns: {df.columns.tolist()}")
        
        if 'Ticker' not in df.columns:
            raise ValueError(f"Column 'Ticker' not found")
        
        tickers = df['Ticker'].dropna().unique().tolist()
        logger.info(f"Found {len(tickers)} tickers")
        
        context['ti'].xcom_push(key='tickers', value=tickers)
        return f"Successfully extracted {len(tickers)} tickers"
        
    except Exception as e:
        logger.error(f"Error in get_tickers: {str(e)}")
        raise

def create_mock_data_by_period(period_name, period_value, **context):
    """Create mock data for specific period"""
    
    ti = context['ti']
    tickers = ti.xcom_pull(task_ids='get_tickers', key='tickers')
    
    if not tickers:
        raise ValueError("No tickers received from get_tickers task")
    
    logger.info(f"Creating mock data for period: {period_name}")
    
    # Connect to MongoDB
    client = pymongo.MongoClient('mongodb://admin:password@mongo_db:27017/')
    db = client['stock_db']
    
    # Determine date range based on period
    end_date = datetime.now()
    
    if period_name == 'daily':
        start_date = end_date - timedelta(days=30)
        interval = timedelta(days=1)
    elif period_name == 'weekly':
        start_date = end_date - timedelta(weeks=52)  # 1 year
        interval = timedelta(weeks=1)
    elif period_name == 'monthly':
        start_date = end_date - timedelta(days=365*3)  # 3 years
        interval = timedelta(days=30)
    elif period_name == 'yearly_1y':
        start_date = end_date - timedelta(days=365)
        interval = timedelta(days=365)
    elif period_name == 'yearly_3y':
        start_date = end_date - timedelta(days=365*3)
        interval = timedelta(days=365)
    elif period_name == 'yearly_5y':
        start_date = end_date - timedelta(days=365*5)
        interval = timedelta(days=365)
    else:
        raise ValueError(f"Unknown period: {period_name}")
    
    # Generate dates
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += interval
    
    success_count = 0
    error_count = 0
    
    for ticker in tickers:
        try:
            logger.info(f"Creating {period_name} data for {ticker}")
            
            # Generate realistic base price
            base_price = random.randint(1000, 50000)
            
            records = []
            for i, date in enumerate(dates):
                # Create price with long-term trend
                trend = i * random.uniform(-5, 10)  # Overall upward trend
                variation = random.uniform(-0.05, 0.05)  # Daily variation
                current_price = base_price + trend + (base_price * variation)
                
                # Create OHLC data
                open_price = current_price * random.uniform(0.98, 1.02)
                close_price = current_price
                high_price = max(open_price, close_price) * random.uniform(1.00, 1.03)
                low_price = min(open_price, close_price) * random.uniform(0.97, 1.00)
                
                # Scale volume based on period
                if period_name == 'daily':
                    volume = random.randint(1000000, 10000000)
                elif period_name == 'weekly':
                    volume = random.randint(5000000, 50000000)  # Week total
                elif period_name == 'monthly':
                    volume = random.randint(20000000, 200000000)  # Month total
                else:
                    volume = random.randint(50000000, 500000000)  # Yearly total
                
                record = {
                    'Date': date.strftime('%Y-%m-%d'),
                    'Open': round(open_price, 2),
                    'High': round(high_price, 2),
                    'Low': round(low_price, 2),
                    'Close': round(close_price, 2),
                    'Volume': volume,
                    '_symbol': ticker,
                    '_period': period_name,
                    '_type': 'mock_data',
                    '_created_at': datetime.now().isoformat()
                }
                
                records.append(record)
            
            # Save to MongoDB with period-specific collection
            collection_name = f"{ticker.lower()}_{period_name}_stock"
            collection = db[collection_name]
            
            # Clear existing data for this period
            collection.delete_many({'_period': period_name, '_type': 'mock_data'})
            
            # Insert new mock data
            collection.insert_many(records)
            
            success_count += 1
            logger.info(f"SUCCESS: Created {len(records)} {period_name} records for {ticker}")
            
        except Exception as e:
            error_count += 1
            logger.error(f"ERROR with {ticker}: {str(e)}")
    
    # Create summary
    summary = {
        'period': period_name,
        'total_tickers': len(tickers),
        'successful': success_count,
        'failed': error_count,
        'records_per_ticker': len(dates),
        'date_range': f'{dates[0].strftime("%Y-%m-%d")} to {dates[-1].strftime("%Y-%m-%d")}',
        'created_at': datetime.now().isoformat()
    }
    
    # Save summary
    db[f'{period_name}_summary'].insert_one(summary)
    
    client.close()
    
    result = {
        'success': True,
        'period': period_name,
        'total_tickers': len(tickers),
        'successful': success_count,
        'failed': error_count,
        'message': f"Mock data creation for {period_name}: {success_count}/{len(tickers)} successful"
    }
    
    logger.info(result['message'])
    return result

def verify_all_periods(**context):
    """Verify data for all periods"""
    
    client = pymongo.MongoClient('mongodb://admin:password@mongo_db:27017/')
    db = client['stock_db']
    
    periods = ['daily', 'weekly', 'monthly', 'yearly_1y', 'yearly_3y', 'yearly_5y']
    
    results = {}
    
    for period in periods:
        # Get all collections for this period
        collections = db.list_collection_names()
        period_collections = [col for col in collections if f'_{period}_stock' in col]
        
        total_records = 0
        for collection_name in period_collections:
            collection = db[collection_name]
            count = collection.count_documents({'_period': period, '_type': 'mock_data'})
            total_records += count
        
        results[period] = {
            'collections': len(period_collections),
            'total_records': total_records,
            'avg_records_per_ticker': round(total_records / len(period_collections), 2) if period_collections else 0
        }
        
        logger.info(f"{period}: {results[period]}")
    
    # Create verification summary
    verification = {
        'verification_type': 'all_periods',
        'periods_checked': periods,
        'results': results,
        'verified_at': datetime.now().isoformat()
    }
    
    db['all_periods_verification'].insert_one(verification)
    
    client.close()
    
    logger.info("All periods verification complete")
    return verification

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
    dag_id='multi_period_stock_data',
    default_args=default_args,
    description='Create mock stock data for multiple periods',
    schedule_interval=None, 
    max_active_runs=1,
    catchup=False,
)

# Task 1: Get tickers
get_tickers_task = PythonOperator(
    task_id='get_tickers',
    python_callable=get_tickers,
    provide_context=True,
    dag=dag
)

# Task 2: Create tasks for each period
period_tasks = []
for period_name, period_value in PERIODS.items():
    task = PythonOperator(
        task_id=f'create_{period_name}_data',
        python_callable=create_mock_data_by_period,
        op_kwargs={'period_name': period_name, 'period_value': period_value},
        provide_context=True,
        dag=dag
    )
    period_tasks.append(task)

# Task 3: Verify all periods
verify_task = PythonOperator(
    task_id='verify_all_periods',
    python_callable=verify_all_periods,
    provide_context=True,
    dag=dag
)

# Set dependencies
get_tickers_task >> period_tasks >> verify_task