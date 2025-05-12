import pymongo
import mysql.connector
import time
from datetime import datetime

print("\n=== SCRIPT MIGRASI SEMUA DATA EMITEN ===\n")

start_time = time.time()

# Connect to MongoDB
print("Connecting to MongoDB...")
try:
    mongo_client = pymongo.MongoClient('mongodb://localhost:27017/')
    mongo_db = mongo_client['stock_db']
    print("✅ MongoDB connected!")
except Exception as e:
    print(f"❌ MongoDB error: {str(e)}")
    exit(1)

# Connect to MySQL
print("\nConnecting to MySQL...")
try:
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='stock_db'
    )
    mysql_cursor = mysql_conn.cursor()
    print("✅ MySQL connected!")
except Exception as e:
    print(f"❌ MySQL error: {str(e)}")
    exit(1)

# Get all collections
collections = mongo_db.list_collection_names()
stock_collections = [col for col in collections if '_stock' in col]
print(f"\nFound {len(stock_collections)} total collections")

# Extract unique tickers
unique_tickers = set()
for col in stock_collections:
    if '_stock' in col:
        ticker = col.split('_')[0].upper()
        unique_tickers.add(ticker)
tickers = sorted(list(unique_tickers))
print(f"Found {len(tickers)} unique tickers: {', '.join(tickers[:10])}...")

# Get all periods
periods = ['daily', 'weekly', 'monthly', 'yearly_1y', 'yearly_3y', 'yearly_5y']

# Create master table for all tickers
print("\nCreating master tickers table...")
try:
    mysql_cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_tickers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(10) UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Insert all tickers
    for ticker in tickers:
        try:
            mysql_cursor.execute('INSERT IGNORE INTO stock_tickers (ticker) VALUES (%s)', (ticker,))
        except:
            pass
    
    mysql_conn.commit()
    print(f"✅ Added {len(tickers)} tickers to master table")
except Exception as e:
    print(f"❌ Error creating master table: {str(e)}")

# Stats
total_tables = 0
total_records = 0
total_errors = 0

# Process each ticker and period
for i, ticker in enumerate(tickers, 1):
    print(f"\n[{i}/{len(tickers)}] Processing ticker: {ticker}")
    
    ticker_start = time.time()
    ticker_records = 0
    
    for period in periods:
        collection_name = f"{ticker.lower()}_{period}_stock"
        table_name = f"{ticker.lower()}_{period}"
        
        # Check if collection exists
        if collection_name not in stock_collections:
            continue
            
        print(f"  - Processing {period} data...")
        
        # Create table
        try:
            mysql_cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date DATE,
                    open DECIMAL(14,4),
                    high DECIMAL(14,4),
                    low DECIMAL(14,4),
                    close DECIMAL(14,4),
                    volume BIGINT,
                    symbol VARCHAR(10),
                    INDEX idx_date (date),
                    INDEX idx_symbol (symbol)
                ) ENGINE=InnoDB
            ''')
            total_tables += 1
        except Exception as e:
            print(f"    ❌ Error creating table: {str(e)}")
            total_errors += 1
            continue
        
        # Get and insert data
        try:
            collection = mongo_db[collection_name]
            documents = list(collection.find({'_type': 'mock_data'}))
            
            if not documents:
                print(f"    ⚠️ No documents found")
                continue
                
            print(f"    📊 Found {len(documents)} records")
            
            # Insert documents
            inserted = 0
            for doc in documents:
                try:
                    # Format date
                    date_val = doc.get('Date')
                    if isinstance(date_val, str):
                        date_obj = date_val
                    elif isinstance(date_val, datetime):
                        date_obj = date_val.strftime('%Y-%m-%d')
                    else:
                        date_obj = datetime.now().strftime('%Y-%m-%d')
                    
                    # Insert record
                    mysql_cursor.execute(f'''
                        INSERT INTO `{table_name}` (date, open, high, low, close, volume, symbol)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        date_obj,
                        float(doc.get('Open', 0)),
                        float(doc.get('High', 0)),
                        float(doc.get('Low', 0)),
                        float(doc.get('Close', 0)),
                        int(doc.get('Volume', 0)),
                        ticker
                    ))
                    inserted += 1
                except Exception as e:
                    total_errors += 1
                    continue
            
            mysql_conn.commit()
            ticker_records += inserted
            total_records += inserted
            print(f"    ✅ Inserted {inserted}/{len(documents)} records")
            
        except Exception as e:
            print(f"    ❌ Error processing collection: {str(e)}")
            total_errors += 1
    
    ticker_end = time.time()
    ticker_time = ticker_end - ticker_start
    print(f"  ✅ Completed {ticker} in {ticker_time:.2f} seconds, {ticker_records} records")

# Create consolidated view
print("\nCreating consolidated view...")
try:
    mysql_cursor.execute('''
        CREATE OR REPLACE VIEW all_stocks_daily AS
        SELECT 
            date, open, high, low, close, volume, symbol
        FROM (
            SELECT 
                table_name
            FROM 
                information_schema.tables
            WHERE 
                table_name LIKE '%_daily' AND
                table_schema = 'stock_db'
        ) AS tables
        JOIN 
            (SELECT CONCAT('SELECT date, open, high, low, close, volume, symbol FROM ', 
                  GROUP_CONCAT(table_name SEPARATOR ' UNION ALL SELECT date, open, high, low, close, volume, symbol FROM ')) AS query
             FROM information_schema.tables
             WHERE table_name LIKE '%_daily' AND table_schema = 'stock_db') AS query_builder
    ''')
    print("✅ Created consolidated view")
except Exception as e:
    print(f"❌ Error creating view: {str(e)}")

# Final stats
end_time = time.time()
total_time = end_time - start_time
print("\n=== MIGRATION COMPLETE ===")
print(f"Total time: {total_time:.2f} seconds")
print(f"Total tickers: {len(tickers)}")
print(f"Total tables: {total_tables}")
print(f"Total records: {total_records}")
print(f"Total errors: {total_errors}")
print("\nCheck phpMyAdmin to see all your data!")

# Close connections
mysql_cursor.close()
mysql_conn.close()
mongo_client.close()