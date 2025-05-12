import pymongo
import pandas as pd
import yfinance as yf
from datetime import datetime

def test_mongo_connection():
    """Test MongoDB connection"""
    try:
        client = pymongo.MongoClient(
            "mongodb://admin:password@localhost:27017/",
            serverSelectionTimeoutMS=5000
        )
        # Test connection
        client.server_info()
        print("✓ MongoDB connection successful")
        
        # List databases
        dbs = client.list_database_names()
        print(f"✓ Available databases: {dbs}")
        
        # Test stock_db
        db = client["stock_db"]
        collections = db.list_collection_names()
        print(f"✓ Collections in stock_db: {collections}")
        
        # Test insert
        test_collection = db["test_collection"]
        test_document = {"test": "data", "timestamp": datetime.now()}
        result = test_collection.insert_one(test_document)
        print(f"✓ Test insert successful, ID: {result.inserted_id}")
        
        # Clean up
        test_collection.delete_one({"_id": result.inserted_id})
        print("✓ Test cleanup successful")
        
        return True
    except Exception as e:
        print(f"✗ MongoDB connection failed: {str(e)}")
        return False

def test_excel_read():
    """Test Excel file reading"""
    try:
        file_path = "tickers.xlsx"
        df = pd.read_excel(file_path)
        print(f"✓ Excel file read successful, shape: {df.shape}")
        print(f"✓ Columns: {df.columns.tolist()}")
        
        if 'Ticker' in df.columns:
            tickers = df['Ticker'].dropna().unique().tolist()
            print(f"✓ Found tickers: {tickers}")
        else:
            print("✗ 'Ticker' column not found")
            print(f"Available columns: {df.columns.tolist()}")
        return True
    except Exception as e:
        print(f"✗ Excel reading failed: {str(e)}")
        return False

def test_yfinance():
    """Test yfinance API"""
    try:
        # Test dengan salah satu symbol yang umum
        stock = yf.Ticker("AAPL")
        hist = stock.history(period="1d")
        
        if not hist.empty:
            print("✓ yfinance API working")
            print(f"✓ Sample data for AAPL: {len(hist)} rows")
        else:
            print("✗ yfinance returned empty data")
        return True
    except Exception as e:
        print(f"✗ yfinance test failed: {str(e)}")
        return False

def main():
    print("=== Testing Components ===")
    print()
    
    print("1. Testing Excel file...")
    test_excel_read()
    print()
    
    print("2. Testing MongoDB connection...")
    test_mongo_connection()
    print()
    
    print("3. Testing yfinance API...")
    test_yfinance()
    print()
    
    print("=== Test Complete ===")

if __name__ == "__main__":
    main()