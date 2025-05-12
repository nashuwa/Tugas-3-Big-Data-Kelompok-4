import pymongo
from datetime import datetime
import json

def debug_mongodb():
    """Debug MongoDB connection and data"""
    print("=== MongoDB Debugging Tool ===\n")
    
    # Connection strings untuk test
    connection_strings = [
        "mongodb://admin:password@localhost:27017/",
        "mongodb://admin:password@mongo_db:27017/",
        "mongodb://localhost:27017/",
        "mongodb://mongo_db:27017/"
    ]
    
    client = None
    for conn_str in connection_strings:
        try:
            print(f"Trying connection: {conn_str}")
            client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
            client.server_info()
            print(f"✓ Success with: {conn_str}\n")
            break
        except Exception as e:
            print(f"✗ Failed: {str(e)}\n")
    
    if not client:
        print("✗ All connection attempts failed")
        return
    
    try:
        # List all databases
        print("=== Available Databases ===")
        databases = client.list_database_names()
        for db_name in databases:
            print(f"- {db_name}")
        print()
        
        # Check stock_db specifically
        print("=== Checking stock_db ===")
        db = client["stock_db"]
        collections = db.list_collection_names()
        print(f"Collections in stock_db: {len(collections)}")
        
        for collection_name in collections:
            collection = db[collection_name]
            count = collection.count_documents({})
            print(f"- {collection_name}: {count} documents")
            
            # Show sample document
            if count > 0:
                sample = collection.find_one()
                print(f"  Sample document keys: {list(sample.keys())}")
                print(f"  Sample document (first 3 keys):")
                for i, (key, value) in enumerate(sample.items()):
                    if i < 3:
                        print(f"    {key}: {value}")
                print()
        
        # Check authentication
        print("=== Authentication Info ===")
        stats = db.command("serverStatus")
        print(f"MongoDB version: {stats.get('version', 'unknown')}")
        print(f"Uptime: {stats.get('uptime', 0)} seconds")
        
        # Test write permission
        print("\n=== Testing Write Permission ===")
        test_collection = db["_test_write"]
        test_doc = {"test": "write", "timestamp": datetime.now()}
        result = test_collection.insert_one(test_doc)
        print(f"✓ Write successful, ID: {result.inserted_id}")
        test_collection.delete_one({"_id": result.inserted_id})
        print("✓ Delete successful")
        
        # Show recent stock data if available
        print("\n=== Recent Stock Data Sample ===")
        for collection_name in collections:
            if "_stock" in collection_name:
                collection = db[collection_name]
                recent = collection.find().sort("_created_at", -1).limit(1)
                for doc in recent:
                    print(f"Recent document from {collection_name}:")
                    print(f"  Symbol: {doc.get('_symbol', 'N/A')}")
                    print(f"  Created: {doc.get('_created_at', 'N/A')}")
                    print(f"  Period: {doc.get('_period', 'N/A')}")
                    if 'Date' in doc:
                        print(f"  Date: {doc['Date']}")
                        print(f"  Close: {doc.get('Close', 'N/A')}")
                    break
        
    except Exception as e:
        print(f"✗ Error during debugging: {str(e)}")
    finally:
        if client:
            client.close()

def test_yfinance_data():
    """Test if yfinance can fetch data"""
    print("\n=== Testing yfinance Connectivity ===")
    try:
        import yfinance as yf
        
        # Test with popular symbols
        test_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN"]
        
        for symbol in test_symbols:
            try:
                stock = yf.Ticker(symbol)
                info = stock.info
                hist = stock.history(period="1d")
                
                print(f"\n{symbol}:")
                print(f"  Info available: {'Yes' if info else 'No'}")
                print(f"  Historical data: {len(hist)} rows")
                if not hist.empty:
                    print(f"  Latest close: {hist['Close'].iloc[-1]}")
            except Exception as e:
                print(f"  Error: {str(e)}")
                
    except ImportError:
        print("✗ yfinance not installed")
    except Exception as e:
        print(f"✗ Error: {str(e)}")

if __name__ == "__main__":
    debug_mongodb()
    test_yfinance_data()