from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(
    title="PU Prime Data API",
    description="API to fetch all scraped PU Prime account data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# MongoDB connection
def get_database():
    """Get MongoDB connection using existing settings"""
    connection_string = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    database_name = os.getenv('DATABASE_NAME', 'puprime_data')
    
    try:
        # Use same connection options as your existing scraper
        connection_options = {
            'serverSelectionTimeoutMS': 10000,
            'connectTimeoutMS': 10000,
            'socketTimeoutMS': 20000,
            'retryWrites': True,
            'w': 'majority'
        }
        
        client = MongoClient(connection_string, **connection_options)
        client.admin.command('ping')  # Test connection
        db = client[database_name]
        return db
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/")
async def root():
    """API information"""
    return {
        "message": "PU Prime Data API", 
        "version": "1.0.0",
        "description": "API to fetch scraped account data from PU Prime portal",
        "endpoints": {
            "/accounts": "GET - Fetch all account data",
            "/health": "GET - Health check",
            "/stats": "GET - Statistics",
            "/docs": "GET - Interactive API documentation"
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/accounts")
async def get_all_accounts():
    """
    Fetch all account data from MongoDB
    
    Returns:
        JSON response with all scraped account records from the database
    """
    try:
        db = get_database()
        collection = db['accounts']
        
        # Get all accounts, sorted by most recent first
        cursor = collection.find({}).sort("scraped_at", -1)
        
        accounts = []
        for doc in cursor:
            # Convert ObjectId to string for JSON serialization
            if '_id' in doc:
                doc["_id"] = str(doc["_id"])
            accounts.append(doc)
        
        return {
            "status": "success",
            "total_records": len(accounts),
            "data": accounts,
            "timestamp": datetime.now().isoformat(),
            "database": "puprime_data",
            "collection": "accounts"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error fetching accounts: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    
    Returns:
        Database connection status and basic statistics
    """
    try:
        db = get_database()
        collection = db['accounts']
        
        # Get basic stats
        total_accounts = collection.count_documents({})
        
        # Get latest sync info
        sync_collection = db['sync_logs']
        latest_sync = sync_collection.find_one(
            {"status": "success"},
            sort=[("sync_time", -1)]
        )
        
        return {
            "status": "healthy",
            "database_connected": True,
            "total_accounts": total_accounts,
            "latest_sync": {
                "time": latest_sync.get("sync_time").isoformat() if latest_sync and latest_sync.get("sync_time") else None,
                "records_processed": latest_sync.get("records_processed") if latest_sync else 0
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "database_connected": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/stats")
async def get_stats():
    """
    Get basic statistics about the scraped data
    
    Returns:
        Statistics about accounts and sync operations
    """
    try:
        db = get_database()
        accounts_collection = db['accounts']
        sync_collection = db['sync_logs']
        
        # Account statistics
        total_accounts = accounts_collection.count_documents({})
        
        # Get accounts by date ranges
        now = datetime.now()
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_ago = today.replace(day=now.day-7) if now.day > 7 else today.replace(month=now.month-1, day=28)
        month_ago = today.replace(month=now.month-1) if now.month > 1 else today.replace(year=now.year-1, month=12)
        
        accounts_today = accounts_collection.count_documents({"date": {"$gte": today}})
        accounts_this_week = accounts_collection.count_documents({"date": {"$gte": week_ago}})
        accounts_this_month = accounts_collection.count_documents({"date": {"$gte": month_ago}})
        
        # Sync statistics
        total_syncs = sync_collection.count_documents({})
        successful_syncs = sync_collection.count_documents({"status": "success"})
        failed_syncs = sync_collection.count_documents({"status": "failed"})
        
        return {
            "status": "success",
            "account_stats": {
                "total_accounts": total_accounts,
                "accounts_today": accounts_today,
                "accounts_this_week": accounts_this_week,
                "accounts_this_month": accounts_this_month
            },
            "sync_stats": {
                "total_syncs": total_syncs,
                "successful_syncs": successful_syncs,
                "failed_syncs": failed_syncs,
                "success_rate": round((successful_syncs / total_syncs * 100), 2) if total_syncs > 0 else 0
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error fetching statistics: {str(e)}"
        )

# Error handler for unhandled exceptions
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return {
        "status": "error",
        "message": f"Internal server error: {str(exc)}",
        "timestamp": datetime.now().isoformat()
    }