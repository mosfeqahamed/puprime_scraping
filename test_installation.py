#!/usr/bin/env python3
"""
Test script to verify all dependencies are installed correctly
"""

def test_imports():
    """Test all required imports"""
    print("Testing imports...")
    
    try:
        import selenium
        print("✅ selenium")
    except ImportError as e:
        print(f"❌ selenium: {e}")
        return False
    
    try:
        import pymongo
        print("✅ pymongo")
    except ImportError as e:
        print(f"❌ pymongo: {e}")
        return False
    
    try:
        import schedule
        print("✅ schedule")
    except ImportError as e:
        print(f"❌ schedule: {e}")
        return False
    
    try:
        from dotenv import load_dotenv
        print("✅ python-dotenv")
    except ImportError as e:
        print(f"❌ python-dotenv: {e}")
        return False
    
    try:
        import undetected_chromedriver as uc
        print("✅ undetected-chromedriver")
    except ImportError as e:
        print(f"⚠️  undetected-chromedriver: {e} (optional)")
    
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        print("✅ webdriver-manager")
    except ImportError as e:
        print(f"⚠️  webdriver-manager: {e} (optional)")
    
    return True

def test_mongodb_connection():
    """Test MongoDB connection"""
    print("\nTesting MongoDB connection...")
    
    try:
        from pymongo import MongoClient
        import os
        from dotenv import load_dotenv
        
        # Load environment variables
        load_dotenv()
        
        # Get MongoDB URI from environment or use default
        mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        
        print(f"   Connecting to: {mongodb_uri[:50]}...")
        
        client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        print("✅ MongoDB connection successful")
        
        # Test database access
        db = client.get_default_database()
        print(f"✅ Database access successful: {db.name}")
        
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("   Make sure:")
        print("   1. MongoDB Atlas cluster is running")
        print("   2. Connection string is correct in .env file")
        print("   3. Network access is configured in Atlas")
        print("   4. Username/password are correct")
        return False

def main():
    print("PU Prime Scraper - Installation Test")
    print("=" * 40)
    
    imports_ok = test_imports()
    mongodb_ok = test_mongodb_connection()
    
    print("\n" + "=" * 40)
    if imports_ok and mongodb_ok:
        print("✅ All tests passed! You're ready to run the scraper.")
        print("\nNext steps:")
        print("1. Run: python puprime.py --email your@email.com --password yourpassword --mode full")
        print("2. Or run: python puprime.py (legacy mode with hardcoded credentials)")
    else:
        print("❌ Some tests failed. Please install missing dependencies:")
        print("   pip install -r requirements.txt")
        if not mongodb_ok:
            print("   And configure MongoDB Atlas connection in .env file")

if __name__ == "__main__":
    main()
