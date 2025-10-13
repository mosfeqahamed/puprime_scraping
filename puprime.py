import time
import json
import hashlib
import random
import os
import schedule
import signal
import atexit
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import requests
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Try to import undetected-chromedriver, but don't fail if not available
try:
    import undetected_chromedriver as uc
    UC_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    UC_AVAILABLE = False
    print("Note: undetected-chromedriver not available, using enhanced regular Selenium")

# Global cleanup registry for drivers
_active_drivers = set()

def _cleanup_all_drivers():
    """Cleanup all active drivers on program exit"""
    for driver_ref in list(_active_drivers):
        try:
            if hasattr(driver_ref, '_cleanup_driver'):
                driver_ref._cleanup_driver()
        except:
            pass
    _active_drivers.clear()

def _signal_handler(signum, frame):
    """Handle signals to ensure proper cleanup"""
    _cleanup_all_drivers()
    exit(0)

# Register cleanup handlers
atexit.register(_cleanup_all_drivers)
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


class MongoDBManager:
    """MongoDB connection and data management"""
    
    def __init__(self, logger, connection_string: str = None, database_name: str = "puprime_data"):
        self.logger = logger
        self.connection_string = connection_string or os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        self.database_name = database_name
        self.client: MongoClient = None
        self.db: Database = None
        self.accounts_collection: Collection = None
        self.sync_log_collection: Collection = None
        
        # Validate connection string format
        self._validate_connection_string()
    
    def _validate_connection_string(self):
        """Validate MongoDB connection string format"""
        if not self.connection_string:
            raise ValueError("MongoDB connection string is required")
        
        # Check for MongoDB Atlas format
        if self.connection_string.startswith('mongodb+srv://'):
            self.logger.log('INFO', 'Using MongoDB Atlas connection')
        elif self.connection_string.startswith('mongodb://'):
            self.logger.log('INFO', 'Using MongoDB connection')
        else:
            self.logger.log('WARNING', 'Unrecognized MongoDB connection string format')
        
        # Basic validation
        if '@' not in self.connection_string and 'mongodb+srv://' in self.connection_string:
            self.logger.log('WARNING', 'MongoDB Atlas connection string should include username:password@')
        
        self.logger.log('DEBUG', f'Connection string: {self.connection_string[:50]}...')
        
    def connect(self):
        """Connect to MongoDB"""
        try:
            # Enhanced connection options for MongoDB Atlas
            connection_options = {
                'serverSelectionTimeoutMS': 10000,  # 10 second timeout
                'connectTimeoutMS': 10000,          # 10 second connection timeout
                'socketTimeoutMS': 20000,           # 20 second socket timeout
                'retryWrites': True,                # Enable retryable writes
                'w': 'majority'                     # Write concern
            }
            
            self.client = MongoClient(self.connection_string, **connection_options)
            
            # Test the connection
            self.client.admin.command('ping')
            
            self.db = self.client[self.database_name]
            self.accounts_collection = self.db['accounts']
            self.sync_log_collection = self.db['sync_logs']
            
            # Create indexes for better performance (with error handling)
            try:
                self.accounts_collection.create_index([("account_number", 1)], unique=True)
                self.accounts_collection.create_index([("user_id", 1)])
                self.accounts_collection.create_index([("date", 1)])
                self.accounts_collection.create_index([("scraped_at", 1)])
                self.logger.log('INFO', 'Database indexes created/verified')
            except Exception as index_error:
                self.logger.log('WARNING', f'Index creation warning: {str(index_error)}')
            
            self.logger.log('INFO', f'Connected to MongoDB: {self.database_name}')
            return True
        except Exception as e:
            self.logger.log('ERROR', f'Failed to connect to MongoDB: {str(e)}')
            if 'authentication failed' in str(e).lower():
                self.logger.log('ERROR', 'Authentication failed - check username/password')
            elif 'network' in str(e).lower() or 'timeout' in str(e).lower():
                self.logger.log('ERROR', 'Network error - check connection string and IP whitelist')
            return False
    
    def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            self.logger.log('INFO', 'Disconnected from MongoDB')
    
    def insert_accounts(self, accounts: List[Dict]) -> int:
        """Insert or update accounts in MongoDB"""
        if not accounts:
            return 0
        
        inserted_count = 0
        updated_count = 0
        
        for account in accounts:
            try:
                # Add metadata
                account['scraped_at'] = datetime.now(timezone.utc)
                account['last_updated'] = datetime.now(timezone.utc)
                
                # Check if account already exists
                existing = self.accounts_collection.find_one({"account_number": account['account_number']})
                
                if existing:
                    # Update existing record
                    update_data = {k: v for k, v in account.items() if k != '_id'}
                    update_data['last_updated'] = datetime.now(timezone.utc)
                    
                    result = self.accounts_collection.update_one(
                        {"account_number": account['account_number']},
                        {"$set": update_data}
                    )
                    if result.modified_count > 0:
                        updated_count += 1
                else:
                    # Insert new record
                    result = self.accounts_collection.insert_one(account)
                    if result.inserted_id:
                        inserted_count += 1
                        
            except Exception as e:
                self.logger.log('ERROR', f'Error inserting account {account.get("account_number", "unknown")}: {str(e)}')
        
        self.logger.log('INFO', f'Database operation completed: {inserted_count} inserted, {updated_count} updated')
        return inserted_count + updated_count
    
    def get_latest_sync_time(self) -> Optional[datetime]:
        """Get the timestamp of the last successful sync"""
        try:
            latest_log = self.sync_log_collection.find_one(
                {"status": "success"},
                sort=[("sync_time", -1)]
            )
            return latest_log.get('sync_time') if latest_log else None
        except Exception as e:
            self.logger.log('ERROR', f'Error getting latest sync time: {str(e)}')
            return None
    
    def log_sync(self, status: str, records_processed: int, error_message: str = None):
        """Log sync operation"""
        try:
            log_entry = {
                'sync_time': datetime.now(timezone.utc),
                'status': status,
                'records_processed': records_processed,
                'error_message': error_message
            }
            self.sync_log_collection.insert_one(log_entry)
        except Exception as e:
            self.logger.log('ERROR', f'Error logging sync: {str(e)}')
    
    def get_account_count(self) -> int:
        """Get total number of accounts in database"""
        try:
            return self.accounts_collection.count_documents({})
        except Exception as e:
            self.logger.log('ERROR', f'Error getting account count: {str(e)}')
            return 0


class PUPrimeSeleniumScraper:
    """
    Selenium-based web scraper for PU Prime IB Portal.
    Works with both undetected-chromedriver and regular Selenium.
    """
    
    def __init__(self, logger, headless=False, use_uc=None):
        self.logger = logger
        self.base_url = 'https://myaccount.puprime.com'  # Updated to correct domain
        self.login_url = 'https://myaccount.puprime.com/login'
        self.api_base_url = 'https://ibportal.puprime.com'  # For API calls
        self.headless = headless
        self.use_uc = use_uc if use_uc is not None else UC_AVAILABLE
        self.driver = None
        self.wait = None
        self.driver_initialized = False
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup"""
        self._cleanup_driver()
        
    def _setup_driver(self):
        """Setup Chrome driver with anti-detection measures"""
        try:
            if self.use_uc and UC_AVAILABLE:
                self._setup_undetected_driver()
            else:
                self._setup_regular_driver()
                
        except Exception as e:
            self.logger.log('ERROR', f'Failed to setup driver: {str(e)}')
            # Fallback to regular driver if UC fails
            if self.use_uc:
                self.logger.log('INFO', 'Falling back to regular Selenium driver')
                self.use_uc = False
                self._setup_regular_driver()
            else:
                raise
    
    def _setup_undetected_driver(self):
        """Setup undetected Chrome driver"""
        self.logger.log('INFO', 'Setting up undetected-chromedriver')
        
        options = uc.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--window-size=1920,1080')
        
        if self.headless:
            options.add_argument('--headless=new')
            
        # Create driver with undetected-chromedriver
        self.driver = uc.Chrome(options=options, version_main=None)
        self.wait = WebDriverWait(self.driver, 20)
        self.driver_initialized = True
        
        # Register for global cleanup
        _active_drivers.add(self)
        
        self.logger.log('INFO', 'Undetected Chrome driver initialized successfully')
    
    def _setup_regular_driver(self):
        """Setup regular Chrome driver with maximum anti-detection"""
        self.logger.log('INFO', 'Setting up enhanced regular Chrome driver')
        
        options = Options()
        
        # Essential anti-detection arguments
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Additional stealth options
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-gpu-sandbox')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--start-maximized')
        
        # User agent
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Preferences to look more human
        prefs = {
            'credentials_enable_service': False,
            'profile.password_manager_enabled': False,
            'profile.default_content_setting_values.notifications': 2,
            'excludeSwitches': ['enable-logging'],
            'useAutomationExtension': False,
            'profile.default_content_settings.popups': 0,
            'profile.managed_default_content_settings.images': 1,
        }
        options.add_experimental_option('prefs', prefs)
        
        # Enable performance logging
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        if self.headless:
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
        
        # Create driver
        try:
            self.driver = webdriver.Chrome(options=options)
        except Exception as e:
            self.logger.log('WARNING', f'Chrome driver failed: {str(e)}, trying with Service')
            # Try with Service if direct creation fails
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
        
        self.wait = WebDriverWait(self.driver, 20)
        
        # Execute anti-detection scripts
        self._apply_stealth_scripts()
        self.driver_initialized = True
        
        # Register for global cleanup
        _active_drivers.add(self)
        
        self.logger.log('INFO', 'Enhanced regular Chrome driver initialized')
    
    def _apply_stealth_scripts(self):
        """Apply JavaScript to hide automation indicators"""
        stealth_js = """
        // Overwrite the navigator.webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
        
        // Overwrite the navigator.plugins property
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5]
        });
        
        // Overwrite the navigator.languages property
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });
        
        // Overwrite the chrome property
        window.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {}
        };
        
        // Overwrite the permissions property
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );
        """
        
        try:
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': stealth_js
            })
        except:
            # Fallback if CDP command fails
            self.driver.execute_script(stealth_js)
    
    def _random_delay(self, min_seconds=1, max_seconds=3):
        """Add random delay to simulate human behavior"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
        
    def _human_like_typing(self, element, text):
        """Type text with human-like delays"""
        element.clear()
        for char in text:
            element.send_keys(char)
            time.sleep(random.uniform(0.05, 0.15))
    
    def _move_to_element(self, element):
        """Move mouse to element before interacting"""
        try:
            actions = ActionChains(self.driver)
            actions.move_to_element(element).perform()
            self._random_delay(0.2, 0.5)
        except:
            pass
    
    def _dismiss_overlays(self):
        """Dismiss any page overlays that might block clicks"""
        try:
            # Common overlay selectors
            overlay_selectors = [
                "//div[@id='driver-page-overlay']",
                "//div[contains(@class, 'overlay')]",
                "//div[contains(@class, 'modal')]",
                "//div[contains(@class, 'popup')]",
                "//div[contains(@class, 'backdrop')]",
                "//*[contains(@class, 'driver-overlay')]"
            ]
            
            for selector in overlay_selectors:
                try:
                    overlays = self.driver.find_elements(By.XPATH, selector)
                    for overlay in overlays:
                        if overlay.is_displayed():
                            # Try to click outside the overlay or press Escape
                            self.driver.execute_script("arguments[0].style.display = 'none';", overlay)
                            self.logger.log('DEBUG', f'Dismissed overlay: {selector}')
                except:
                    continue
                    
            # Also try pressing Escape key
            try:
                self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
            except:
                pass
                
        except Exception as e:
            self.logger.log('DEBUG', f'Error dismissing overlays: {str(e)}')
    
    def _is_driver_valid(self):
        """Check if the driver is still valid and usable"""
        try:
            if not self.driver or not self.driver_initialized:
                return False
            # Try to get current URL to test if driver is responsive
            self.driver.current_url
            return True
        except Exception:
            return False
    
    def _cleanup_driver(self):
        """Safely cleanup the driver"""
        if not self.driver_initialized:
            return
            
        # Unregister from global cleanup
        _active_drivers.discard(self)
            
        try:
            if self.driver and self._is_driver_valid():
                # For undetected-chromedriver, we need to be more careful
                if self.use_uc and UC_AVAILABLE:
                    # Try to close all windows first
                    try:
                        for handle in self.driver.window_handles:
                            self.driver.switch_to.window(handle)
                            self.driver.close()
                    except:
                        pass
                
                # Now quit the driver
                self.driver.quit()
                self.logger.log('INFO', 'Browser closed successfully')
        except Exception as e:
            self.logger.log('WARNING', f'Error closing browser: {str(e)}')
        finally:
            # Clear references to prevent __del__ from being called
            if self.driver:
                try:
                    # For undetected-chromedriver, set service to None to prevent cleanup issues
                    if hasattr(self.driver, 'service') and self.driver.service:
                        self.driver.service = None
                    # Also try to clear the process reference
                    if hasattr(self.driver, 'service') and hasattr(self.driver.service, 'process'):
                        self.driver.service.process = None
                except:
                    pass
            self.driver = None
            self.wait = None
            self.driver_initialized = False
    
    def _wait_for_element(self, by, value, timeout=10):
        """Wait for element to be present and return it"""
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException:
            self.logger.log('WARNING', f'Element not found: {value}')
            return None
    
    def _wait_and_click(self, by, value, timeout=10):
        """Wait for element to be clickable and click it"""
        try:
            # First, try to dismiss any overlays
            self._dismiss_overlays()
            
            element = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((by, value))
            )
            self._move_to_element(element)
            
            # Try multiple click methods
            try:
                element.click()
                return True
            except Exception as click_error:
                # If regular click fails, try JavaScript click
                self.logger.log('DEBUG', f'Regular click failed, trying JavaScript click: {str(click_error)}')
                try:
                    self.driver.execute_script("arguments[0].click();", element)
                    return True
                except Exception as js_error:
                    self.logger.log('WARNING', f'JavaScript click also failed: {str(js_error)}')
                    return False
                    
        except Exception as e:
            self.logger.log('WARNING', f'Could not click element: {value} - {str(e)}')
            return False
    
    def login_and_get_session(self, email: str, password: str):
        """Login using Selenium and extract session data"""
        try:
            # Navigate directly to login page
            self.logger.log('INFO', 'Navigating to PU Prime login page')
            self.driver.get(self.login_url)
            self._random_delay(3, 5)
            
            # Take screenshot for debugging
            self.driver.save_screenshot('1_initial_page.png')
            
            # Check current URL and page content
            current_url = self.driver.current_url
            self.logger.log('DEBUG', f'Current URL: {current_url}')
            
            # If we're on logout page, navigate to login
            if 'logout' in current_url.lower():
                self.logger.log('INFO', 'On logout page, navigating to login')
                self.driver.get(self.login_url)
                self._random_delay(2, 3)
                current_url = self.driver.current_url
                self.logger.log('DEBUG', f'New URL: {current_url}')
            
            # Look for login form or button
            # Try multiple selectors (fixed XPath syntax)
            selectors_to_try = [
                (By.XPATH, "//input[@type='email']"),
                (By.XPATH, "//input[@name='email']"),
                (By.XPATH, "//input[contains(@placeholder, 'mail')]"),  # Fixed XPath
                (By.XPATH, "//input[contains(@placeholder, 'Email')]"),
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.CSS_SELECTOR, "input[type='text']"),
                (By.CSS_SELECTOR, "input#email"),
                (By.CSS_SELECTOR, "input.email"),
                (By.XPATH, "//input[@id='email']"),
                (By.XPATH, "//input[contains(@class, 'email')]"),
                (By.NAME, "email"),
                (By.ID, "email"),
            ]
            
            email_field = None
            for selector_by, selector_value in selectors_to_try:
                try:
                    email_field = self._wait_for_element(selector_by, selector_value, timeout=2)
                    if email_field:
                        self.logger.log('DEBUG', f'Found email field with selector: {selector_value}')
                        break
                except Exception as e:
                    continue  # Skip invalid selectors
            
            if not email_field:
                # Maybe we need to click a login button first
                self.logger.log('INFO', 'Email field not found, looking for login button')
                
                login_buttons = [
                    (By.XPATH, "//button[contains(text(), 'Login')]"),
                    (By.XPATH, "//button[contains(text(), 'Sign In')]"),
                    (By.XPATH, "//a[contains(text(), 'Login')]"),
                    (By.XPATH, "//a[contains(@href, 'login')]"),
                    (By.CSS_SELECTOR, "button.login"),
                    (By.CSS_SELECTOR, "a.login"),
                ]
                
                for btn_by, btn_value in login_buttons:
                    if self._wait_and_click(btn_by, btn_value, timeout=3):
                        self.logger.log('INFO', 'Clicked login button')
                        self._random_delay(2, 3)
                        break
                
                # Try to find email field again
                for selector_by, selector_value in selectors_to_try:
                    email_field = self._wait_for_element(selector_by, selector_value, timeout=3)
                    if email_field:
                        break
            
            if not email_field:
                self.logger.log('ERROR', 'Could not find email field')
                self.driver.save_screenshot('error_no_email_field.png')
                
                # Log page source for debugging
                page_source = self.driver.page_source[:1000]
                self.logger.log('DEBUG', f'Page source snippet: {page_source}')
                return None
            
            # Enter email
            self.logger.log('INFO', 'Entering email')
            self._human_like_typing(email_field, email)
            self._random_delay(0.5, 1)
            
            # Find password field
            password_selectors = [
                (By.XPATH, "//input[@type='password']"),
                (By.XPATH, "//input[@name='password']"),
                (By.XPATH, "//input[@id='password']"),
                (By.CSS_SELECTOR, "input[type='password']"),
            ]
            
            password_field = None
            for selector_by, selector_value in password_selectors:
                password_field = self._wait_for_element(selector_by, selector_value, timeout=3)
                if password_field:
                    self.logger.log('DEBUG', f'Found password field with selector: {selector_value}')
                    break
            
            if not password_field:
                self.logger.log('ERROR', 'Could not find password field')
                self.driver.save_screenshot('error_no_password_field.png')
                return None
            
            # Enter password
            self.logger.log('INFO', 'Entering password')
            self._human_like_typing(password_field, password)
            self._random_delay(0.5, 1)
            
            # Find and click submit button
            submit_selectors = [
                (By.XPATH, "//button[@type='submit']"),
                (By.XPATH, "//button[contains(text(), 'Login')]"),
                (By.XPATH, "//button[contains(text(), 'Sign In')]"),
                (By.XPATH, "//input[@type='submit']"),
                (By.CSS_SELECTOR, "button[type='submit']"),
            ]
            
            clicked = False
            for selector_by, selector_value in submit_selectors:
                if self._wait_and_click(selector_by, selector_value, timeout=3):
                    self.logger.log('INFO', 'Clicked submit button')
                    clicked = True
                    break
            
            if not clicked:
                # Try pressing Enter
                self.logger.log('INFO', 'No submit button found, pressing Enter')
                password_field.send_keys('\n')
            
            # Wait for login to complete
            self._random_delay(3, 5)
            
            # Check if login was successful
            self.driver.save_screenshot('2_after_login.png')
            
            # Check for successful login indicators
            success_indicators = [
                (By.XPATH, "//div[contains(@class, 'dashboard')]"),
                (By.XPATH, "//div[contains(@class, 'account')]"),
                (By.XPATH, "//*[contains(text(), 'Dashboard')]"),
                (By.XPATH, "//*[contains(text(), 'Account')]"),
                (By.XPATH, "//*[contains(text(), 'Logout')]"),
                (By.XPATH, "//*[contains(text(), 'Sign Out')]"),
            ]
            
            login_success = False
            for selector_by, selector_value in success_indicators:
                if self._wait_for_element(selector_by, selector_value, timeout=5):
                    login_success = True
                    self.logger.log('INFO', f'Login successful, found: {selector_value}')
                    break
            
            if login_success:
                # Extract session data
                return self._extract_session_data()
            else:
                self.logger.log('ERROR', 'Login appears to have failed')
                return None
                
        except Exception as e:
            self.logger.log('ERROR', f'Login error: {str(e)}')
            self.driver.save_screenshot('login_error.png')
            return None
    
    def _extract_session_data(self):
        """Extract cookies and session data from browser"""
        try:
            cookies = self.driver.get_cookies()
            
            # Try to get localStorage and sessionStorage
            try:
                local_storage = self.driver.execute_script("return Object.assign({}, window.localStorage);")
            except:
                local_storage = {}
            
            try:
                session_storage = self.driver.execute_script("return Object.assign({}, window.sessionStorage);")
            except:
                session_storage = {}
            
            # Extract tokens
            xtoken = None
            session_id = None
            
            # Check storage
            for storage in [local_storage, session_storage]:
                if storage:
                    for key in ['xtoken', 'token', 'access_token', 'accessToken']:
                        if key in storage:
                            xtoken = storage[key]
                            break
                    for key in ['sessionId', 'session_id', 'sid']:
                        if key in storage:
                            session_id = storage[key]
                            break
            
            # Check cookies
            for cookie in cookies:
                name = cookie.get('name', '').lower()
                if 'token' in name or 'xtoken' in name:
                    xtoken = cookie['value']
                elif 'session' in name:
                    session_id = cookie['value']
            
            self.logger.log('INFO', f'Extracted session data - Token: {bool(xtoken)}, Session: {bool(session_id)}')
            
            return {
                'cookies': cookies,
                'xtoken': xtoken,
                'session_id': session_id,
                'local_storage': local_storage,
                'session_storage': session_storage
            }
        except Exception as e:
            self.logger.log('ERROR', f'Error extracting session data: {str(e)}')
            return None
    
    def fetch_account_data_via_js(self, mt4accounts: List[str]) -> List[Dict]:
        """Fetch data by executing JavaScript API calls in browser"""
        data = []
        unique_records = {}
        
        # First navigate to the IB portal where the API endpoints are available
        self.logger.log('INFO', 'Navigating to IB Portal for API access')
        self.driver.get(self.api_base_url)
        self._random_delay(3, 5)
        
        # Wait for page to load completely
        try:
            WebDriverWait(self.driver, 10).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
        except:
            pass
        
        for mt4account in mt4accounts:
            try:
                self.logger.log('INFO', f'Fetching data for MT4: {mt4account}')
                
                # Method 1: Try synchronous XMLHttpRequest (more reliable)
                js_code = """
                var mt4 = arguments[0];
                var xhr = new XMLHttpRequest();
                xhr.open('POST', '/web-api/api/tradeaccount/getNearestOpenAccount', false);  // Synchronous
                xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
                xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
                try {
                    xhr.send('mt4account=' + mt4);
                    if (xhr.status === 200) {
                        return JSON.parse(xhr.responseText);
                    } else {
                        return {error: 'Status: ' + xhr.status};
                    }
                } catch(e) {
                    return {error: e.toString()};
                }
                """
                
                result = self.driver.execute_script(js_code, mt4account)
                
                if result and 'data' in result and result['data']:
                    self._process_result(result['data'], mt4account, unique_records)
                elif result and 'error' in result:
                    self.logger.log('WARNING', f'API error for {mt4account}: {result["error"]}')
                    
                    # Method 2: Try alternative fetch with callback
                    self.logger.log('INFO', 'Trying alternative fetch method')
                    js_alternative = """
                    var callback = arguments[arguments.length - 1];
                    var mt4 = arguments[0];
                    fetch('/web-api/api/tradeaccount/getNearestOpenAccount', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-Requested-With': 'XMLHttpRequest'
                        },
                        body: 'mt4account=' + mt4
                    })
                    .then(response => response.json())
                    .then(data => callback(data))
                    .catch(error => callback({error: error.toString()}));
                    """
                    
                    # Set script timeout
                    self.driver.set_script_timeout(10)
                    result = self.driver.execute_async_script(js_alternative, mt4account)
                    
                    if result and 'data' in result and result['data']:
                        self._process_result(result['data'], mt4account, unique_records)
                
                self._random_delay(1, 2)
                
            except Exception as e:
                self.logger.log('ERROR', f'Error fetching {mt4account}: {str(e)}')
                
                # Method 3: Try navigating directly to the endpoint
                try:
                    self.logger.log('INFO', 'Trying direct navigation method')
                    self._fetch_via_navigation(mt4account, unique_records)
                except Exception as nav_error:
                    self.logger.log('ERROR', f'Navigation method also failed: {str(nav_error)}')
        
        return list(unique_records.values())
    
    def _process_result(self, data, mt4account, unique_records):
        """Process API result data"""
        items = data if isinstance(data, list) else [data]
        
        for item in items:
            if isinstance(item, dict) and 'userId' in item and 'userName' in item:
                key = f"{item['userId']}|{mt4account}"
                if key not in unique_records:
                    name_parts = item['userName'].strip().split(' ', 1)
                    unique_records[key] = {
                        'account_id': str(item['userId']),
                        'name': item['userName'],
                        'first_name': name_parts[0] if name_parts else '',
                        'last_name': name_parts[1] if len(name_parts) > 1 else '',
                        'email': item.get('email', ''),
                        'regdate': self.ms_to_date(item.get('regdate')),
                        'section': mt4account
                    }
                    self.logger.log('INFO', f'Found: {item["userName"]} ({item["userId"]})')
    
    def _fetch_via_navigation(self, mt4account, unique_records):
        """Fetch data by navigating to specific pages"""
        # Navigate to account page if there's a specific URL pattern
        account_url = f"{self.api_base_url}/accounts/{mt4account}"
        self.driver.get(account_url)
        self._random_delay(2, 3)
        
        # Look for account information in the DOM
        selectors = [
            "//div[@class='account-info']",
            "//div[@class='user-details']",
            "//table[@class='account-table']",
            "//div[contains(@class, 'account')]",
        ]
        
        for selector in selectors:
            elements = self.driver.find_elements(By.XPATH, selector)
            for element in elements:
                text = element.text
                if text:
                    # Parse the text to extract account info
                    lines = text.split('\n')
                    if lines:
                        # This is a basic example - adjust based on actual page structure
                        name = lines[0] if lines else 'Unknown'
                        account_id = mt4account
                        
                        key = f"{account_id}|{mt4account}"
                        if key not in unique_records:
                            name_parts = name.strip().split(' ', 1)
                            unique_records[key] = {
                                'account_id': account_id,
                                'name': name,
                                'first_name': name_parts[0] if name_parts else '',
                                'last_name': name_parts[1] if len(name_parts) > 1 else '',
                                'email': '',
                                'regdate': None,
                                'section': mt4account
                            }
                            self.logger.log('INFO', f'Found via navigation: {name}')
    
    def ms_to_date(self, ms: Optional[int]) -> Optional[str]:
        """Convert milliseconds timestamp to datetime string"""
        if not ms:
            return None
        try:
            sec = int(ms / 1000)
            return datetime.fromtimestamp(sec).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    
    def scrape_puprime(self, email: str, password: str, mt4accounts_input: str) -> List[Dict]:
        """Main scraping method"""
        try:
            # Parse MT4 accounts
            mt4accounts = [acc.strip() for acc in mt4accounts_input.split(',') if acc.strip()]
            if not mt4accounts:
                raise ValueError('No valid MT4 accounts provided')
            
            self.logger.log('INFO', f'Starting scrape for {len(mt4accounts)} MT4 accounts')
            
            # Setup driver
            self._setup_driver()
            
            # Login
            session_data = self.login_and_get_session(email, password)
            if not session_data:
                raise Exception('Failed to login')
            
            # Try multiple methods to fetch data
            self.logger.log('INFO', 'Attempting to fetch data')
            
            # Method 1: Try via JavaScript/API
            data = self.fetch_account_data_via_js(mt4accounts)
            
            # Method 2: If no data, try searching in the UI
            if not data:
                self.logger.log('INFO', 'No data from API, trying UI search')
                data = self._fetch_via_ui_search(mt4accounts)
            
            # Method 3: If still no data, check if we're logged in properly
            if not data:
                self.logger.log('WARNING', 'No data found, checking session')
                # Take a screenshot to see what's happening
                self.driver.save_screenshot('no_data_debug.png')
                
                # Try to extract any visible account info
                data = self._extract_visible_accounts(mt4accounts)
            
            if not data:
                self.logger.log('ERROR', 'No data found with any method')
                raise Exception('No data scraped')
            
            self.logger.log('INFO', f'Successfully scraped {len(data)} records')
            return data
            
        except Exception as e:
            self.logger.log('ERROR', f'Scraping failed: {str(e)}')
            if self.driver:
                self.driver.save_screenshot('final_error.png')
            raise
        finally:
            self._cleanup_driver()
    
    def _fetch_via_ui_search(self, mt4accounts: List[str]) -> List[Dict]:
        """Try to search for accounts through the UI"""
        data = []
        unique_records = {}
        
        # Navigate back to main account page
        self.driver.get(self.base_url)
        self._random_delay(2, 3)
        
        for mt4account in mt4accounts:
            try:
                # Look for search box
                search_selectors = [
                    "//input[contains(@placeholder, 'Search')]",
                    "//input[contains(@placeholder, 'MT4')]",
                    "//input[contains(@placeholder, 'Account')]",
                    "//input[@type='search']",
                    "//input[@name='search']",
                ]
                
                for selector in search_selectors:
                    search_box = self._wait_for_element(By.XPATH, selector, timeout=2)
                    if search_box:
                        search_box.clear()
                        self._human_like_typing(search_box, mt4account)
                        search_box.send_keys('\n')
                        self._random_delay(2, 3)
                        break
                
                # Extract any visible account information
                self._extract_account_from_page(mt4account, unique_records)
                
            except Exception as e:
                self.logger.log('WARNING', f'UI search failed for {mt4account}: {str(e)}')
        
        return list(unique_records.values())
    
    def _extract_visible_accounts(self, mt4accounts: List[str]) -> List[Dict]:
        """Extract any visible account information from the current page"""
        data = []
        unique_records = {}
        
        # Take screenshot for debugging
        self.driver.save_screenshot('extract_accounts_page.png')
        
        # Try to find any account-related elements
        account_selectors = [
            "//div[contains(@class, 'account')]",
            "//table[contains(@class, 'account')]",
            "//div[contains(@class, 'user')]",
            "//div[contains(@class, 'customer')]",
            "//tr[contains(@class, 'account')]",
        ]
        
        for selector in account_selectors:
            elements = self.driver.find_elements(By.XPATH, selector)
            for element in elements:
                try:
                    text = element.text
                    if text and len(text) > 5:  # Skip empty or too short text
                        # Look for MT4 account numbers in the text
                        for mt4account in mt4accounts:
                            if mt4account in text:
                                # Extract account details
                                lines = text.split('\n')
                                name = "Unknown"
                                for line in lines:
                                    # Look for name patterns
                                    if len(line) > 3 and not line.isdigit():
                                        name = line
                                        break
                                
                                key = f"{mt4account}|{mt4account}"
                                if key not in unique_records:
                                    name_parts = name.strip().split(' ', 1)
                                    unique_records[key] = {
                                        'account_id': mt4account,
                                        'name': name,
                                        'first_name': name_parts[0] if name_parts else '',
                                        'last_name': name_parts[1] if len(name_parts) > 1 else '',
                                        'email': '',
                                        'regdate': None,
                                        'section': mt4account
                                    }
                                    self.logger.log('INFO', f'Extracted: {name} for MT4: {mt4account}')
                except Exception as e:
                    continue
        
        return list(unique_records.values())
    
    def _extract_account_from_page(self, mt4account, unique_records):
        """Extract account info from current page"""
        # Similar to _extract_visible_accounts but for single account
        pass
    
    def navigate_to_account_report(self):
        """Navigate to the Account Report page after login"""
        try:
            self.logger.log('INFO', 'Navigating to Account Report page')
            
            # Wait for the page to load completely
            self._random_delay(3, 5)
            
            # Look for Account Report link in the sidebar
            account_report_selectors = [
                (By.XPATH, "//a[contains(text(), 'Account Report')]"),
                (By.XPATH, "//div[contains(text(), 'Account Report')]"),
                (By.XPATH, "//span[contains(text(), 'Account Report')]"),
                (By.XPATH, "//*[contains(@class, 'nav-item') and contains(text(), 'Account Report')]"),
                (By.XPATH, "//*[contains(@href, 'ibaccounts')]"),
            ]
            
            clicked = False
            for selector_by, selector_value in account_report_selectors:
                if self._wait_and_click(selector_by, selector_value, timeout=5):
                    self.logger.log('INFO', 'Successfully clicked Account Report link')
                    clicked = True
                    break
            
            if not clicked:
                # Try direct navigation
                self.logger.log('INFO', 'Direct navigation to Account Report page')
                self.driver.get('https://ibportal.puprime.com/ibaccounts')
            
            # Wait for the page to load
            self._random_delay(3, 5)
            
            # Verify we're on the correct page
            current_url = self.driver.current_url
            if 'ibaccounts' in current_url:
                self.logger.log('INFO', f'Successfully navigated to Account Report: {current_url}')
                return True
            else:
                self.logger.log('WARNING', f'Unexpected URL after navigation: {current_url}')
                return False
                
        except Exception as e:
            self.logger.log('ERROR', f'Error navigating to Account Report: {str(e)}')
            return False
    
    def scrape_account_report_data(self) -> List[Dict]:
        """Scrape all account data from the Account Report page with pagination"""
        all_accounts = []
        
        try:
            # Navigate to account report page
            if not self.navigate_to_account_report():
                raise Exception('Failed to navigate to Account Report page')
            
            # Take screenshot for debugging
            self.driver.save_screenshot('account_report_page.png')
            
            page_num = 1
            while True:
                self.logger.log('INFO', f'Scraping page {page_num}')
                
                # Extract data from current page
                page_accounts = self._extract_accounts_from_current_page()
                if page_accounts:
                    all_accounts.extend(page_accounts)
                    self.logger.log('INFO', f'Found {len(page_accounts)} accounts on page {page_num}')
                else:
                    self.logger.log('WARNING', f'No accounts found on page {page_num}')
                
                # Check if there's a next page
                if not self._navigate_to_next_page():
                    self.logger.log('INFO', 'No more pages to scrape')
                    break
                
                page_num += 1
                self._random_delay(2, 3)  # Wait between pages
            
            self.logger.log('INFO', f'Total accounts scraped: {len(all_accounts)}')
            return all_accounts
            
        except Exception as e:
            self.logger.log('ERROR', f'Error scraping account report data: {str(e)}')
            return all_accounts
    
    def _extract_accounts_from_current_page(self) -> List[Dict]:
        """Extract account data from the current page"""
        accounts = []
        
        try:
            # Wait for the table to load
            table_selectors = [
                (By.XPATH, "//table"),
                (By.XPATH, "//div[contains(@class, 'table')]"),
                (By.XPATH, "//div[contains(@class, 'data-table')]"),
            ]
            
            table_element = None
            for selector_by, selector_value in table_selectors:
                table_element = self._wait_for_element(selector_by, selector_value, timeout=10)
                if table_element:
                    break
            
            if not table_element:
                self.logger.log('WARNING', 'No table found on current page')
                return accounts
            
            # Find all rows in the table (skip header row)
            row_selectors = [
                "//tbody/tr",
                "//tr[position()>1]",  # Skip first row (header)
                "//table//tr[position()>1]",
            ]
            
            rows = []
            for selector in row_selectors:
                rows = self.driver.find_elements(By.XPATH, selector)
                if rows:
                    break
            
            if not rows:
                self.logger.log('WARNING', 'No data rows found in table')
                return accounts
            
            self.logger.log('INFO', f'Found {len(rows)} data rows')
            
            # Extract data from each row
            for i, row in enumerate(rows):
                try:
                    account_data = self._extract_account_from_row(row, i + 1)
                    if account_data:
                        accounts.append(account_data)
                except Exception as e:
                    self.logger.log('WARNING', f'Error extracting row {i + 1}: {str(e)}')
                    continue
            
        except Exception as e:
            self.logger.log('ERROR', f'Error extracting accounts from current page: {str(e)}')
        
        return accounts
    
    def _extract_account_from_row(self, row_element, row_index: int) -> Optional[Dict]:
        """Extract account data from a single table row"""
        try:
            # Get all cells in the row
            cells = row_element.find_elements(By.TAG_NAME, "td")
            if len(cells) < 5:  # We need at least 5 columns: Date, User ID, Account Number, Name, Email
                self.logger.log('WARNING', f'Row {row_index} has insufficient columns: {len(cells)}')
                return None
            
            # Extract data from cells (based on the image structure)
            date_text = cells[0].text.strip() if len(cells) > 0 else ""
            user_id_text = cells[1].text.strip() if len(cells) > 1 else ""
            account_number_text = cells[2].text.strip() if len(cells) > 2 else ""
            name_text = cells[3].text.strip() if len(cells) > 3 else ""
            email_text = cells[4].text.strip() if len(cells) > 4 else ""
            
            # Validate required fields
            if not all([date_text, user_id_text, account_number_text, name_text, email_text]):
                self.logger.log('WARNING', f'Row {row_index} missing required data')
                return None
            
            # Parse date
            try:
                # Convert date from DD/MM/YYYY to datetime
                parsed_date = datetime.strptime(date_text, '%d/%m/%Y')
            except ValueError:
                self.logger.log('WARNING', f'Invalid date format in row {row_index}: {date_text}')
                parsed_date = None
            
            # Create account record
            account_data = {
                'date': parsed_date,
                'date_string': date_text,
                'user_id': user_id_text,
                'account_number': account_number_text,
                'name': name_text,
                'email': email_text,
                'campaign_source': cells[5].text.strip() if len(cells) > 5 else "",
                'id_status': cells[6].text.strip() if len(cells) > 6 else "",
                'poa_status': cells[7].text.strip() if len(cells) > 7 else "",
                'scraped_at': datetime.now(timezone.utc),
                'row_index': row_index
            }
            
            self.logger.log('DEBUG', f'Extracted account: {name_text} ({account_number_text})')
            return account_data
            
        except Exception as e:
            self.logger.log('ERROR', f'Error extracting row {row_index}: {str(e)}')
            return None
    
    def _navigate_to_next_page(self) -> bool:
        """Navigate to the next page if available"""
        try:
            # Look for next page button
            next_page_selectors = [
                (By.XPATH, "//button[contains(@class, 'next')]"),
                (By.XPATH, "//a[contains(@class, 'next')]"),
                (By.XPATH, "//button[contains(text(), '>')]"),
                (By.XPATH, "//a[contains(text(), '>')]"),
                (By.XPATH, "//button[contains(text(), 'Next')]"),
                (By.XPATH, "//a[contains(text(), 'Next')]"),
                (By.XPATH, "//*[contains(@aria-label, 'next')]"),
                (By.XPATH, "//*[contains(@aria-label, 'Next')]"),
            ]
            
            for selector_by, selector_value in next_page_selectors:
                next_button = self._wait_for_element(selector_by, selector_value, timeout=3)
                if next_button:
                    # Check if button is enabled/clickable
                    if next_button.is_enabled() and next_button.is_displayed():
                        # Check if it's not disabled
                        disabled = next_button.get_attribute('disabled')
                        if not disabled:
                            self._move_to_element(next_button)
                            next_button.click()
                            self._random_delay(2, 3)
                            return True
            
            # If no next button found, check if we can click on page numbers
            current_page_elements = self.driver.find_elements(By.XPATH, "//*[contains(@class, 'active') or contains(@class, 'current')]")
            if current_page_elements:
                # Try to find the next page number
                current_page_text = current_page_elements[0].text.strip()
                try:
                    current_page_num = int(current_page_text)
                    next_page_num = current_page_num + 1
                    
                    # Look for the next page number
                    next_page_element = self._wait_for_element(
                        By.XPATH, f"//*[text()='{next_page_num}']", timeout=3
                    )
                    if next_page_element and next_page_element.is_enabled():
                        self._move_to_element(next_page_element)
                        next_page_element.click()
                        self._random_delay(2, 3)
                        return True
                except ValueError:
                    pass
            
            return False
            
        except Exception as e:
            self.logger.log('ERROR', f'Error navigating to next page: {str(e)}')
            return False


class PUPrimeAccountScraper:
    """Main class that combines scraping and MongoDB operations"""
    
    def __init__(self, logger, email: str, password: str, mongodb_uri: str = None, headless: bool = False):
        self.logger = logger
        self.email = email
        self.password = password
        self.scraper = PUPrimeSeleniumScraper(logger, headless=headless)
        self.mongodb = MongoDBManager(logger, mongodb_uri)
        
    def run_full_sync(self) -> Dict:
        """Run a full sync - scrape all data and store in MongoDB"""
        try:
            self.logger.log('INFO', 'Starting full sync operation')
            
            # Connect to MongoDB
            if not self.mongodb.connect():
                raise Exception('Failed to connect to MongoDB')
            
            # Setup scraper
            self.scraper._setup_driver()
            
            # Login
            session_data = self.scraper.login_and_get_session(self.email, self.password)
            if not session_data:
                raise Exception('Failed to login')
            
            # Scrape account report data
            accounts = self.scraper.scrape_account_report_data()
            if not accounts:
                raise Exception('No account data scraped')
            
            # Store in MongoDB
            records_processed = self.mongodb.insert_accounts(accounts)
            
            # Log successful sync
            self.mongodb.log_sync('success', records_processed)
            
            result = {
                'status': 'success',
                'records_scraped': len(accounts),
                'records_processed': records_processed,
                'total_in_db': self.mongodb.get_account_count()
            }
            
            self.logger.log('INFO', f'Full sync completed: {result}')
            return result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.log('ERROR', f'Full sync failed: {error_msg}')
            
            # Log failed sync
            if hasattr(self, 'mongodb') and self.mongodb:
                self.mongodb.log_sync('failed', 0, error_msg)
            
            return {
                'status': 'failed',
                'error': error_msg,
                'records_scraped': 0,
                'records_processed': 0
            }
        finally:
            # Cleanup
            if hasattr(self.scraper, '_cleanup_driver'):
                self.scraper._cleanup_driver()
            if hasattr(self, 'mongodb') and self.mongodb:
                self.mongodb.disconnect()
    
    def run_incremental_sync(self) -> Dict:
        """Run an incremental sync - only scrape new data since last sync"""
        try:
            self.logger.log('INFO', 'Starting incremental sync operation')
            
            # Connect to MongoDB
            if not self.mongodb.connect():
                raise Exception('Failed to connect to MongoDB')
            
            # Get last sync time
            last_sync_time = self.mongodb.get_latest_sync_time()
            if not last_sync_time:
                self.logger.log('INFO', 'No previous sync found, running full sync')
                return self.run_full_sync()
            
            self.logger.log('INFO', f'Last sync was at: {last_sync_time}')
            
            # Setup scraper
            self.scraper._setup_driver()
            
            # Login
            session_data = self.scraper.login_and_get_session(self.email, self.password)
            if not session_data:
                raise Exception('Failed to login')
            
            # Scrape account report data
            all_accounts = self.scraper.scrape_account_report_data()
            if not all_accounts:
                raise Exception('No account data scraped')
            
            # Filter for new accounts (created after last sync)
            new_accounts = []
            for account in all_accounts:
                account_date = account.get('date')
                if account_date and account_date > last_sync_time:
                    new_accounts.append(account)
            
            self.logger.log('INFO', f'Found {len(new_accounts)} new accounts since last sync')
            
            # Store new accounts in MongoDB
            records_processed = 0
            if new_accounts:
                records_processed = self.mongodb.insert_accounts(new_accounts)
            
            # Log successful sync
            self.mongodb.log_sync('success', records_processed)
            
            result = {
                'status': 'success',
                'records_scraped': len(all_accounts),
                'new_records_found': len(new_accounts),
                'records_processed': records_processed,
                'total_in_db': self.mongodb.get_account_count(),
                'last_sync_time': last_sync_time.isoformat()
            }
            
            self.logger.log('INFO', f'Incremental sync completed: {result}')
            return result
            
        except Exception as e:
            error_msg = str(e)
            self.logger.log('ERROR', f'Incremental sync failed: {error_msg}')
            
            # Log failed sync
            if hasattr(self, 'mongodb') and self.mongodb:
                self.mongodb.log_sync('failed', 0, error_msg)
            
            return {
                'status': 'failed',
                'error': error_msg,
                'records_scraped': 0,
                'new_records_found': 0,
                'records_processed': 0
            }
        finally:
            # Cleanup
            if hasattr(self.scraper, '_cleanup_driver'):
                self.scraper._cleanup_driver()
            if hasattr(self, 'mongodb') and self.mongodb:
                self.mongodb.disconnect()


class ScheduledSyncManager:
    """Manages scheduled sync operations"""
    
    def __init__(self, logger, email: str, password: str, mongodb_uri: str = None, 
                 sync_interval_hours: int = 6, headless: bool = True):
        self.logger = logger
        self.email = email
        self.password = password
        self.mongodb_uri = mongodb_uri
        self.sync_interval_hours = sync_interval_hours
        self.headless = headless
        self.is_running = False
        
    def start_scheduled_sync(self):
        """Start the scheduled sync service"""
        self.logger.log('INFO', f'Starting scheduled sync service (interval: {self.sync_interval_hours} hours)')
        
        # Schedule the sync job
        schedule.every(self.sync_interval_hours).hours.do(self._run_scheduled_sync)
        
        # Run initial sync
        self.logger.log('INFO', 'Running initial sync')
        self._run_scheduled_sync()
        
        # Start the scheduler
        self.is_running = True
        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                self.logger.log('INFO', 'Scheduled sync service stopped by user')
                break
            except Exception as e:
                self.logger.log('ERROR', f'Error in scheduled sync service: {str(e)}')
                time.sleep(300)  # Wait 5 minutes before retrying
    
    def stop_scheduled_sync(self):
        """Stop the scheduled sync service"""
        self.logger.log('INFO', 'Stopping scheduled sync service')
        self.is_running = False
        schedule.clear()
    
    def _run_scheduled_sync(self):
        """Run a single sync operation"""
        try:
            self.logger.log('INFO', 'Starting scheduled sync operation')
            
            scraper = PUPrimeAccountScraper(
                self.logger, 
                self.email, 
                self.password, 
                self.mongodb_uri, 
                self.headless
            )
            
            # Run incremental sync
            result = scraper.run_incremental_sync()
            
            if result['status'] == 'success':
                self.logger.log('INFO', f'Scheduled sync completed successfully: {result}')
            else:
                self.logger.log('ERROR', f'Scheduled sync failed: {result}')
                
        except Exception as e:
            self.logger.log('ERROR', f'Error in scheduled sync: {str(e)}')


# Example usage
if __name__ == '__main__':
    import sys
    import argparse
    
    class SimpleLogger:
        def log(self, level, message, data=None):
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}] [{level}] {message}")
            if data:
                print(f"  {json.dumps(data, indent=2)}")
    
    def main():
        parser = argparse.ArgumentParser(description='PU Prime Account Scraper')
        parser.add_argument('--email', required=True, help='Login email')
        parser.add_argument('--password', required=True, help='Login password')
        parser.add_argument('--mongodb-uri', help='MongoDB connection string (default: mongodb://localhost:27017/)')
        parser.add_argument('--mode', choices=['full', 'incremental', 'scheduled'], default='full',
                          help='Sync mode: full (all data), incremental (new data only), scheduled (continuous)')
        parser.add_argument('--interval', type=int, default=6, help='Sync interval in hours (for scheduled mode)')
        parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
        
        args = parser.parse_args()
        
        logger = SimpleLogger()
        
        try:
            if args.mode == 'scheduled':
                # Start scheduled sync service
                logger.log('INFO', 'Starting scheduled sync service')
                sync_manager = ScheduledSyncManager(
                    logger=logger,
                    email=args.email,
                    password=args.password,
                    mongodb_uri=args.mongodb_uri,
                    sync_interval_hours=args.interval,
                    headless=args.headless
                )
                sync_manager.start_scheduled_sync()
                
            else:
                # Run single sync operation
                scraper = PUPrimeAccountScraper(
                    logger=logger,
                    email=args.email,
                    password=args.password,
                    mongodb_uri=args.mongodb_uri,
                    headless=args.headless
                )
                
                if args.mode == 'full':
                    result = scraper.run_full_sync()
                else:  # incremental
                    result = scraper.run_incremental_sync()
                
                if result['status'] == 'success':
                    logger.log('INFO', f"✅ Sync completed successfully!")
                    logger.log('INFO', f"Records scraped: {result['records_scraped']}")
                    logger.log('INFO', f"Records processed: {result['records_processed']}")
                    logger.log('INFO', f"Total in database: {result['total_in_db']}")
                    
                    if 'new_records_found' in result:
                        logger.log('INFO', f"New records found: {result['new_records_found']}")
                else:
                    logger.log('ERROR', f"❌ Sync failed: {result['error']}")
                    sys.exit(1)
                    
        except KeyboardInterrupt:
            logger.log('INFO', 'Operation cancelled by user')
        except Exception as e:
            logger.log('ERROR', f"❌ Error: {str(e)}")
            
            # Check if we need to install dependencies
            if "No module named" in str(e) or "ModuleNotFoundError" in str(e):
                logger.log('INFO', "\n📦 Please install required packages:")
                logger.log('INFO', "pip install -r requirements.txt")
                logger.log('INFO', "\nOr install individually:")
                logger.log('INFO', "pip install selenium pymongo schedule python-dotenv")
                logger.log('INFO', "pip install undetected-chromedriver  # Optional for better anti-detection")
            
            sys.exit(1)
    
    # For backward compatibility, also support the old usage
    if len(sys.argv) == 1:
        # Default behavior - run with hardcoded credentials in scheduled mode (every hour)
        logger = SimpleLogger()
        
        logger.log('INFO', '🚀 Starting PU Prime Scraper in scheduled mode')
        logger.log('INFO', '📅 Will scrape every 1 hour automatically')
        logger.log('INFO', '⏹️  Press Ctrl+C to stop the scheduler')
        logger.log('INFO', '')
        logger.log('INFO', 'Using hardcoded credentials for automatic operation')
        logger.log('INFO', 'For custom settings, use command line arguments:')
        logger.log('INFO', 'python puprime.py --email your@email.com --password yourpassword --mode scheduled --interval 1')
        logger.log('INFO', '')
        
        try:
            # Start scheduled sync service with 1-hour interval
            sync_manager = ScheduledSyncManager(
                logger=logger,
                email='',
                password='',
                mongodb_uri=None,  # Use default MongoDB connection
                sync_interval_hours=1,  # Every 1 hour
                headless=True  # Run in headless mode for background operation
            )
            sync_manager.start_scheduled_sync()
            
        except KeyboardInterrupt:
            logger.log('INFO', '🛑 Scheduler stopped by user')
        except Exception as e:
            logger.log('ERROR', f"❌ Error: {str(e)}")
            sys.exit(1)
    else:
        main()