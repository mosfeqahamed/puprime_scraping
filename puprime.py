import time
import json
import hashlib
import random
from datetime import datetime
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import requests

# Try to import undetected-chromedriver, but don't fail if not available
try:
    import undetected_chromedriver as uc
    UC_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    UC_AVAILABLE = False
    print("Note: undetected-chromedriver not available, using enhanced regular Selenium")


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
            element = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((by, value))
            )
            self._move_to_element(element)
            element.click()
            return True
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
            if self.driver:
                self.driver.quit()
                self.logger.log('INFO', 'Browser closed')
    
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


# Example usage
if __name__ == '__main__':
    import sys
    
    class SimpleLogger:
        def log(self, level, message, data=None):
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}] [{level}] {message}")
            if data:
                print(f"  {json.dumps(data, indent=2)}")
    
    logger = SimpleLogger()
    
    # Create scraper (will auto-detect if UC is available)
    scraper = PUPrimeSeleniumScraper(logger, headless=False)
    
    try:
        results = scraper.scrape_puprime(
            email='',
            password='',
            mt4accounts_input=''
        )
        
        print(f"\n✅ Successfully scraped {len(results)} records:")
        for record in results:
            print(json.dumps(record, indent=2))
            
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        
        # Check if we need to install dependencies
        if "No module named" in str(e) or "ModuleNotFoundError" in str(e):
            print("\n📦 Please install required packages:")
            print("pip install selenium")
            print("pip install webdriver-manager")
            print("pip install setuptools")  # For distutils
            print("\nOptional (for better anti-detection):")
            print("pip install undetected-chromedriver")
        
        sys.exit(1)