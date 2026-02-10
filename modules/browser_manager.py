import time
import os
import psutil
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium_stealth import stealth
import config
from modules.logger import logger

class BrowserManager:
    """
    Manages the Chrome Browser instance, including initialization,
    profile management, and lifecycle checking.
    """
    def __init__(self, chrome_profile=None):
        self.driver = None
        self.chrome_profile = chrome_profile or config.CHROME_PROFILE_NAME
        self.use_uc = getattr(config, 'USE_UC', True)
        
    def is_chrome_running_with_profile(self):
        """Check if Chrome is already running with the configured profile."""
        if not config.CHROME_PROFILE_PATH:
            return False
            
        logger.info(f"Checking if Chrome is already using profile: {config.CHROME_PROFILE_NAME}...", extra={"step_name": "BrowserManager"})
        try:
            target_path = os.path.normpath(config.CHROME_PROFILE_PATH).lower()
            for proc in psutil.process_iter(['name', 'cmdline']):
                try:
                    name = proc.info.get('name')
                    if name and 'chrome' in name.lower():
                        cmdline = proc.info.get('cmdline')
                        if cmdline:
                            for arg in cmdline:
                                if arg.lower().startswith('--user-data-dir='):
                                    profile_in_arg = os.path.normpath(arg.split('=', 1)[1]).lower()
                                    if target_path == profile_in_arg:
                                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
        except Exception as e:
            logger.warning(f"Error checking for running Chrome: {e}", extra={"step_name": "BrowserManager"}, exc_info=True)
            
        return False

    def init_driver(self):
        logger.info("Initializing Undetected Chrome...", extra={"step_name": "BrowserManager"})
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--disable-popup-blocking")
        
        # Load existing Chrome profile if configured
        if config.CHROME_PROFILE_PATH:
            if self.is_chrome_running_with_profile():
                logger.error("ALREADY RUNNING: Chrome is already using the selected profile directory. "
                           "Please CLOSE ALL Chrome windows before running this bot, or configure a dedicated profile in .env (e.g., C:/WBL-bots/bot_profile).", 
                           extra={"step_name": "BrowserManager"})
                import sys
                sys.exit(1)

            logger.info(f"Using Chrome profile: {self.chrome_profile}", extra={"step_name": "BrowserManager"})
            chrome_options.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
            chrome_options.add_argument(f"--profile-directory={self.chrome_profile}")
        
        
        # Proxy Support
        proxy_url = getattr(config, 'PROXY_URL', None)
        if proxy_url:
            logger.info(f"Using Proxy: {proxy_url}", extra={"step_name": "BrowserManager"})
            chrome_options.add_argument(f'--proxy-server={proxy_url}')
        
        # Try initializing driver
        try:
            if self.use_uc:
                # Force specific version if provided in config
                version = int(config.CHROME_VERSION) if config.CHROME_VERSION else None
                
                # use_subprocess=True helps with "cannot connect to chrome" errors on Windows
                self.driver = uc.Chrome(options=chrome_options, version_main=version, use_subprocess=True)
            else:
                raise Exception("USE_UC is False, skipping to standard Selenium...")
                
        except Exception as e:
            if self.use_uc:
                logger.warning(f"Undetected ChromeDriver failed: {e}. Falling back to standard Selenium ChromeDriver...", extra={"step_name": "BrowserManager"})
            
            try:
                import selenium.webdriver as webdriver
                from selenium.webdriver.chrome.service import Service as ChromeService
                from webdriver_manager.chrome import ChromeDriverManager
                
                # Ensure the options are compatible (convert uc options back to standard if needed)
                standard_options = webdriver.ChromeOptions()
                for arg in chrome_options.arguments:
                    standard_options.add_argument(arg)
                
                service = ChromeService(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=standard_options)
            except Exception as fallback_e:
                 logger.critical(f"Standard Selenium fallback also failed: {fallback_e}", extra={"step_name": "BrowserManager"})
                 raise e if self.use_uc else fallback_e

        try:
            # Apply selenium-stealth to further mask automation signals
            stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            
            logger.info("Chrome ready!", extra={"step_name": "BrowserManager"})
            return self # Return self for chaining, driver is internal
        except Exception as e:
            if "user data directory is already in use" in str(e).lower():
                logger.error("Chrome User Data directory is currently in use.", extra={"step_name": "BrowserManager"})
                import sys
                sys.exit(1)
            raise e

    def get_driver(self):
        """
        [CONTRACT] Expose driver ONLY for modules that specifically need DOM access (Scraper).
        Orchestrator should NOT use this.
        """
        return self.driver

    def navigate(self, url, retries=3, delay=5):
        """
        Safe navigation wrapper with retry logic.
        """
        if not self.driver:
            logger.error("Driver not initialized.", extra={"step_name": "BrowserManager"})
            return False

        logger.info(f"Navigating to: {url}", extra={"step_name": "BrowserManager"})
        
        for i in range(retries):
            try:
                self.driver.get(url)
                return True
            except Exception as e:
                logger.warning(f"Navigation failed ({i+1}/{retries}): {e}", extra={"step_name": "BrowserManager"})
                time.sleep(delay)
        
        logger.error(f"Failed to navigate to {url} after {retries} attempts.", extra={"step_name": "BrowserManager"})
        return False

    def wait_click(self, selector, by=By.XPATH, timeout=5, retries=3):
        """
        Robust click with wait and retry.
        """
        if not self.driver: return False
        
        for i in range(retries):
            try:
                # 1. Wait for presence
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                
                element = WebDriverWait(self.driver, timeout).until(
                    EC.element_to_be_clickable((by, selector))
                )
                
                # 2. Attempt click
                element.click()
                return True
                
            except (StaleElementReferenceException, Exception) as e:
                logger.debug(f"Click failed ({i+1}/{retries}) for {selector}: {e}", extra={"step_name": "BrowserManager"})
                time.sleep(1)
        
        logger.warning(f"Failed to click element {selector} after {retries} attempts.", extra={"step_name": "BrowserManager"})
        return False

    def safe_get_text(self, element, retries=3):
        """Safely get element text with stale element retries."""
        for i in range(retries):
            try:
                return element.text
            except StaleElementReferenceException:
                if i < retries - 1: time.sleep(0.5)
            except Exception:
                return ""
        return ""

    def safe_get_attribute(self, element, attr, retries=3):
        """Safely get element attribute with stale element retries."""
        for i in range(retries):
            try:
                val = element.get_attribute(attr)
                return val if val else ""
            except StaleElementReferenceException:
                if i < retries - 1: time.sleep(0.5)
            except Exception:
                return ""
        return ""

    def get_current_url(self):
        """Safe URL getter."""
        return self.driver.current_url if self.driver else ""
            
    def login(self, email, password):
        """Perform login if needed."""
        # Check if already logged in (Stricter check)
        # Just checking URL is insufficient as guest page can have 'feed' in parameters
        
        def is_logged_in_check():
             if "feed" not in self.get_current_url(): return False
             # Check for search bar as proxy for "logged in UI"
             try:
                 # Re-use the robust selector list if possible, or just a known simple one
                 if self.driver.find_elements(By.XPATH, "//input[contains(@class, 'search-global-typeahead__input')]"):
                     return True
                 if self.driver.find_elements(By.XPATH, "//input[contains(@placeholder, 'Search')]"):
                     return True
             except: pass
             return False

        if is_logged_in_check():
            logger.info("Already logged in (Feed & UI detected).", extra={"step_name": "BrowserManager"})
            return True

        logger.info("Logging in...", extra={"step_name": "BrowserManager"})
        if not self.driver:
            logger.error("Error: Driver not initialized!", extra={"step_name": "BrowserManager"})
            return False

        self.navigate(config.URLS['LOGIN'])
        time.sleep(2)
        
        # Check again if redirect handled it
        if "feed" in self.get_current_url():
             logger.info("Redirected to Feed. Logged in.", extra={"step_name": "BrowserManager"})
             return True

        # Helper to try list of selectors
        def send_keys_to_first_found(selectors, value, key_to_press=None):
            if isinstance(selectors, str): selectors = [selectors]
            for selector in selectors:
                try:
                    elem = self.driver.find_element(By.ID, selector)
                    elem.clear()
                    elem.send_keys(value)
                    if key_to_press:
                        time.sleep(1)
                        elem.send_keys(key_to_press)
                    return True
                except: continue
            return False

        if not send_keys_to_first_found(config.SELECTORS['login']['username'], email):
            logger.error("Could not find login username field.", extra={"step_name": "BrowserManager"})

        time.sleep(1)
        
        if not send_keys_to_first_found(config.SELECTORS['login']['password'], password, Keys.RETURN):
             logger.error("Could not find login password field.", extra={"step_name": "BrowserManager"})

        time.sleep(5)
        logger.info("Logged in!", extra={"step_name": "BrowserManager"})
        return True

    def human_scroll(self, limit_range=(800, 1200)):
        """
        Scrolls the page like a human: varying speeds, small pauses, and random distances.
        """
        if not self.driver: return

        try:
            scroll_amount = random.randint(*limit_range)
            current_pos = self.driver.execute_script("return window.pageYOffset;")
            target_pos = current_pos + scroll_amount
            
            # Break scroll into chunks
            while current_pos < target_pos:
                step = random.randint(50, 150)
                current_pos += step
                if current_pos > target_pos: current_pos = target_pos
                
                self.driver.execute_script(f"window.scrollTo(0, {current_pos});")
                
                # Tiny sleep between steps for smooth scroll effect
                time.sleep(random.uniform(0.01, 0.05))
                
                # Occasionally pause briefly
                if random.random() < 0.1:
                    time.sleep(random.uniform(0.1, 0.3))
            
            time.sleep(random.uniform(0.5, 1.5))
        except Exception as e:
            logger.debug(f"Human scroll failed: {e}", extra={"step_name": "BrowserManager"})

    def human_mouse_move(self):
        """
        Simulates random mouse movements to deter bot detection.
        Uses ActionChains to move to random coordinates or elements.
        """
        if not self.driver: return

        try:
            from selenium.webdriver.common.action_chains import ActionChains
            
            # 1. Random small offset from current position (simulated)
            # Note: Selenium doesn't easily allow "move to x,y" without an element reference 
            # in standard mode, but we can move relative to body or perform "dummy" moves.
            
            body = self.driver.find_element(By.TAG_NAME, "body")
            
            # Move to random visible elements (headers, buttons, texts)
            possible_targets = self.driver.find_elements(By.CSS_SELECTOR, "h1, h2, span, p, a")
            if possible_targets:
                target = random.choice(possible_targets[:20]) # Limit to top 20 to avoid slow finds
                if target.is_displayed():
                    ActionChains(self.driver).move_to_element(target).perform()
                    time.sleep(random.uniform(0.2, 0.7))
        except:
            pass # Fail silently, this is just enhancement

    def quit(self):
        if self.driver:
            logger.info("Closing Chrome...", extra={"step_name": "BrowserManager"})
            self.driver.quit()

