import time
import os
import psutil
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
                            # Looking for --user-data-dir in the command line
                            if any(target_path in os.path.normpath(arg).lower() for arg in cmdline):
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
                logger.error("Chrome is already running with the selected profile.", extra={"step_name": "BrowserManager"})
                import sys
                sys.exit(1)

            logger.info(f"Using Chrome profile: {self.chrome_profile}", extra={"step_name": "BrowserManager"})
            chrome_options.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
            chrome_options.add_argument(f"--profile-directory={self.chrome_profile}")
        
        # Proxy Support
        if config.PROXY_URL:
            logger.info(f"Using Proxy: {config.PROXY_URL}", extra={"step_name": "BrowserManager"})
            chrome_options.add_argument(f'--proxy-server={config.PROXY_URL}')
        
        # undetected_chromedriver handles its own driver management
        try:
            # Force specific version if provided in config
            version = int(config.CHROME_VERSION) if config.CHROME_VERSION else None
            
            # use_subprocess=True helps with "cannot connect to chrome" errors on Windows
            self.driver = uc.Chrome(options=chrome_options, version_main=version, use_subprocess=True)
            
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

    def get_current_url(self):
        """Safe URL getter."""
        return self.driver.current_url if self.driver else ""
            
    def login(self, email, password):
        """Perform login if needed."""
        logger.info("Logging in...", extra={"step_name": "BrowserManager"})
        if not self.driver:
            logger.error("Error: Driver not initialized!", extra={"step_name": "BrowserManager"})
            return

        self.navigate("https://www.linkedin.com/login")
        time.sleep(2)
        
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

    def quit(self):
        if self.driver:
            logger.info("Closing Chrome...", extra={"step_name": "BrowserManager"})
            self.driver.quit()
