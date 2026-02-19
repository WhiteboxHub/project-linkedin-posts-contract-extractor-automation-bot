import time
import re
import hashlib
import random
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from functools import wraps
from modules.processor import ProcessorModule
from modules.logger import logger
from modules.utils import clean_post_content
import config

def retry_on_failure(retries=3, delay=5):
    """Decorator to retry a method on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Try to get metrics tracker from self (args[0])
            metrics = None
            if args and hasattr(args[0], 'metrics'):
                metrics = args[0].metrics

            last_exception = None
            for i in range(retries):
                try:
                    result = func(*args, **kwargs)
                    if result: # If method returns True/truthy on success
                        return result
                    
                    if metrics: metrics.track_retry(func.__name__)
                    logger.warning(f"Method {func.__name__} returned False, retrying in {delay}s... ({i+1}/{retries})", extra={"step_name": func.__name__})
                except Exception as e:
                    last_exception = e
                    if metrics: metrics.track_retry(func.__name__)
                    logger.warning(f"Method {func.__name__} failed with error: {e}. Retrying in {delay}s... ({i+1}/{retries})", extra={"step_name": func.__name__}, exc_info=True)
                
                time.sleep(delay)
            
            logger.error(f"Method {func.__name__} failed after {retries} attempts.", extra={"step_name": func.__name__})
            if last_exception:
                raise last_exception
            return False
        return wrapper
    return decorator

class ScraperModule:
    def __init__(self, browser_manager, metrics=None):
        self.browser_manager = browser_manager
        self.driver = browser_manager.get_driver()
        self.processor = ProcessorModule()
        self.metrics = metrics

    def validate_selectors(self):
        """
        Verify that critical UI elements are present and identifiable.
        This provides an early fail-safe if LinkedIn's core structure has changed.
        """
        logger.info("Validating LinkedIn UI selectors...", extra={"step_name": "Initialization"})
        
        # We need to be on a LinkedIn page that has the search bar (usually home feed)
        # We need to be on a LinkedIn page that has the search bar (usually home feed)
        if "linkedin.com/feed" not in self.driver.current_url:
            if not self.browser_manager.navigate(config.URLS['FEED']):
                return False
            time.sleep(random.uniform(2.5, 4.0))

        critical_checks = [
            ("Search Input", By.XPATH, config.SELECTORS['search']['global_input']),
            ("Feed Post Container", By.XPATH, config.SELECTORS['post']['containers']),
        ]
        
        missing = []
        for name, by, selectors in critical_checks:
            found = False
            # Ensure selectors is a list
            if isinstance(selectors, str): selectors = [selectors]
            
            for selector in selectors:
                try:
                    WebDriverWait(self.driver, 5).until( # Reduced timeout for fallbacks
                        EC.presence_of_element_located((by, selector))
                    )
                    logger.info(f"{name} found (using: {selector})", extra={"step_name": "Initialization"})
                    found = True
                    break
                except: continue
            
            if not found:
                logger.error(f"{name} NOT found. Checked {len(selectors)} selector(s).", extra={"step_name": "Initialization"})
                missing.append(name)
        
        if missing:
            logger.critical(f"FATAL ERROR: The following critical UI elements were not found: {', '.join(missing)}", extra={"step_name": "Initialization"})
            return False
            
        logger.info("UI validation successful.", extra={"step_name": "Initialization"})
        return True

    def extract_post_id(self, post):
        """Extract unique post ID from LinkedIn post element."""
        try:
            # 1. Direct attribute check (standard LinkedIn)
            for attr in ['data-urn', 'data-activity-urn', 'data-id', 'componentkey']:
                val = self.browser_manager.safe_get_attribute(post, attr)
                if val: return val
                
            # 1b. Check children for componentkey
            try:
                selectors = config.SELECTORS['post']['extract_id']['urn_component']
                if isinstance(selectors, str): selectors = [selectors]
                
                for xpath in selectors:
                    try:
                        elems = post.find_elements(By.XPATH, xpath)
                        for elem in elems:
                            for attr in ['componentkey', 'data-urn', 'data-activity-urn', 'data-id']:
                                val = self.browser_manager.safe_get_attribute(elem, attr)
                                if val: return val
                    except: continue
            except: pass

            # 2. Check for the 'time' link or any link containing 'activity' or 'update'
            try:
                selectors = config.SELECTORS['post']['extract_id']['time_link']
                if isinstance(selectors, str): selectors = [selectors]
                
                for xpath in selectors:
                    try:
                        links = post.find_elements(By.XPATH, ".//a") # Check ALL links in post
                        for link in links:
                            href = self.browser_manager.safe_get_attribute(link, 'href')
                            if not href: continue
                            
                            # Standard Activity URN
                            if 'urn:li:activity:' in href:
                                match = re.search(r'urn:li:activity:(\d+)', href)
                                if match: return f"urn:li:activity:{match.group(1)}"
                            
                            # Feed Update URL format
                            if '/feed/update/urn:li:activity:' in href:
                                match = re.search(r'urn:li:activity:(\d+)', href)
                                if match: return f"urn:li:activity:{match.group(1)}"
                                
                            if '/feed/update/activity:' in href:
                                match = re.search(r'activity:(\d+)', href)
                                if match: return f"urn:li:activity:{match.group(1)}"
                                
                            if '/feed/update/' in href:
                                parts = [p for p in href.split('/') if p]
                                for i, p in enumerate(parts):
                                    if p == 'update' and i + 1 < len(parts):
                                        potential_id = parts[i+1]
                                        if potential_id.isdigit():
                                            return f"urn:li:activity:{potential_id}"
                                        elif len(potential_id) > 15: # Might be the hash format we saw
                                            return potential_id
                    except: continue
            except: pass

            try:
                selectors = config.SELECTORS['post']['extract_id']['copy_link_text']
                if isinstance(selectors, str): selectors = [selectors]
                
                for xpath in selectors:
                    try:
                        copy_link_elems = post.find_elements(By.XPATH, xpath)
                        for copy_link_elem in copy_link_elems:
                            current = copy_link_elem
                            for _ in range(4): 
                                try:
                                    current = current.find_element(By.XPATH, "./..")
                                    for attr in ['data-urn', 'data-activity-urn', 'data-id', 'componentkey', 'data-control-name', 'id']:
                                        val = self.browser_manager.safe_get_attribute(current, attr)
                                        if val and ('activity' in val or 'urn' in val or (val.replace('-', '').isalnum() and len(val) > 15)):
                                            return val
                                except: break
                    except: continue
            except: pass

           
            try:
                menu_selectors = config.SELECTORS['post']['extract_id'].get('control_menu_button', [])
                if isinstance(menu_selectors, str): menu_selectors = [menu_selectors]
                
                for sel in menu_selectors:
                    try:
                        menu_btn = post.find_element(By.XPATH, sel)
                        if menu_btn:
                            
                            try: menu_btn.click()
                            except: self.driver.execute_script("arguments[0].click();", menu_btn)
                            
                            time.sleep(0.5) 
                            
                            
                            link_selectors = config.SELECTORS['post']['extract_id']['copy_link_text']
                            for lsel in link_selectors:
                                try:
                                    copy_elem = post.find_element(By.XPATH, lsel)
                                    current = copy_elem
                                    for _ in range(4):
                                        current = current.find_element(By.XPATH, "./..")
                                        for attr in ['data-urn', 'data-activity-urn', 'data-id', 'componentkey', 'data-control-name', 'id']:
                                            val = self.browser_manager.safe_get_attribute(current, attr)
                                            if val and ('activity' in val or 'urn' in val or len(val) > 15):
                                                return val
                                except: continue
                            break 
                    except: continue
            except: pass

            
            post_html = self.browser_manager.safe_get_attribute(post, 'outerHTML')
            if post_html:
                return hashlib.md5(post_html[:500].encode()).hexdigest()
        except:
            pass
        return None

    def extract_post_url(self, post):
        """
        Attempt to extract the direct URL to the post from its child links.
        Useful if we can't determine a clean URN but can find a link.
        """
        try:
            # 1. Search for standard update links
            links = post.find_elements(By.TAG_NAME, "a")
            for link in links:
                href = link.get_attribute("href")
                if href and ("/feed/update/" in href or "/activity/" in href or "/posts/" in href):
                    # Basic cleanup
                    if '?' in href:
                        href = href.split('?')[0]
                    return href
        except:
            pass
        return ""

  


    @retry_on_failure(retries=3, delay=5)
    def search_posts(self, keyword):
        """Search for posts on LinkedIn for a given keyword using strict URL parameters."""
        logger.info(f"Searching for keyword: {keyword}", extra={"step_name": "Search"})
        if config.DRY_RUN:
            logger.info("DRY RUN ACTIVE: Searching and extracting without saving.", extra={"step_name": "Search"})
        
        
        import urllib.parse
        encoded_kw = urllib.parse.quote(keyword)
        
        # defaults
        sort_param = config.SEARCH_FILTERS['SORT_BY']['date_posted'] 
        
        # map config DATE_FILTER to URL param value
        date_param = config.SEARCH_FILTERS['DATE_POSTED'].get(config.DATE_FILTER, config.SEARCH_FILTERS['DATE_POSTED']['past-24h'])
        
        target_url = (
            f"{config.URLS['SEARCH']}"
            f"?keywords={encoded_kw}"
            f"&sortBy={sort_param}"
            f"&datePosted={date_param}"
        )
        
        if not self.browser_manager.navigate(target_url):
            return False
            
        self.browser_manager.human_mouse_move()
        time.sleep(random.uniform(4.0, 7.0))
        
        # Validate we are on the content tab
        if '/search/results/content' not in self.browser_manager.get_current_url():
             logger.warning("Redirection might have failed, attempting strict navigation again...", extra={"step_name": "Search"})
             if not self.browser_manager.navigate(target_url): 
                 return False
             time.sleep(5)
            
        return True

    @retry_on_failure(retries=3, delay=5)
    def apply_sort_filter(self):
        """Apply the Sort By filter on the search results page via UI if URL parameter failed."""
        try:
            logger.info("Applying Sort By filter via UI...", extra={"step_name": "Search"})
            
            # Click the "Sort by" button
            selectors = config.SELECTORS['search']['sort_filter']['dropdown_button']
            if isinstance(selectors, str): selectors = [selectors]
            
            sort_btn_found = False
            for selector in selectors:
                if self.browser_manager.wait_click(selector, timeout=4):
                    sort_btn_found = True
                    break
            
            if not sort_btn_found:
                logger.warning("Sort button not found.", extra={"step_name": "Search"})
                return False
                
            time.sleep(random.uniform(1.5, 3.0))

            # Select the option based on config
            sort_value = getattr(config, 'SORT_BY', 'latest').lower()
            if sort_value == 'latest':
                option_selectors = config.SELECTORS['search']['sort_filter']['option_latest']
            else:
                option_selectors = config.SELECTORS['search']['sort_filter']['option_relevance']
            
            if isinstance(option_selectors, str): option_selectors = [option_selectors]
            
            option_clicked = False
            for selector in option_selectors:
                # Try simple click first via robust waiter
                if self.browser_manager.wait_click(selector, timeout=3):
                    option_clicked = True
                    break
                
                # Fallback to JS click if standard click fails (often needed for dropdown items)
                try:
                    option_element = self.driver.find_element(By.XPATH, selector)
                    self.driver.execute_script("arguments[0].click();", option_element)
                    option_clicked = True
                    break
                except: continue
            
            if not option_clicked:
                 logger.warning(f"Sort option '{sort_value}' not found.", extra={"step_name": "Search"})

            time.sleep(1)
            
            # Click show results
            show_selectors = config.SELECTORS['search']['sort_filter']['show_results_button']
            if isinstance(show_selectors, str): show_selectors = [show_selectors]
            
            for selector in show_selectors:
                try:
                    show_results_btn = self.driver.find_element(By.XPATH, selector)
                    show_results_btn.click()
                    break
                except: continue
                
            time.sleep(random.uniform(2.5, 4.5))
            
            return True
        except Exception as e:
            logger.warning(f"Could not apply sort filter via UI: {e}", extra={"step_name": "Search"}, exc_info=True)
            return False

    def get_posts(self, processed_posts=None):
        """
        Scroll aggressively to the bottom to load ALL posts, then collect them.
        """
        processed_posts = processed_posts or set()
        logger.info("Scrolling to load all available posts...", extra={"step_name": "Collection"})
        
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        last_count = len(self._find_post_elements())
        no_growth_count = 0
        max_scrolls = 45
        
        for i in range(1, max_scrolls + 1):
            # 1. Scroll Logic - Mix of smooth and jump
            if i % 4 == 0:
                # Direct jump to push loading
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            else:
                scroll_by = random.randint(1500, 2000)
                self.browser_manager.human_scroll(limit_range=(scroll_by-200, scroll_by+200))
            
            time.sleep(random.uniform(3.0, 5.0)) # Increased wait
            
            # 1b. Scroll to last element to trigger lazy loading
            try:
                current_posts = self._find_post_elements()
                if current_posts:
                    last_p = current_posts[-1]
                    self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", last_p)
                    time.sleep(2.0)
            except: pass

            # 2. Check for "Load More" / "Show more results" buttons
            try:
                load_more_selectors = config.SELECTORS['post']['load_more_results']
                if isinstance(load_more_selectors, str): load_more_selectors = [load_more_selectors]
                
                clicked_load = False
                for selector in load_more_selectors:
                    # Increased timeout to 3s
                    if self.browser_manager.wait_click(selector, timeout=3, retries=1):
                        logger.info(f"Clicked 'Load More' (Scroll {i})...", extra={"step_name": "Collection"})
                        clicked_load = True
                        no_growth_count = 0 
                        time.sleep(6) 
                        break
                
                if not clicked_load:
                    try:
                        # More inclusive text matching
                        btns = self.driver.find_elements(By.TAG_NAME, "button")
                        for btn in btns:
                            t = btn.text.lower()
                            if btn.is_displayed() and ("results" in t or "more" in t) and ("show" in t or "see" in t):
                                self.driver.execute_script("arguments[0].click();", btn)
                                logger.info(f"Clicked generic results button (Scroll {i})", extra={"step_name": "Collection"})
                                no_growth_count = 0
                                time.sleep(6)
                                break
                    except: pass
            except: pass

            # 3. Growth Check
            current_height = self.driver.execute_script("return document.body.scrollHeight")
            current_count = len(self._find_post_elements())
            
            if current_height > last_height or current_count > last_count:
                logger.info(f"Scroll {i}: Grew (C:{current_count})", extra={"step_name": "Collection"})
                last_height = current_height
                last_count = current_count
                no_growth_count = 0
            else:
                no_growth_count += 1
                if no_growth_count >= 4:
                     # Force a small upward scroll then down to trigger state change
                     self.driver.execute_script("window.scrollBy(0, -500);")
                     time.sleep(1)
                     self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                logger.debug(f"Scroll {i}: Stagnant ({no_growth_count}/10)")

            # 4. Exit Conditions
            if no_growth_count >= 10: # Extra patient
                logger.info("Stop: No page growth detected after 10 attempts.", extra={"step_name": "Collection"})
                break
        
        # 5. Final Collection
        logger.info("Finished scrolling. collecting all loaded posts...", extra={"step_name": "Collection"})
        all_elements = self._find_post_elements()
        
        # Deduplicate and return valid new posts
        unique_posts = []
        seen_ids_locally = set()
        
        for p in all_elements:
            try:
                # Basic visibility check
                if not p.is_displayed(): continue
                
                p_id = self.extract_post_id(p)
                if p_id and p_id not in processed_posts and p_id not in seen_ids_locally:
                    unique_posts.append(p)
                    seen_ids_locally.add(p_id)
            except: continue
            
        logger.info(f"Collection complete. Found {len(unique_posts)} new unique posts.", extra={"step_name": "Collection"})
        return unique_posts

    def _find_post_elements(self):
        """Internal helper to find posts on the page."""
        post_selectors = config.SELECTORS['post']['containers']
        
        all_found = []
        for selector in post_selectors:
            try:
                found = self.driver.find_elements(By.XPATH, selector)
                if found:
                    for p in found:
                        if p.is_displayed() and p not in all_found:
                            if p.text.strip():
                                all_found.append(p)
            except: pass
            
        return all_found

    def find_post_by_id(self, target_id):
        """Re-find a specific post element on the current page by its ID/URN."""
        if not target_id: return None
        
        # Refresh current visible elements
        elements = self._find_post_elements()
        for p in elements:
            try:
                if self.extract_post_id(p) == target_id:
                    return p
            except: continue
        return None

    def extract_post_data(self, post, get_full_html=False):
        """Extract all relevant data from a single post element."""
        data = {
            'name': '', 'email': '', 'phone': '', 'post_text': '',
            'profile_url': '', 'company': '', 'location': '', 'post_url': '',
            'is_relevant': False, 'has_job': False, 'post_html': ''
        }
        
        try:
            if get_full_html:
                data['post_html'] = self.browser_manager.safe_get_attribute(post, 'outerHTML')
            # Click see more - [UPDATED] Aggressive Loop with stale protection
            try:
                more_selectors = config.SELECTORS['post']['see_more_button']
                if isinstance(more_selectors, str): more_selectors = [more_selectors]
                
                # Retry loop to catch buttons that might appear or need re-clicking
                for attempt in range(3): 
                    clicked_any = False
                    
                    # Re-find buttons on every attempt to avoid stale elements
                    for sel in more_selectors:
                        try:
                            found_btns = post.find_elements(By.XPATH, sel)
                            for btn in found_btns:
                                if btn.is_displayed():
                                    try:
                                        # Try standard click first
                                        btn.click()
                                        clicked_any = True
                                    except:
                                        # Fallback to JS click
                                        self.driver.execute_script("arguments[0].click();", btn)
                                        clicked_any = True
                                    time.sleep(0.1)
                        except: continue
                    
                    if not clicked_any:
                        break # No buttons found/clicked, stop trying
                    
                    time.sleep(0.5) # Wait for expansion
            except Exception as e:
                logger.debug(f"Error clicking see more: {e}", extra={"step_name": "Post Extraction"})
            
            # Post Text - [UPDATED] Prioritize extraction and saving FIRST
            # User Request: "I want to just get post text first, then save it fo a file with post_id"
            try:
                text_elem = None
                text_selectors = config.SELECTORS['post']['content_text']
                
                # [NEW] Prioritized Loop
                for selector in text_selectors:
                    try:
                        text_elem = post.find_element(By.XPATH, selector)
                        if text_elem and self.browser_manager.safe_get_text(text_elem): 
                            break
                    except: continue

                if text_elem:
                    text = self.browser_manager.safe_get_text(text_elem)
                    cleaned = clean_post_content(text)
                    data['post_text'] = cleaned.encode('ascii', 'ignore').decode('ascii')
                else: 
                     # Fallback to old catch-all text
                     # raw_all_text = self.browser_manager.safe_get_text(post)
                     pass

            except Exception as e:
                logger.error(f"Text Extraction Failed: {e}", extra={"step_name": "Post Extraction"})
            
            # [NEW] Immediate File Save
            # Get ID first to use filename
            post_id = self.extract_post_id(post)
            if post_id and data['post_text']:
                try:
                    import os
                    save_dir = "saved_posts_raw"
                    if not os.path.exists(save_dir):
                        os.makedirs(save_dir)
                    
                    filename = os.path.join(save_dir, f"{post_id.replace(':', '_')}.txt")
                    with open(filename, "w", encoding='utf-8') as f:
                        f.write(data['post_text'])
                    logger.info(f"Saved raw post text to {filename}", extra={"step_name": "Post Extraction"})
                except Exception as save_err:
                    logger.error(f"Failed to save raw text: {save_err}", extra={"step_name": "Post Extraction"})

            
            # [OLD LOGIC COMMENTED OUT AS REQUESTED]
            # Name extraction etc. - we still do this but user said "extract all contacts later"
            # leaving it active for now so the rest of the bot doesn't crash, but commented logic logic elsewhere
            
            # Name
            try:
                name_selectors = config.SELECTORS['post']['author_name']
                for selector in name_selectors:
                    try:
                        name_elem = post.find_element(By.XPATH, selector)
                        name = self.browser_manager.safe_get_text(name_elem)
                        if name and 0 < len(name) < 100:
                            data['name'] = name
                            break
                    except: continue
                # Final fallback for name: check the first few words of the post text
                if not data['name']:
                    try:
                        raw_text = self.browser_manager.safe_get_text(post).split('\n')[0].strip()
                        if raw_text and len(raw_text) < 50:
                            data['name'] = raw_text
                    except: pass
            except: pass

            # Author Headline (Headline/Company often here)
            try:
                headline_selectors = config.SELECTORS['post']['author_headline']
                for selector in headline_selectors:
                    try:
                        headline_elem = post.find_element(By.XPATH, selector)
                        headline = self.browser_manager.safe_get_text(headline_elem)
                        if headline:
                            data['company'] = headline # We use company field for headline in post data
                            break
                    except: continue
            except: pass
            
            # [REMOVED - Text extraction was here, moved up]
            
            # Skip technical "posts" that are just long numeric IDs
            if data['post_text'] and data['post_text'].strip().isdigit() and len(data['post_text'].strip()) > 8:
                logger.debug(f"Skipping technical/numeric post container: {data['post_text'][:20]}...")
                return data

            is_job, job_details = self.processor.classify_job_post(data['post_text'])
            data['is_relevant'] = is_job
            data['has_job'] = is_job
            
            if data['post_text']:
                data['email'] = self.processor.extract_email(data['post_text'])
                data['phone'] = self.processor.extract_phone(data['post_text'])
            
            # Final Name cleanup: discard purely numeric names
            if data['name'] and data['name'].strip().isdigit():
                data['name'] = ""

            # Profile URL
            try:
                link_selectors = config.SELECTORS['post']['profile_link']
                for selector in link_selectors:
                    try:
                        link = post.find_element(By.XPATH, selector)
                        url = self.browser_manager.safe_get_attribute(link, 'href')
                        if url and '/in/' in url:
                            data['profile_url'] = url.split('?')[0]
                            break
                    except: continue
            except: pass
        except Exception as e:
            # Catch-all for unexpected errors during extraction
            pass 
        
        # [NEW] Ensure post_url is populated if we have an ID
        if not data.get('post_url') and post_id:
            # Construct standard LinkedIn post URL if it looks like a URN or valid ID
            if 'urn:li:activity:' in post_id:
                data['post_url'] = f"https://www.linkedin.com/feed/update/{post_id}"
            elif post_id.isdigit():
                 data['post_url'] = f"https://www.linkedin.com/feed/update/urn:li:activity:{post_id}"
            elif len(post_id) > 15: # GUID or hash
                 data['post_url'] = f"https://www.linkedin.com/feed/update/{post_id}"

        return data

    # [REMOVED] extract_full_profile_data - strictly feed-only now.
