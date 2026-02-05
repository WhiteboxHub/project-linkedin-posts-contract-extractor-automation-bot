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
import config

def retry_on_failure(retries=3, delay=5):
    """Decorator to retry a method on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(retries):
                try:
                    result = func(*args, **kwargs)
                    if result: # If method returns True/truthy on success
                        return result
                    logger.warning(f"Method {func.__name__} returned False, retrying in {delay}s... ({i+1}/{retries})", extra={"step_name": func.__name__})
                except Exception as e:
                    last_exception = e
                    logger.warning(f"Method {func.__name__} failed with error: {e}. Retrying in {delay}s... ({i+1}/{retries})", extra={"step_name": func.__name__}, exc_info=True)
                
                time.sleep(delay)
                # Optional: Refresh page or handle "Something went wrong" here if needed
            
            logger.error(f"Method {func.__name__} failed after {retries} attempts.", extra={"step_name": func.__name__})
            if last_exception:
                raise last_exception
            return False
        return wrapper
    return decorator

class ScraperModule:
    def __init__(self, browser_manager):
        self.browser_manager = browser_manager
        self.driver = browser_manager.get_driver()
        self.processor = ProcessorModule()

    def validate_selectors(self):
        """
        Verify that critical UI elements are present and identifiable.
        This provides an early fail-safe if LinkedIn's core structure has changed.
        """
        logger.info("Validating LinkedIn UI selectors...", extra={"step_name": "Initialization"})
        
        # We need to be on a LinkedIn page that has the search bar (usually home feed)
        # We need to be on a LinkedIn page that has the search bar (usually home feed)
        if "linkedin.com/feed" not in self.driver.current_url:
            if not self.browser_manager.navigate("https://www.linkedin.com/feed/"):
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
                val = post.get_attribute(attr)
                if val: return val
                
            # 1b. Check children for componentkey
            # 1b. Check children for componentkey
            try:
                selectors = config.SELECTORS['post']['extract_id']['urn_component']
                if isinstance(selectors, str): selectors = [selectors]
                
                for xpath in selectors:
                    try:
                        elem = post.find_element(By.XPATH, xpath)
                        for attr in ['componentkey', 'data-urn', 'data-activity-urn']:
                            val = elem.get_attribute(attr)
                            if val: return val
                    except: continue
            except: pass

            # 2. Check for the 'time' link which often contains the URN
            try:
                selectors = config.SELECTORS['post']['extract_id']['time_link']
                if isinstance(selectors, str): selectors = [selectors]
                
                for xpath in selectors:
                    try:
                        time_links = post.find_elements(By.XPATH, xpath)
                        for link in time_links:
                            href = link.get_attribute('href')
                            if 'urn:li:activity:' in href:
                                match = re.search(r'urn:li:activity:(\d+)', href)
                                if match: return f"urn:li:activity:{match.group(1)}"
                            elif '/feed/update/' in href: # Fallback for non-urn format if any
                                return href.split('/')[-2]
                    except: continue
            except: pass

            try:
                selectors = config.SELECTORS['post']['extract_id']['copy_link_text']
                if isinstance(selectors, str): selectors = [selectors]
                
                for xpath in selectors:
                    try:
                        copy_link_elem = post.find_element(By.XPATH, xpath)
                        if copy_link_elem:
                            parent = copy_link_elem.find_element(By.XPATH, "./..")
                            val = parent.get_attribute('data-control-name') or parent.get_attribute('id')
                            if val and 'activity' in val: return val
                    except: continue
            except: pass

            # 4. Generate hash if truly nothing found
            post_html = post.get_attribute('outerHTML')
            if post_html:
                return hashlib.md5(post_html[:500].encode()).hexdigest()
        except:
            pass
        return None

    def clean_post_text(self, text):
        """Clean post text by removing hashtags, '…more', and UI elements."""
        if not text:
            return ""
            
        text = text.replace("…more", "").replace("...more", "")
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            if "Like Comment Share" in line or "Comment" == line.strip() or "Share" == line.strip():
                continue
            cleaned_lines.append(line)
        text = "\n".join(cleaned_lines)
        text = re.sub(r'#\w+', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        
        return text.strip()

    @retry_on_failure(retries=3, delay=5)
    def search_posts(self, keyword):
        """Search for posts on LinkedIn for a given keyword."""
        logger.info(f"Searching for keyword: {keyword}", extra={"step_name": "Search"})
        
        if 'linkedin.com' not in self.driver.current_url:
            if not self.browser_manager.navigate("https://www.linkedin.com/feed/"):
                return False
            time.sleep(random.uniform(2.5, 4.5))
        
        try:
            selectors = config.SELECTORS['search']['global_input']
            if isinstance(selectors, str): selectors = [selectors]
            
            search_box = None
            for selector in selectors:
                try:
                    search_box = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                    break
                except: continue
                
            if not search_box:
                logger.error("Could not find search box.", extra={"step_name": "Search"})
                return False

            search_box.click() 
            search_box.clear()
            time.sleep(1)
            
            for char in keyword:
                search_box.send_keys(char)
                time.sleep(0.05)
            
            time.sleep(random.uniform(2.5, 5.0))
            search_box.send_keys(Keys.RETURN)
            time.sleep(random.uniform(4.0, 6.5))
            
            current_url = self.driver.current_url
            if '/search/results/all' in current_url or '/search/results/people' in current_url:
                logger.info("Switching to 'Posts' tab...", extra={"step_name": "Search"})
                posts_url = current_url.replace('/search/results/all', '/search/results/content').replace('/search/results/people', '/search/results/content')
                
                sort_val = getattr(config, 'SORT_BY', 'latest').lower()
                if 'sortBy' not in posts_url:
                    sort_param = '%5B%22date_posted%22%5D' if sort_val == 'latest' else '%5B%22relevance%22%5D'
                    sep = '&' if '?' in posts_url else '?'
                    posts_url += f"{sep}sortBy={sort_param}"
                
                if not self.browser_manager.navigate(posts_url):
                    return False
                time.sleep(random.uniform(4.0, 7.0))
            
            if '/search/results/content' not in self.driver.current_url:
                try:
                    # Use robust click for tab switching
                    self.browser_manager.wait_click(config.SELECTORS['search']['posts_tab_button'], timeout=5)
                    time.sleep(5)
                except: pass
            
            # Final verification: Ensure sort is applied (UI fallback)
            if 'sortBy' not in self.driver.current_url:
                if not self.apply_sort_filter():
                    logger.critical("Failed to apply sort filter after 3 attempts. Moving to next keyword.", extra={"step_name": "Search"})
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error searching: {e}", extra={"step_name": "Search"}, exc_info=True)
            return False

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
        """Scan and collect post elements from the search result page."""
        processed_posts = processed_posts or set()
        logger.info("Scanning for posts...", extra={"step_name": "Collection"})
        last_total = 0
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        no_growth_count = 0
        height_stagnation_count = 0
        
        # Maintain a map of p_id -> element to ensure uniqueness and skip already processed
        found_new_posts = {} 
        
        for i in range(1, 21): # Limit to 20 scrolls per keyword
            current_elements = self._find_post_elements()
            total_visible = len(current_elements)
            current_height = self.driver.execute_script("return document.body.scrollHeight")
            
            new_this_scroll = 0
            for p in current_elements:
                try:
                    p_id = self.extract_post_id(p)
                    if p_id and p_id not in processed_posts:
                        if p_id not in found_new_posts:
                            found_new_posts[p_id] = p
                            new_this_scroll += 1
                except StaleElementReferenceException:
                    continue
            
            new_total = len(found_new_posts)
            
            # Growth detection logic: Check both element count and page height
            has_element_growth = total_visible > last_total
            has_height_growth = current_height > last_height
            
            if has_element_growth or has_height_growth:
                growth_msg = []
                if has_element_growth: growth_msg.append(f"posts {last_total}->{total_visible}")
                if has_height_growth: growth_msg.append(f"height {last_height}->{current_height}")
                
                logger.info(f"Scroll {i}: Growth detected ({', '.join(growth_msg)}). Found {new_total} new posts so far.", extra={"step_name": "Collection"})
                last_total = total_visible
                last_height = current_height
                no_growth_count = 0
                height_stagnation_count = 0 # Reset on any growth
            else:
                no_growth_count += 1
                height_stagnation_count += 1
                if no_growth_count % 2 == 0:
                    logger.debug(f"Scroll {i}: No growth detected (Height stagnant for {height_stagnation_count} scrolls)...", extra={"step_name": "Collection"})
            
            # Hard exit on height stagnation (> 2 scrolls)
            if height_stagnation_count > 2:
                logger.info(f"Stop: Height stagnant for {height_stagnation_count} scrolls. Terminating search.", extra={"step_name": "Collection"})
                break

            # Adaptive scroll distance
            scroll_by = 1200 if no_growth_count < 3 else 2500
            self.driver.execute_script(f"window.scrollBy(0, {scroll_by});")
            time.sleep(random.uniform(2.0, 3.5))
            
            # Explicit "Load more" check
            if i % 3 == 0 or height_stagnation_count >= 2:
                try:
                    load_more_selectors = config.SELECTORS['post']['load_more_results']
                    if isinstance(load_more_selectors, str): load_more_selectors = [load_more_selectors]
                    
                    for selector in load_more_selectors:
                        # Try robust waiter first
                        if self.browser_manager.wait_click(selector, timeout=4, retries=2):
                            logger.info(f"Clicked 'Load More' button to force growth...", extra={"step_name": "Collection"})
                            no_growth_count = 0 
                            height_stagnation_count = 0 
                            time.sleep(4)
                            break
                except: pass

            if no_growth_count >= 12: 
                logger.info("Stop: Reached end of content (no growth detected).", extra={"step_name": "Collection"})
                break
            if new_total >= 60: 
                logger.info(f"Stop: Found sufficient new posts ({new_total}).", extra={"step_name": "Collection"})
                break

        return list(found_new_posts.values())

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
                try: data['post_html'] = post.get_attribute('outerHTML')
                except: pass
            
            # Click see more
            try:
                more_selectors = config.SELECTORS['post']['see_more_button']
                for selector in more_selectors:
                    try:
                        more_btns = post.find_elements(By.XPATH, selector)
                        for btn in more_btns:
                            if btn.is_displayed() and 'more' in btn.text.lower():
                                # Human-like click with random mouse offset
                                try:
                                    actions = ActionChains(self.driver)
                                    # Offset by random pixels (-3 to 3 range)
                                    x_off = random.randint(-4, 4)
                                    y_off = random.randint(-4, 4)
                                    actions.move_to_element_with_offset(btn, x_off, y_off).click().perform()
                                except:
                                    self.driver.execute_script("arguments[0].click();", btn)
                                
                                time.sleep(random.uniform(0.5, 1.5))
                                break
                    except: continue
            except: pass
            
            # Name
            try:
                name_selectors = config.SELECTORS['post']['author_name']
                for selector in name_selectors:
                    try:
                        name_elem = post.find_element(By.XPATH, selector)
                        name = name_elem.text.strip()
                        if name and 0 < len(name) < 100:
                            data['name'] = name
                            break
                    except: continue
                # Final fallback for name: check the first few words of the post text
                if not data['name']:
                    try:
                        raw_text = post.text.split('\n')[0].strip()
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
                        headline = headline_elem.text.strip()
                        if headline:
                            data['company'] = headline # We use company field for headline in post data
                            break
                    except: continue
            except: pass
            
            # Text
            try:
                text_elem = None
                text_selectors = config.SELECTORS['post']['content_text']
                for selector in text_selectors:
                    try:
                        text_elem = post.find_element(By.XPATH, selector)
                        if text_elem and text_elem.text.strip(): break
                    except: continue
                        
                if text_elem:
                    text = text_elem.text.strip()
                    cleaned = self.clean_post_text(text)
                    data['post_text'] = cleaned.encode('ascii', 'ignore').decode('ascii')
                
                # Final fallback for post text: capture all visible text if specific selectors failed
                if not data['post_text'] or len(data['post_text']) < 20:
                    try:
                        raw_all_text = post.text
                        cleaned = self.clean_post_text(raw_all_text)
                        if len(cleaned) > len(data.get('post_text', '')):
                            data['post_text'] = cleaned.encode('ascii', 'ignore').decode('ascii')
                    except: pass
            except: pass
            
            data['is_relevant'] = self.processor.is_ai_tech_related(data['post_text'])
            data['has_job'] = self.processor.has_job_keywords(data['post_text'])
            
            if data['post_text']:
                data['email'] = self.processor.extract_email(data['post_text'])
                data['phone'] = self.processor.extract_phone(data['post_text'])
            
            # Profile URL
            try:
                link_selectors = config.SELECTORS['post']['profile_link']
                for selector in link_selectors:
                    try:
                        link = post.find_element(By.XPATH, selector)
                        url = link.get_attribute('href')
                        if url and '/in/' in url:
                            data['profile_url'] = url.split('?')[0]
                            break
                    except: continue
            except: pass
        except: pass
        
        return data

    def extract_full_profile_data(self, profile_url):
        """Navigate to profile page and extract full details."""
        profile_data = {
            'full_name': '', 'email': '', 'phone': '',
            'company_name': '', 'location': '', 'linkedin_id': profile_url
        }
        
        try:
            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self.driver.get(profile_url)
            time.sleep(4)
            
            try:
                for selector in config.SELECTORS['profile']['full_name']:
                    try:
                        name_elem = self.driver.find_element(By.XPATH, selector)
                        profile_data['full_name'] = name_elem.text.strip()
                        break
                    except: pass
            except: pass
            
            try:
                for selector in config.SELECTORS['profile']['location']:
                    try:
                        loc_elem = self.driver.find_element(By.XPATH, selector)
                        profile_data['location'] = loc_elem.text.strip()
                        break
                    except: pass
            except: pass
        
            try:
                company_selectors = config.SELECTORS['profile']['company']
                for selector in company_selectors:
                    try:
                        company_elem = self.driver.find_element(By.XPATH, selector)
                        company_text = company_elem.text.strip()
                        if company_text and 0 < len(company_text) < 100:
                            profile_data['company_name'] = company_text
                            break
                    except: continue
            except: pass
            
            try:
                contact_btn = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, config.SELECTORS['profile']['contact_info_link']))
                )
                contact_btn.click()
                time.sleep(3)
                
                try:
                    email_link = self.driver.find_element(By.XPATH, config.SELECTORS['profile']['email_mailto'])
                    profile_data['email'] = email_link.get_attribute('href').replace('mailto:', '')
                except:
                    email = self.processor.extract_email(self.driver.page_source)
                    if email: profile_data['email'] = email
        
                try:
                    phone_section = self.driver.find_element(By.XPATH, config.SELECTORS['profile']['phone_section'])
                    phone = self.processor.extract_phone(phone_section.text)
                    if phone: profile_data['phone'] = phone
                except: pass
                    
            except:
                page_text = self.driver.page_source
                if not profile_data['email']: profile_data['email'] = self.processor.extract_email(page_text)
                if not profile_data['phone']: profile_data['phone'] = self.processor.extract_phone(page_text)
            
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            return profile_data
        except:
            try:
                if len(self.driver.window_handles) > 1: self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            except: pass
            return profile_data
