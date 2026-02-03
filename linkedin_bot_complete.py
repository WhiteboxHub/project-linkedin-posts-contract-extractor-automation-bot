"""Complete LinkedIn Bot - Extracts ALL required fields for database."""
import time
import csv
import re
import os
import json
import hashlib
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import config
from job_activity_logger import JobActivityLogger


class LinkedInBotComplete:
    def __init__(self, email=None, password=None, candidate_id=None, keywords=None):
        self.driver = None
        self.linkedin_email = email or config.LINKEDIN_EMAIL
        self.linkedin_password = password or config.LINKEDIN_PASSWORD
        self.candidate_id = candidate_id
        self.processed_profiles = set()
        self.processed_posts = set()  # Track post IDs to avoid duplicates
        self.keywords = keywords if keywords else []
        self.total_saved = 0
        self.posts_saved = 0  # Track total posts saved
        
        # Initialize logger with candidate ID if provided
        self.activity_logger = JobActivityLogger()
        if self.candidate_id:
            self.activity_logger.selected_candidate_id = self.candidate_id
            
        self.posts_dir = "saved_posts"  # Directory for saved posts
        self.load_processed_posts()  # Load previously processed post IDs
        self.profile_cache = {}      # Cache for profile data {url: {email, phone, etc}}
        self.load_processed_profiles() # Load previously processed profile URLs
        self.extracted_contacts_buffer = []  # Buffer for bulk sync to backend
        
    def load_processed_profiles(self):
        """Load already processed profile data from output CSV for reuse."""
        try:
            if os.path.exists(config.OUTPUT_FILE):
                with open(config.OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        url = row.get('linkedin_id')
                        if url:
                            url = url.strip().rstrip('/')
                            # Store full data for reuse
                            self.profile_cache[url] = {
                                'full_name': row.get('full_name', ''),
                                'email': row.get('email', ''),
                                'phone': row.get('phone', ''),
                                'company_name': row.get('company_name', ''),
                                'location': row.get('location', '')
                            }
                            self.processed_profiles.add(url)
                print(f"Loaded {len(self.processed_profiles)} profiles into cache from {config.OUTPUT_FILE}")
        except Exception as e:
            print(f"Could not load profiles for cache: {e}")
        
    def load_keywords(self):
        # If keywords provided in constructor, use them
        if self.keywords:
            print(f"Loaded {len(self.keywords)} keywords from config")
            return True
            
        try:
            with open(config.KEYWORDS_FILE, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            self.keywords = lines
            print(f"Loaded {len(self.keywords)} keywords from file")
            return True
        except FileNotFoundError:
            print(f"[ERROR] Keywords file not found")
            return False
    
    def load_processed_posts(self):
        """Load previously processed post IDs from all_posts.csv to avoid duplicates."""
        try:
            if os.path.exists('all_posts.csv'):
                with open('all_posts.csv', 'r', encoding='utf-8') as f:
                    # Skip header
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get('post_id'):
                            self.processed_posts.add(row['post_id'])
                print(f"Loaded {len(self.processed_posts)} previously processed post IDs from CSV")
            # Fallback to text file if it exists (migration support)
            elif os.path.exists('processed_posts.txt'):
                 with open('processed_posts.txt', 'r') as f:
                    ids = set(line.strip() for line in f if line.strip())
                    self.processed_posts.update(ids)
                 print(f"Loaded {len(ids)} processed IDs from legacy txt file")
        except Exception as e:
            print(f"Could not load processed posts: {e}")
            self.processed_posts = set()
    
    def save_processed_post_id(self, post_id):
        """Track locally."""
        self.processed_posts.add(post_id)
    
    def extract_post_id(self, post):
        """
        Extract unique post ID from LinkedIn post element.
        Uses data-urn, data-activity-urn, or searches for 'Copy link to post' URL.
        """
        try:
            # 1. Direct attribute check (standard LinkedIn)
            for attr in ['data-urn', 'data-activity-urn', 'data-id', 'componentkey']:
                val = post.get_attribute(attr)
                if val: return val
                
            # 1b. Check children for componentkey (User's pattern)
            try:
                elem = post.find_element(By.XPATH, ".//*[@componentkey or @data-urn]")
                for attr in ['componentkey', 'data-urn']:
                    val = elem.get_attribute(attr)
                    if val: return val
            except: pass

            # 2. Check for the 'time' link which often contains the URN
            # Usually look like "7 hours ago" or "1d"
            try:
                time_links = post.find_elements(By.XPATH, ".//a[contains(@href, 'feed/update/urn:li:activity:')]")
                for link in time_links:
                    href = link.get_attribute('href')
                    if 'urn:li:activity:' in href:
                        # Extract the part after activity:
                        match = re.search(r'urn:li:activity:(\d+)', href)
                        if match: return f"urn:li:activity:{match.group(1)}"
            except: pass
            try:
                # Look for the specific text suggested by the user
                # We search for the text because classes like '_2c6d258a' are obfuscated
                copy_link_elem = post.find_element(By.XPATH, ".//*[contains(text(), 'Copy link to post')]")
                if copy_link_elem:
                    # In some views, the ID might be in a parent attribute
                    parent = copy_link_elem.find_element(By.XPATH, "./..")
                    val = parent.get_attribute('data-control-name') or parent.get_attribute('id')
                    if val and 'activity' in val: return val
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
            
        # Remove "...more"
        text = text.replace("…more", "").replace("...more", "")
        
        # Remove "Like Comment Share" and similar UI text lines
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            if "Like Comment Share" in line or "Comment" == line.strip() or "Share" == line.strip():
                continue
            cleaned_lines.append(line)
        text = "\n".join(cleaned_lines)
            
        # Remove hashtags (e.g. #hiring, #AI) - handles embedded and standalone
        # This regex looks for # followed by alphanumerics
        text = re.sub(r'#\w+', '', text)
        
        # Clean up extra whitespace
        text = re.sub(r'\n{3,}', '\n\n', text) # Max 2 newlines
        text = re.sub(r'[ \t]+', ' ', text) # Collapse spaces
        
        return text.strip()

    def save_full_post(self, post_content, post_id, keyword, metadata=None):
        """Save full post content to a single file per keyword, including metadata."""
        try:
            # Create posts directory if it doesn't exist
            if not os.path.exists(self.posts_dir):
                os.makedirs(self.posts_dir)
            
            # Create safe keyword filename
            safe_keyword = keyword.replace(' ', '_').replace('/', '_')
            filename = f"{safe_keyword}_posts.txt"
            filepath = os.path.join(self.posts_dir, filename)
            
            # Prepare metadata text
            meta_text = ""
            if metadata:
                meta_text = (
                    f"Full Name: {metadata.get('full_name', 'N/A')}\n"
                    f"Email: {metadata.get('email', 'N/A')}\n"
                    f"Phone: {metadata.get('phone', 'N/A')}\n"
                    f"LinkedIn ID: {metadata.get('linkedin_id', 'N/A')}\n"
                    f"Company: {metadata.get('company_name', 'N/A')}\n"
                    f"Location: {metadata.get('location', 'N/A')}\n"
                    f"Extraction Date: {metadata.get('extraction_date', 'N/A')}\n"
                    f"Search Keyword: {metadata.get('search_keyword', 'N/A')}\n"
                )
            
            # Append plain text content
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write("\n" + "=" * 80 + "\n")
                f.write(f"POST ID: {post_id}\n")
                f.write("-" * 80 + "\n")
                if meta_text:
                    f.write("METADATA:\n")
                    f.write(meta_text)
                    f.write("-" * 80 + "\n")
                f.write("POST CONTENT:\n\n")
                f.write(post_content)
                f.write("\n\n")
            
            return True
        except Exception as e:
            print(f"      [ERROR saving post]: {e}")
            return False
    
    def save_post_metadata(self, post_data, keyword, post_id):
        """Save post metadata to master posts CSV."""
        posts_csv = 'all_posts.csv'
        file_exists = os.path.exists(posts_csv)
        
        try:
            with open(posts_csv, 'a', newline='', encoding='utf-8') as f:
                fieldnames = [
                    'post_id', 'post_url', 'keyword', 'author_name', 'post_text_preview',
                    'profile_url', 'has_email', 'has_phone', 'is_job_post',
                    'is_ai_related', 'extraction_date'
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                # Truncate post text for preview
                post_preview = post_data.get('post_text', '')[:200]
                
                # Construct post URL from URN if available
                post_url = post_data.get('post_url', '')
                if not post_url and post_id and 'urn:li:activity:' in post_id:
                    post_url = f"https://www.linkedin.com/feed/update/{post_id}/"
                
                writer.writerow({
                    'post_id': post_id,
                    'post_url': post_url,
                    'keyword': keyword,
                    'author_name': post_data.get('name', ''),
                    'post_text_preview': post_preview,
                    'profile_url': post_data.get('profile_url', ''),
                    'has_email': 'Yes' if post_data.get('email') else 'No',
                    'has_phone': 'Yes' if post_data.get('phone') else 'No',
                    'is_job_post': 'Yes' if post_data.get('has_job') else 'No',
                    'is_ai_related': 'Yes' if post_data.get('is_relevant') else 'No',
                    'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
            return True
        except Exception as e:
            print(f"      [ERROR saving metadata]: {e}")
            return False
    
    def init_driver(self):
        print("Initializing Chrome...")
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        print("Chrome ready!")
        
    def login(self):
        """Login."""
        print("Logging in...")
        self.driver.get("https://www.linkedin.com/login")
        time.sleep(2)
        
        self.driver.find_element(By.ID, "username").send_keys(self.linkedin_email)
        time.sleep(1)
        self.driver.find_element(By.ID, "password").send_keys(self.linkedin_password)
        time.sleep(1)
        self.driver.find_element(By.ID, "password").send_keys(Keys.RETURN)
        time.sleep(5)
        print("Logged in!")
        
    def apply_sort_filter(self):
        """Apply the Sort By filter on the search results page."""
        try:
            # Check if already sorted (via URL)
            current_url = self.driver.current_url
            if 'sortBy' in current_url:
                print("  Sort already applied via URL.")
                return True

            print("  Applying Sort By filter via UI...")
            
            # 1. Click the "Sort by" button - Try multiple selectors
            sort_button = None
            sort_selectors = [
                (By.ID, "searchFilter_sortBy"),
                (By.XPATH, "//button[contains(., 'Sort by')]"),
                (By.XPATH, "//div[@role='button'][contains(., 'Sort by')]"),
                (By.XPATH, "//button[contains(@aria-label, 'Sort by')]"),
                (By.CSS_SELECTOR, "div[data-view-name='search-filter-top-bar-select']")
            ]
            
            for selector_type, selector_val in sort_selectors:
                try:
                    sort_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((selector_type, selector_val))
                    )
                    if sort_button:
                        break
                except:
                    continue
            
            if not sort_button:
                raise Exception("Could not find Sort button")

            sort_button.click()
            time.sleep(2)

            # 2. Select the option based on config
            sort_value = getattr(config, 'SORT_BY', 'latest').lower()
            if sort_value == 'latest':
                option_selectors = [
                    (By.ID, "sortBy-date_posted"),
                    (By.XPATH, "//label[contains(., 'Latest')]"),
                    (By.XPATH, "//span[contains(., 'Latest')]")
                ]
            else: # relevance / top match
                option_selectors = [
                    (By.ID, "sortBy-relevance"),
                    (By.XPATH, "//label[contains(., 'Top match')]"),
                    (By.XPATH, "//label[contains(., 'Relevance')]"),
                    (By.XPATH, "//span[contains(., 'Top match')]")
                ]
            
            # Click the radio button/label
            option_clicked = False
            for selector_type, selector_val in option_selectors:
                try:
                    option_element = self.driver.find_element(selector_type, selector_val)
                    self.driver.execute_script("arguments[0].click();", option_element)
                    option_clicked = True
                    break
                except:
                    continue
            
            if not option_clicked:
                raise Exception(f"Could not find sort option for: {sort_value}")
                
            time.sleep(1)
            
            # Click show results
            show_results_selectors = [
                "//button[contains(@aria-label, 'Apply current filter')]",
                "//button[contains(., 'Show results')]",
                "//button[contains(@class, 'search-reusable-footer__apply-button')]"
            ]
            
            for selector in show_results_selectors:
                try:
                    show_results_btn = self.driver.find_element(By.XPATH, selector)
                    if show_results_btn.is_displayed():
                        show_results_btn.click()
                        time.sleep(3)
                        return True
                except:
                    continue
            
            return True
        except Exception as e:
            print(f"  [Warning] Could not apply sort filter via UI: {e}")
            return False

    def search_posts(self, keyword):
        print(f"\n{'='*60}\nKEYWORD: {keyword}\n{'='*60}")
        
        # Ensure we are logged in/on a clean state
        if 'linkedin.com' not in self.driver.current_url:
            self.driver.get("https://www.linkedin.com/feed/")
            time.sleep(3)
        
        try:
            # Clear search and enter keyword
            search_box = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@data-view-name='search-global-typeahead-input']"))
            )
            search_box.click() 
            search_box.clear()
            time.sleep(1)
            
            for char in keyword:
                search_box.send_keys(char)
                time.sleep(0.05)
            
            time.sleep(2)
            search_box.send_keys(Keys.RETURN)
            time.sleep(5)
            
            # Transition to 'Posts' (Content) tab
            current_url = self.driver.current_url
            if '/search/results/all' in current_url or '/search/results/people' in current_url:
                print("  Switching to 'Posts' tab...")
                posts_url = current_url.replace('/search/results/all', '/search/results/content').replace('/search/results/people', '/search/results/content')
                
                # Apply sort via URL parameter
                sort_val = getattr(config, 'SORT_BY', 'latest').lower()
                if 'sortBy' not in posts_url:
                    sort_param = '%5B%22date_posted%22%5D' if sort_val == 'latest' else '%5B%22relevance%22%5D'
                    sep = '&' if '?' in posts_url else '?'
                    posts_url += f"{sep}sortBy={sort_param}"
                
                self.driver.get(posts_url)
                time.sleep(5)
            
            # Final check to ensure we are on the right tab
            if '/search/results/content' not in self.driver.current_url:
                try:
                    posts_tab = self.driver.find_element(By.XPATH, "//button[contains(., 'Posts')]")
                    posts_tab.click()
                    time.sleep(4)
                except: pass
            
            return True
        except Exception as e:
            print(f"  [Error searching]: {e}")
            return False
    
    def extract_email(self, text):
        if not text:
            return None
            
        patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        ]
        
        # Invalid email patterns to exclude (mostly media extensions or placeholders)
        invalid_patterns = [
            r'\.png', r'\.jpg', r'\.jpeg', r'\.gif', r'\.svg',
            r'@2x\.', r'entity-circle', r'placeholder',
            r'example\.com', r'test\.com'
        ]
        
        for pattern in patterns:
            emails = re.findall(pattern, text, re.IGNORECASE)
            for email in emails:
                if '@' in email and '.' in email.split('@')[1]:
                   
                    is_invalid = any(re.search(inv, email, re.IGNORECASE) for inv in invalid_patterns)
                    
                 
                    if config.LINKEDIN_EMAIL and email.lower().strip() == config.LINKEDIN_EMAIL.lower().strip():
                        is_invalid = True
                        
                    if not is_invalid:
                        return email
        
        return None
    
    def extract_phone(self, text):
        if not text:
            return None
        patterns = [
            r'\+?\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}',
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',
        ]
        for pattern in patterns:
            phones = re.findall(pattern, text)
            if phones:
                return phones[0]
        return None
    
    def has_job_keywords(self, text):
        
        if not text:
            return False
        text_lower = text.lower()
        return any(kw in text_lower for kw in config.JOB_KEYWORDS)
    
    def is_ai_tech_related(self, text):
        
        if not text:
            return False
        text_lower = text.lower()
        return any(kw in text_lower for kw in config.AI_KEYWORDS)
    
    def get_posts(self):
        print("  Scanning for posts...")
        last_count = 0
        no_new_posts_count = 0
        
    def get_posts(self):
        print("  Scanning for posts...")
        last_total = 0
        no_growth_count = 0
        
        # Consistent aggressive scrolling
        for i in range(1, 41): # Up to 40 scrolls
            current_elements = self._find_post_elements()
            total_visible = len(current_elements)
            
            # Count only NEW posts for logging
            new_count = 0
            for p in current_elements:
                p_id = self.extract_post_id(p)
                if p_id and p_id not in self.processed_posts:
                    new_count += 1
            
            if total_visible > last_total:
                print(f"    - Scroll {i}: Found {total_visible} total ({new_count} are new)...")
                last_total = total_visible
                no_growth_count = 0
            else:
                no_growth_count += 1
            
            # Scroll down
            scroll_by = 1200 if no_growth_count < 3 else 2500
            self.driver.execute_script(f"window.scrollBy(0, {scroll_by});")
            time.sleep(2.5)
            
            # Try to click "Load more" or "Show more results" every 3rd scroll
            if i % 3 == 0:
                try:
                    # Specific pattern provided by user + generic fallbacks
                    load_more_selectors = [
                        "//button[.//span[contains(text(), 'Load more')]]",
                        "//button[contains(., 'Load more')]",
                        "//button[contains(., 'Show more results')]",
                        "//button[contains(@class, 'infinite-scroll')]"
                    ]
                    for selector in load_more_selectors:
                        btns = self.driver.find_elements(By.XPATH, selector)
                        for btn in btns:
                            if btn.is_displayed():
                                print(f"    [Action] Clicking '{btn.text.strip()}' button...")
                                self.driver.execute_script("arguments[0].click();", btn)
                                time.sleep(4)
                                # Reset growth count if we clicked something
                                no_growth_count = 0
                                break
                except:
                    pass

            # Break if we've scrolled a lot and find absolutely no more content
            if no_growth_count >= 12:
                break
                
            # If we have 60+ new posts, that's a good batch
            if new_count >= 60:
                break

        final_posts = self._find_post_elements()
        return final_posts

    def _find_post_elements(self):
        """Internal helper to find posts on the page."""
        post_selectors = [
            # High-level update containers
            "//div[contains(@class, 'feed-shared-update-v2')]",
            "//div[@data-view-name='feed-full-update']",
            "//div[contains(@class, 'reusable-search__result-container')]",
            "//li[contains(@class, 'reusable-search__result-container')]",
            "//div[contains(@id, 'ember') and contains(@class, 'search-results__list-item')]",
            # User provided / specific commentator tags
            "//*[contains(@data-view-name, 'feed-commentary')]/ancestor::div[contains(@class, 'update-v2')]",
            "//*[contains(@data-testid, 'expandable-text-box')]/ancestor::div[contains(@class, 'update-v2')]"
        ]
        
        all_found = []
        for selector in post_selectors:
            try:
                found = self.driver.find_elements(By.XPATH, selector)
                if found:
                    for p in found:
                        if p.is_displayed() and p not in all_found:
                            # Verify it has some content
                            if p.text.strip():
                                all_found.append(p)
            except: pass
            
        return all_found

    

    
    def extract_post_data(self, post, get_full_html=False):
        """Extract data from post. Optionally get full HTML."""
        data = {
            'name': '', 'email': '', 'phone': '', 'post_text': '',
            'profile_url': '', 'company': '', 'location': '', 'post_url': '',
            'is_relevant': False, 'has_job': False, 'post_html': ''
        }
        
        try:
            # Get full HTML if requested
            if get_full_html:
                try:
                    data['post_html'] = post.get_attribute('outerHTML')
                except:
                    pass
            
            # Try to click see more to get full content
            try:
                more_selectors = [
                    ".//button[@data-testid='expandable-text-button']",
                    ".//button[contains(@class, 'see-more')]",
                    ".//button[contains(., 'more')]",
                    ".//span[contains(., '...more')]"
                ]
                for selector in more_selectors:
                    try:
                        more_btns = post.find_elements(By.XPATH, selector)
                        for btn in more_btns:
                            if btn.is_displayed() and 'more' in btn.text.lower():
                                self.driver.execute_script("arguments[0].click();", btn)
                                time.sleep(0.5)
                                break
                    except:
                        continue
            except:
                pass
            
            try:
                name_selectors = [
                    ".//span[@aria-hidden='true']",
                    ".//span[contains(@class, 'update-components-actor__name')]",
                    ".//span[contains(@class, 'entity-result__title-text')]",
                    ".//div[contains(@class, 't-black')]//span",
                    ".//div[contains(@class, 'actor')]//span",
                    ".//p[contains(@class, 'actor')]//span"
                ]
                for selector in name_selectors:
                    try:
                        name_elem = post.find_element(By.XPATH, selector)
                        name = name_elem.text.strip()
                        if name and 0 < len(name) < 100:
                            data['name'] = name
                            break
                    except:
                        continue
            except:
                pass
            
            try:
                text_elem = None
                text_selectors = [
                    ".//p[@data-view-name='feed-commentary']//span[@data-testid='expandable-text-box']",
                    ".//div[@data-view-name='feed-commentary']//span[@data-testid='expandable-text-box']",
                    ".//p[@data-view-name='feed-commentary']",
                    ".//div[@data-view-name='feed-commentary']",
                    ".//span[@data-testid='expandable-text-box']",
                    ".//div[contains(@class, 'update-components-text')]",
                    ".//span[contains(@class, 'break-words')]",
                    ".//div[contains(@class, 'feed-shared-update-v2__description')]",
                    ".//div[contains(@class, 'feed-shared-update-v2__commentary')]",
                    ".//div[@data-view-name='feed-full-update']//span"
                ]
                for selector in text_selectors:
                    try:
                        text_elem = post.find_element(By.XPATH, selector)
                        if text_elem and text_elem.text.strip():
                            break
                    except:
                        continue
                        
                if text_elem:
                    text = text_elem.text.strip()
                    # Clean the text using our helper method
                    cleaned = self.clean_post_text(text)
                    data['post_text'] = cleaned.encode('ascii', 'ignore').decode('ascii')
            except:
                pass
            
            
            data['is_relevant'] = self.is_ai_tech_related(data['post_text'])
            data['has_job'] = self.has_job_keywords(data['post_text'])
            
            if not data['is_relevant'] or not data['has_job']:
                rel_str = "Yes" if data['is_relevant'] else "No"
                job_str = "Yes" if data['has_job'] else "No"
                # Internal debug print, will be seen in logs
                # print(f"      [Debug] Relevant: {rel_str}, Job: {job_str}")
                pass
            
            
            if data['post_text']:
                data['email'] = self.extract_email(data['post_text'])
                data['phone'] = self.extract_phone(data['post_text'])
            
            
            try:
                link_selectors = [
                    ".//a[contains(@href, '/in/')]",
                    ".//a[contains(@class, 'update-components-actor__container-link')]",
                    ".//a[contains(@class, 'app-aware-link') and contains(@href, '/in/')]",
                    ".//a[contains(@data-test-app-aware-link, '') and contains(@href, '/in/')]"
                ]
                for selector in link_selectors:
                    try:
                        link = post.find_element(By.XPATH, selector)
                        url = link.get_attribute('href')
                        if url and '/in/' in url:
                            data['profile_url'] = url.split('?')[0]
                            break
                    except:
                        continue
            except:
                pass
        except:
            pass
        
        return data
    
    def extract_full_profile_data(self, profile_url):
        
        profile_data = {
            'full_name': '',
            'email': '',
            'phone': '',
            'company_name': '',
            'location': '',
            'linkedin_id': profile_url
        }
        
        try:
            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            self.driver.get(profile_url)
            time.sleep(4)
            
           
            try:
                name_elem = self.driver.find_element(By.XPATH, "//h1[contains(@class, 't-24')]")
                profile_data['full_name'] = name_elem.text.strip()
            except:
                try:
                    name_elem = self.driver.find_element(By.XPATH, "//h1")
                    profile_data['full_name'] = name_elem.text.strip()
                except:
                    pass
            
      
            try:
                loc_elem = self.driver.find_element(By.XPATH, "//span[contains(@class, 'text-body-small') and contains(., ',')]")
                profile_data['location'] = loc_elem.text.strip()
            except:
                try:
                    loc_elem = self.driver.find_element(By.XPATH, "//div[contains(@class, 'mt2')]//span[contains(text(), ',')]")
                    profile_data['location'] = loc_elem.text.strip()
                except:
                    pass
        
      
            try:
                
                company_selectors = [
                    "//div[contains(@class, 'inline-show-more-text')]//div[contains(@class, 'inline-show-more-text--is-collapsed')]",
                    "//section[contains(@id, 'experience')]//div[contains(@class, 'pvs-entity__path-node')]//span[contains(@aria-hidden, 'true')]",
                    "//section[contains(@id, 'experience')]//span[contains(@class, 't-bold')]",
                    "//div[@id='experience']//span[@aria-hidden='true']"
                ]
                
                for selector in company_selectors:
                    try:
                        company_elem = self.driver.find_element(By.XPATH, selector)
                        company_text = company_elem.text.strip()
                        if company_text and len(company_text) > 0 and len(company_text) < 100:
                            profile_data['company_name'] = company_text
                            break
                    except:
                        continue
            except:
                    pass
            
            
            try:
                contact_btn = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, 'overlay/contact-info')]"))
                )
                contact_btn.click()
                time.sleep(3)
                
                
                try:
                    email_link = self.driver.find_element(By.XPATH, "//a[contains(@href, 'mailto:')]")
                    profile_data['email'] = email_link.get_attribute('href').replace('mailto:', '')
                except:
                    
                    email = self.extract_email(self.driver.page_source)
                    if email:
                        profile_data['email'] = email
                
        
                try:
                    phone_section = self.driver.find_element(By.XPATH, "//section[contains(@class, 'pv-contact-info')]")
                    phone = self.extract_phone(phone_section.text)
                    if phone:
                        profile_data['phone'] = phone
                except:
                    pass
                    
            except:
                
                page_text = self.driver.page_source
                if not profile_data['email']:
                    profile_data['email'] = self.extract_email(page_text)
                if not profile_data['phone']:
                    profile_data['phone'] = self.extract_phone(page_text)
            
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
            
            return profile_data
                
        except Exception as e:
            try:
                if len(self.driver.window_handles) > 1:
                    self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            except:
                pass
            return profile_data
    
    def save_contact(self, data, keyword):
        """Save to CSV immediately."""
        file_exists = os.path.exists(config.OUTPUT_FILE)
        
        try:
            # Save local first
            with open(config.OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                fieldnames = [
                    'full_name', 'email', 'phone', 'linkedin_id',
                    'company_name', 'location', 'post_url', 'extraction_date', 'search_keyword'
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow({
                    'full_name': data.get('full_name', '') or 'Unknown',
                    'email': data.get('email', ''),
                    'phone': data.get('phone', ''),
                    'linkedin_id': data.get('linkedin_id', ''),
                    'company_name': data.get('company_name', ''),
                    'location': data.get('location', ''),
                    'post_url': data.get('post_url', ''),
                    'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                    'search_keyword': keyword
                })
            
            # Queue for bulk sync to WBL Backend
            self.extracted_contacts_buffer.append(data)
            
            self.total_saved += 1
            return True
        except Exception as e:
            print(f"      [ERROR SAVING] {e}")
            return False
    
    def process_keyword(self, keyword):
        """Process keyword."""
        if not self.search_posts(keyword):
            return 0
        
        posts = self.get_posts()
        if not posts:
            print("  No posts")
            return 0
        
        print(f"  Processing {len(posts)} posts...")
        found = 0
        posts_processed = 0
        
        
        for post in posts:
            # Extract post ID first
            post_id = self.extract_post_id(post)
            
            # Skip if we've already processed this post
            if post_id and post_id in self.processed_posts:
                continue
            
            # Extract post data
            post_data = self.extract_post_data(post, get_full_html=True)
            
            # Construct post URL
            post_url = ""
            if post_id and 'urn:li:activity:' in post_id:
                post_url = f"https://www.linkedin.com/feed/update/{post_id}/"
            post_data['post_url'] = post_url

            # Initialize metadata with what we have from the post
            current_meta = {
                'full_name': post_data.get('name', ''),
                'email': post_data.get('email', ''),
                'phone': post_data.get('phone', ''),
                'linkedin_id': post_data.get('profile_url', ''), # Use profile URL as ID
                'company_name': post_data.get('company', ''),
                'location': post_data.get('location', ''),
                'post_url': post_url,
                'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                'search_keyword': keyword
            }
            
            is_extracted = False
            
            # Normalize profile URL to prevent duplicates due to trailing slashes
            raw_url = post_data.get('profile_url')
            normalized_url = raw_url.strip().rstrip('/') if raw_url else ""
            
            # Check relevance for full extraction
            is_relevant = post_data['has_job'] and post_data['is_relevant']
            
            if normalized_url and is_relevant:
                # CASE 1: New post, but we already have contact info for this person
                if normalized_url in self.profile_cache:
                    cached = self.profile_cache[normalized_url]
                    print(f"    [Cache Match] Re-extracting new post from: {cached['full_name']}")
                    
                    final_data = {
                        'full_name': cached['full_name'],
                        'email': cached['email'],
                        'phone': cached['phone'],
                        'linkedin_id': normalized_url,
                        'company_name': cached['company_name'],
                        'location': cached['location'],
                        'post_url': post_url,
                        'extraction_date': current_meta['extraction_date'],
                        'search_keyword': keyword
                    }
                    self.save_contact(final_data, keyword)
                    is_extracted = True
                    found += 1
                
                # CASE 2: New post and brand new person
                elif normalized_url not in self.processed_profiles:
                    print(f"  NEW RELEVANT POST FOUND - Extracting full profile...")
                    profile_data = self.extract_full_profile_data(post_data['profile_url'])
        
                    best_email = profile_data['email'] or post_data['email']
                    
                    if best_email:
                        found += 1
                        print(f"  [{found}] NEW CONTACT: {profile_data['full_name'] or post_data['name']}")
                        
                        final_data = {
                            'full_name': profile_data['full_name'] or post_data['name'],
                            'email': best_email,
                            'phone': profile_data['phone'] or post_data['phone'],
                            'linkedin_id': profile_data['linkedin_id'],
                            'company_name': profile_data['company_name'],
                            'location': profile_data['location'],
                            'post_url': post_url,
                            'extraction_date': current_meta['extraction_date'],
                            'search_keyword': keyword
                        }
                        
                        current_meta.update(final_data)
                        self.save_contact(final_data, keyword)
                        
                        # Add to cache to avoid re-visiting this profile in this run
                        self.profile_cache[normalized_url] = final_data
                        self.processed_profiles.add(normalized_url)
                        
                        is_extracted = True
                    else:
                        print(f"  [SKIPPED] No email found for: {profile_data['full_name'] or post_data['name']}")
                        self.processed_profiles.add(normalized_url)
            else:
                if not normalized_url:
                    # Skip silent for posts with no profile link (often ads or broken)
                    pass
                elif normalized_url in self.processed_profiles:
                    # print(f"      [Skip] Already processed: {normalized_url}")
                    pass
                elif not is_relevant:
                    reasons = []
                    if not post_data['has_job']: reasons.append("No job keywords")
                    if not post_data['is_relevant']: reasons.append("Not AI related")
                    print(f"      [Skip] {', '.join(reasons)} (ID: {post_id[:8]})")
            
            # Save ALL posts (relevant or not) with best available metadata
            if post_id:
                status_msg = " (With Contact Info)" if is_extracted else ""
                print(f"  [Post {posts_processed + 1}] Saving post {post_id[:30]}...{status_msg}")
                
                if self.save_full_post(post_data['post_text'], post_id, keyword, metadata=current_meta):
                    if not is_extracted:
                        print(f"      ✓ Post saved")

                # Save metadata to CSV
                self.save_post_metadata(post_data, keyword, post_id)
                
                # Mark as processed
                self.save_processed_post_id(post_id)
                self.posts_saved += 1
                posts_processed += 1

            time.sleep(2)
        
        print(f"  Keyword complete: {posts_processed} posts saved, {found} contacts extracted")
        return found
    
    def run(self):
        """Run bot."""
        try:
            print("=" * 60)
            print("LinkedIn Complete Data Extractor")
            print("=" * 60)
            print("Extracts: Name, Email, Phone, Company, Location")
            print(f"Output: {config.OUTPUT_FILE}")
            
            
            cand_id = getattr(self.activity_logger, 'selected_candidate_id', 0)
            if cand_id != 0:
                print(f"Logging activity for Candidate ID: {cand_id}")
            
            print("=" * 60)
            
            if not self.load_keywords():
                return
            
            self.init_driver()
            self.login()
            
            for idx, keyword in enumerate(self.keywords, 1):
                print(f"\n[Keyword {idx}/{len(self.keywords)}]")
                self.process_keyword(keyword)
                
                if idx < len(self.keywords):
                    time.sleep(3)
            
            # Perform bulk sync of extracting contacts to WBL backend
            if self.extracted_contacts_buffer:
                print(f"\nBulk syncing {len(self.extracted_contacts_buffer)} contacts to WBL backend...")
                self.activity_logger.bulk_save_vendor_contacts(
                    self.extracted_contacts_buffer, 
                    source_email=self.linkedin_email
                )
            
            print("\n" + "=" * 60)
            print(f"COMPLETED!")
            print(f"  - {self.posts_saved} total posts saved")
            print(f"  - {self.total_saved} contacts extracted")
            print(f"  - Posts saved to: {self.posts_dir}/")
            print(f"  - Post metadata: all_posts.csv")
            print(f"  - Contacts: {config.OUTPUT_FILE}")
            print("=" * 60)
            
           
            # Log activity with contact details in notes
            print("\nLogging activity to WBL backend...")
            
            notes = f"LinkedIn extraction: {self.posts_saved} posts saved, {self.total_saved} contacts extracted.\n\n"
            
            if os.path.exists(config.OUTPUT_FILE) and self.total_saved > 0:
                try:
                    import csv
                    contacts_summary = []
                    with open(config.OUTPUT_FILE, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        rows = list(reader)
                        latest_rows = rows[-self.total_saved:] if len(rows) >= self.total_saved else rows
                        
                        for row in latest_rows:
                            item_details = [
                                f"Name: {row.get('full_name', 'N/A')}",
                                f"Email: {row.get('email', 'N/A')}",
                                f"Phone: {row.get('phone', 'N/A')}",
                                f"LinkedIn ID: {row.get('linkedin_id', 'N/A')}",
                                f"Company: {row.get('company_name', 'N/A')}",
                                f"Location: {row.get('location', 'N/A')}",
                                f"Post URL: {row.get('post_url', 'N/A')}",
                                f"Date: {row.get('extraction_date', 'N/A')}",
                                f"Keyword: {row.get('search_keyword', 'N/A')}"
                            ]
                            contacts_summary.append(" | ".join(item_details))
                    
                    if contacts_summary:
                        notes += "Extracted Contacts details:\n" + "\n".join(contacts_summary)
                except Exception as read_err:
                    print(f"  [Warning] Could not read CSV for notes: {read_err}")
                    notes += f"Summary: {self.total_saved} contacts saved to {config.OUTPUT_FILE}"

            self.activity_logger.log_activity(
                activity_count=self.total_saved,
                notes=notes
            )
        
        except KeyboardInterrupt:
            print(f"\n\n[STOPPED] Saved: {self.total_saved}")
            
            # Perform bulk sync for contacts collected before interruption
            if self.extracted_contacts_buffer:
                print(f"Syncing {len(self.extracted_contacts_buffer)} contacts to backend before exit...")
                self.activity_logger.bulk_save_vendor_contacts(
                    self.extracted_contacts_buffer, 
                    source_email=self.linkedin_email
                )
            
            if self.total_saved > 0:
                print("Logging partial activity...")
                # Try to build notes for partial run too
                notes = f"LinkedIn extraction stopped by user. {self.total_saved} contacts extracted.\n\n"
                if os.path.exists(config.OUTPUT_FILE):
                    try:
                        import csv
                        contacts_summary = []
                        with open(config.OUTPUT_FILE, 'r', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            rows = list(reader)
                            latest_rows = rows[-self.total_saved:] if len(rows) >= self.total_saved else rows
                            for row in latest_rows:
                                item_details = [
                                    f"Name: {row.get('full_name', 'N/A')}",
                                    f"Email: {row.get('email', 'N/A')}",
                                    f"Phone: {row.get('phone', 'N/A')}",
                                    f"Post URL: {row.get('post_url', 'N/A')}"
                                ]
                                contacts_summary.append(" | ".join(item_details))
                        if contacts_summary:
                            notes += "Partial Contacts:\n" + "\n".join(contacts_summary)
                    except:
                        pass
                        
                self.activity_logger.log_activity(
                    activity_count=self.total_saved,
                    notes=notes
                )
                
        except Exception as e:
            print(f"\n[ERROR] {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            if self.driver:
                print("\nClosing...")
                time.sleep(3)
                self.driver.quit()


if __name__ == "__main__":
    import json
    
    # Check for candidates.json
    candidates_file = "candidates.json"
    
    if os.path.exists(candidates_file):
        print(f"Found {candidates_file}. running multi-candidate mode...")
        try:
            with open(candidates_file, 'r') as f:
                candidates = json.load(f)
            
            if not candidates:
                print(" [WARNING] candidates.json exists but is empty. Falling back to .env settings...")
                # Fall through to single user block
            else:
                for i, cand in enumerate(candidates, 1):
                    try:
                        print(f"\n\n{'#'*80}")
                        print(f"PROCESSING CANDIDATE {i}/{len(candidates)}")
                        print(f"Email: {cand.get('linkedin_email')}")
                        print(f"Candidate ID: {cand.get('candidate_id', 'Not Set')}")
                        print(f"{'#'*80}\n")
                        
                        if not cand.get('linkedin_email') or not cand.get('linkedin_password'):
                            print("Skipping - missing credentials")
                            continue
                            
                        bot = LinkedInBotComplete(
                            email=cand.get('linkedin_email'),
                            password=cand.get('linkedin_password'),
                            candidate_id=cand.get('candidate_id'),
                            keywords=cand.get('keywords', [])
                        )
                        bot.run()
                        
                        # Cool down between candidates
                        if i < len(candidates):
                            print("Waiting 10 seconds before next candidate...")
                            time.sleep(10)
                            
                    except Exception as e:
                        print(f"Error processing candidate {i}: {e}")
                        continue
                
                print("\nAll candidates processed.")
                exit(0)

        except Exception as e:
            print(f"Error reading config: {e}")
            print("Falling back to .env settings...")

    else:
        print("No candidates.json found. Running in single-user mode using .env settings.")

    # Fallback to single user .env mode
    bot = LinkedInBotComplete()
    bot.run()
