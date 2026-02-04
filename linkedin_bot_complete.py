"""Complete LinkedIn Bot - Extracts ALL required fields for database."""
import time
import csv
import re
import os
import json
import hashlib
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium_stealth import stealth
import undetected_chromedriver as uc
import psutil
import duckdb
import config
from modules import ScraperModule, ProcessorModule
from job_activity_logger import JobActivityLogger


class LinkedInBotComplete:
    """
    Main Orchestrator class. 
    Controls the bot's workflow, manages driver life-cycle, handles local storage (CSV/Files),
    and coordinates between the ScraperModule, ProcessorModule, and JobActivityLogger.
    """
    def __init__(self, email=None, password=None, candidate_id=None, keywords=None):
        self.driver = None
        self.linkedin_email = email or config.LINKEDIN_EMAIL
        self.linkedin_password = password or config.LINKEDIN_PASSWORD
        self.candidate_id = candidate_id
        self.processed_profiles = set()
        self.processed_posts = set()  # Track post IDs to avoid duplicates
        self.keywords = keywords if keywords else []
        self.total_saved = 0
        self.posts_saved = 0  # Track total posts saved to local storage
        self.total_seen = 0   # Total posts encountered during session
        self.total_relevant = 0 # Total posts passing AI/Tech keyword filter
        self.total_synced = 0 # Total contacts successfully sent to backend
        
        # Initialize logger with candidate ID if provided
        self.activity_logger = JobActivityLogger()
        if self.candidate_id:
            self.activity_logger.selected_candidate_id = self.candidate_id
            
        self.posts_dir = "saved_posts"  # Directory for saved posts
        self.load_processed_posts()  # Load previously processed post IDs
        self.profile_cache = {}      # Cache for profile data {url: {email, phone, etc}}
        self.load_processed_profiles() # Load previously processed profile URLs
        self.extracted_contacts_buffer = []  # Buffer for bulk sync to backend
        
        # Initialize modules
        self.processor = ProcessorModule()
        self.scraper = None  # Initialized after driver in init_driver
        
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
    
    def _ensure_db_schema(self, con):
        """Create the posts table with exact fieldnames from legacy all_posts.csv."""
        con.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                post_id VARCHAR PRIMARY KEY,
                post_url VARCHAR,
                keyword VARCHAR,
                author_name VARCHAR,
                post_text_preview TEXT,
                profile_url VARCHAR,
                has_email BOOLEAN,
                has_phone BOOLEAN,
                is_job_post BOOLEAN,
                is_ai_related BOOLEAN,
                extraction_date TIMESTAMP
            )
        """)

    def load_processed_posts(self):
        """Load previously processed post IDs from DuckDB to avoid duplicates."""
        try:
            db_file = 'linkedin_data.db'
            con = duckdb.connect(db_file)
            self._ensure_db_schema(con)
            
            # Load IDs
            results = con.execute("SELECT post_id FROM posts").fetchall()
            for row in results:
                self.processed_posts.add(row[0])
            
            print(f"Loaded {len(self.processed_posts)} previously processed post IDs from DuckDB ({db_file})")
            con.close()
        except Exception as e:
            print(f"Could not load processed posts from DuckDB: {e}")
            self.processed_posts = set()
    
    def save_processed_post_id(self, post_id):
        """Track locally."""
        self.processed_posts.add(post_id)
    
    

    def save_full_post(self, text, post_id, keyword, metadata=None):
        """Save the actual post content to a separate file for storage."""
        if config.DRY_RUN:
            return True
            
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
                f.write(text)
                f.write("\n\n")
            
            return True
        except Exception as e:
            print(f"      [ERROR saving post]: {e}")
            return False
    
    def save_post_metadata(self, post_data, keyword, post_id):
        """Save post metadata to DuckDB database."""
        if config.DRY_RUN:
            return True
            
        try:
            db_file = 'linkedin_data.db'
            con = duckdb.connect(db_file)
            self._ensure_db_schema(con)
            
            # Truncate post text for preview
            post_preview = post_data.get('post_text', '')[:500]
            
            # Construct post URL from URN if available
            post_url = post_data.get('post_url', '')
            if not post_url and post_id and 'urn:li:activity:' in post_id:
                post_url = f"https://www.linkedin.com/feed/update/{post_id}/"
            
            # Prepare data
            has_email = bool(post_data.get('email'))
            has_phone = bool(post_data.get('phone'))
            is_job_post = bool(post_data.get('has_job'))
            is_ai_related = bool(post_data.get('is_relevant'))
            
            # UPSERT logic (DuckDB 0.7.0+)
            con.execute("""
                INSERT OR REPLACE INTO posts 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post_id, post_url, keyword, post_data.get('name', ''),
                post_preview, post_data.get('profile_url', ''),
                has_email, has_phone, is_job_post, is_ai_related,
                datetime.now()
            ))
            
            con.close()
            return True
        except Exception as e:
            print(f"      [ERROR saving metadata to DuckDB]: {e}")
            return False
    
    def is_chrome_running_with_profile(self):
        """Check if Chrome is already running with the configured profile."""
        if not config.CHROME_PROFILE_PATH:
            return False
            
        print(f"Checking if Chrome is already using profile: {config.CHROME_PROFILE_NAME}...")
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
            print(f"      [Warning] Error checking for running Chrome: {e}")
            
        return False

    def init_driver(self):
        print("Initializing Undetected Chrome...")
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        
        # Load existing Chrome profile if configured
        if config.CHROME_PROFILE_PATH:
            if self.is_chrome_running_with_profile():
                print("\n" + "!"*80)
                print("ERROR: Chrome is already running with the selected profile.")
                print(f"Profile Path: {config.CHROME_PROFILE_PATH}")
                print("Please close all Chrome windows using this profile before running the bot.")
                print("!"*80 + "\n")
                import sys
                sys.exit(1)

            print(f"  Using Chrome profile: {config.CHROME_PROFILE_NAME}")
            chrome_options.add_argument(f"--user-data-dir={config.CHROME_PROFILE_PATH}")
            chrome_options.add_argument(f"--profile-directory={config.CHROME_PROFILE_NAME}")
        
        # Proxy Support
        if config.PROXY_URL:
            print(f"  Using Proxy: {config.PROXY_URL}")
            chrome_options.add_argument(f'--proxy-server={config.PROXY_URL}')
        
        # undetected_chromedriver handles its own driver management
        try:
            self.driver = uc.Chrome(options=chrome_options)
            
            # Apply selenium-stealth to further mask automation signals
            stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            
            self.scraper = ScraperModule(self.driver)
            print("Chrome ready!")
        except Exception as e:
            if "user data directory is already in use" in str(e).lower():
                print("\n" + "!"*80)
                print("ERROR: Chrome User Data directory is currently in use.")
                print("Even though the process check passed, individual files may still be locked.")
                print("Please ensure ALL Chrome processes are closed.")
                print("!"*80 + "\n")
                import sys
                sys.exit(1)
            raise e
        
    def login(self):
        """Login."""
        print("Logging in...")
        self.driver.get("https://www.linkedin.com/login")
        time.sleep(2)
        
        self.driver.find_element(By.ID, config.SELECTORS['login']['username']).send_keys(self.linkedin_email)
        time.sleep(1)
        self.driver.find_element(By.ID, config.SELECTORS['login']['password']).send_keys(self.linkedin_password)
        time.sleep(1)
        self.driver.find_element(By.ID, config.SELECTORS['login']['password']).send_keys(Keys.RETURN)
        time.sleep(5)
        print("Logged in!")
        
    
    
    def save_contact(self, data, keyword):
        """Save to CSV immediately."""
        if config.DRY_RUN:
            print(f"      [Dry Run] skipping CSV and Sync for: {data.get('full_name')}")
            self.total_saved += 1
            return True
            
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
        if not self.scraper.search_posts(keyword):
            return 0
        
        posts = self.scraper.get_posts(processed_posts=self.processed_posts)
        if not posts:
            print(f"[Keyword: {keyword}] No posts found.")
            return 0
        
        print(f"[Keyword: {keyword}] Processing {len(posts)} posts...")
        found = 0
        posts_processed = 0
        
        for post in posts:
            # Check if we've reached the run limit
            if self.total_saved >= config.MAX_CONTACTS_PER_RUN:
                print(f"[Keyword: {keyword}] Stop: Reached MAX_CONTACTS_PER_RUN ({config.MAX_CONTACTS_PER_RUN}).")
                break
                
            # Extract post ID first
            post_id = self.scraper.extract_post_id(post)
            
            # Skip if we've already processed this post
            if post_id and post_id in self.processed_posts:
                continue
            
            # Extract post data
            try:
                post_data = self.scraper.extract_post_data(post, get_full_html=True)
                self.total_seen += 1
                if post_data.get('is_relevant'):
                    self.total_relevant += 1
            except StaleElementReferenceException:
                print(f"[Keyword: {keyword}] [Post: {post_id[:8]}] Element went stale. Attempting recovery...")
                fresh_post = self.scraper.find_post_by_id(post_id)
                if fresh_post:
                    post_data = self.scraper.extract_post_data(fresh_post, get_full_html=True)
                else:
                    print(f"[Keyword: {keyword}] [Post: {post_id[:8]}] Recovery failed. Skipping.")
                    continue
            
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
                    print(f"[Keyword: {keyword}] [Post: {post_id[:8]}] Cache Match: Re-extracting from {cached['full_name']}")
                    
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
                    print(f"[Keyword: {keyword}] [Post: {post_id[:8]}] New relevant post - Extracting full profile...")
                    profile_data = self.scraper.extract_full_profile_data(post_data['profile_url'])
        
                    best_email = profile_data['email'] or post_data['email']
                    
                    if best_email:
                        found += 1
                        print(f"[Keyword: {keyword}] [Post: {post_id[:8]}] New Contact: {profile_data['full_name'] or post_data['name']}")
                        
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
                        print(f"[Keyword: {keyword}] [Post: {post_id[:8]}] Skipped: No email found for {profile_data['full_name'] or post_data['name']}")
                        self.processed_profiles.add(normalized_url)
            else:
                if not normalized_url: pass
                elif normalized_url in self.processed_profiles: pass
                elif not is_relevant:
                    reasons = []
                    if not post_data['has_job']: reasons.append("No job keywords")
                    if not post_data['is_relevant']: reasons.append("Not AI related")
                    print(f"[Keyword: {keyword}] [Post: {post_id[:8]}] Skip: {', '.join(reasons)}")
            
            # Save ALL posts (relevant or not) with best available metadata
            if post_id:
                status_msg = " (With Contact Info)" if is_extracted else ""
                print(f"[Keyword: {keyword}] [Post: {post_id[:30]}] Saving post metadata{status_msg}...")
                
                if self.save_full_post(post_data['post_text'], post_id, keyword, metadata=current_meta):
                    pass

                # Save metadata to CSV
                self.save_post_metadata(post_data, keyword, post_id)
                
                # Mark as processed
                self.save_processed_post_id(post_id)
                self.posts_saved += 1
                posts_processed += 1

            time.sleep(random.uniform(1.5, 4.0))
        
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
            
            # Check if we are already logged in via profile
            print("Checking session...")
            self.driver.get("https://www.linkedin.com/feed/")
            time.sleep(random.uniform(4.0, 6.5))
            
            if "login" in self.driver.current_url or "checkpoint" in self.driver.current_url:
                print("Session not found or expired. Logging in...")
                self.login()
            else:
                print("Active session detected! Skipping login.")
            
            # CRITICAL: Validate UI before proceeding
            if not self.scraper.validate_selectors():
                print("Aborting: UI does not match expected selectors.")
                return

            for idx, keyword in enumerate(self.keywords, 1):
                if self.total_saved >= config.MAX_CONTACTS_PER_RUN:
                    print(f"\n[Limit Reached] Already saved {self.total_saved} contacts. Skipping remaining keywords.")
                    break
                    
                print(f"\n[Keyword {idx}/{len(self.keywords)}]")
                self.process_keyword(keyword)
                
                if idx < len(self.keywords):
                    time.sleep(random.uniform(2.5, 5.0))
            
            # Perform bulk sync of extracting contacts to WBL backend
            if self.extracted_contacts_buffer:
                print(f"\nBulk syncing {len(self.extracted_contacts_buffer)} contacts to WBL backend...")
                success = self.activity_logger.bulk_save_vendor_contacts(
                    self.extracted_contacts_buffer, 
                    source_email=self.linkedin_email
                )
                if success:
                    self.total_synced += len(self.extracted_contacts_buffer)
            
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
                success = self.activity_logger.bulk_save_vendor_contacts(
                    self.extracted_contacts_buffer, 
                    source_email=self.linkedin_email
                )
                if success:
                    self.total_synced += len(self.extracted_contacts_buffer)
            
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
                        
                if config.DRY_RUN:
                    print(f"\n[Dry Run] Skipping activity log sync to WBL Backend.")
                    return

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
                print("\n" + "="*60)
                print("RUN COMPLETE - SESSION METRICS:")
                print(f" - Total Posts Seen:     {self.total_seen}")
                print(f" - AI Relevant Posts:   {self.total_relevant}")
                print(f" - Contacts Found:       {self.total_saved}")
                print(f" - Contacts Synced:      {self.total_synced}")
                print(f" - Posts Saved to Disk: {self.posts_saved}")
                print("="*60)
                
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
                            wait_time = random.randint(30, 60)
                            print(f"Waiting {wait_time} seconds before next candidate...")
                            time.sleep(wait_time)
                            
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
