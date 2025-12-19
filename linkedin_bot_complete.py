"""Complete LinkedIn Bot - Extracts ALL required fields for database."""
import time
import csv
import re
import os
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
    def __init__(self):
        self.driver = None
        self.processed_profiles = set()
        self.keywords = []
        self.total_saved = 0
        self.activity_logger = JobActivityLogger()  
        
    def load_keywords(self):
        try:
            with open(config.KEYWORDS_FILE, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            self.keywords = lines
            print(f"Loaded {len(self.keywords)} keywords")
            return True
        except FileNotFoundError:
            print(f"[ERROR] Keywords file not found")
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
        
        self.driver.find_element(By.ID, "username").send_keys(config.LINKEDIN_EMAIL)
        time.sleep(1)
        self.driver.find_element(By.ID, "password").send_keys(config.LINKEDIN_PASSWORD)
        time.sleep(1)
        self.driver.find_element(By.ID, "password").send_keys(Keys.RETURN)
        time.sleep(5)
        print("Logged in!")
        
    def search_posts(self, keyword):
        print(f"\n{'='*60}\nKEYWORD: {keyword}\n{'='*60}")
        
        if 'feed' not in self.driver.current_url:
            self.driver.get("https://www.linkedin.com/feed/")
            time.sleep(3)
        
        try:
            search_box = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Search']"))
            )
            search_box.clear()
            time.sleep(1)
            
            for char in keyword:
                search_box.send_keys(char)
                time.sleep(0.05)
            
            time.sleep(2)
            search_box.send_keys(Keys.RETURN)
            time.sleep(5)
            
            current_url = self.driver.current_url
            if '/search/results/all/' in current_url:
                posts_url = current_url.replace('/search/results/all/', '/search/results/content/')
                self.driver.get(posts_url)
                time.sleep(5)
            
            return True
        except:
            return False
    
    def extract_email(self, text):
        if not text:
            return None
            
        patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        ]
        
        # Invalid email patterns to exclude
        invalid_patterns = [
            r'\.png',
            r'\.jpg',
            r'\.jpeg',
            r'\.gif',
            r'\.svg',
            r'@2x\.',
            r'entity-circle',
            r'placeholder',
            r'example\.com',
            r'test\.com',
            r'guruteja234@gmail\.com',
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
      
        time.sleep(5)
        for i in range(5):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        try:
            posts = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'feed-shared-update-v2')]")
            if posts:
                print(f"  Found {len(posts)} posts")
                return posts
        except:
            pass
        return []
    
    def extract_post_data(self, post):
        data = {
            'name': '', 'email': '', 'phone': '', 'post_text': '',
            'profile_url': '', 'company': '', 'location': '',
            'is_relevant': False, 'has_job': False
        }
        
        try:
            try:
                name_elem = post.find_element(By.XPATH, ".//span[@aria-hidden='true']")
                name = name_elem.text.strip()
                if name and 0 < len(name) < 100:
                    data['name'] = name
            except:
                pass
            
            try:
                text_elem = post.find_element(By.XPATH, ".//div[contains(@class, 'update-components-text')]")
                text = text_elem.text.strip()
                if text:
                    data['post_text'] = text.encode('ascii', 'ignore').decode('ascii')
            except:
                pass
            
            
            data['is_relevant'] = self.is_ai_tech_related(data['post_text'])
            data['has_job'] = self.has_job_keywords(data['post_text'])
            
            
            if data['post_text']:
                data['email'] = self.extract_email(data['post_text'])
                data['phone'] = self.extract_phone(data['post_text'])
            
            
            try:
                link = post.find_element(By.XPATH, ".//a[contains(@href, '/in/')]")
                url = link.get_attribute('href')
                if url and '/in/' in url:
                    data['profile_url'] = url.split('?')[0]
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
            with open(config.OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
                fieldnames = [
                    'full_name', 'email', 'phone', 'linkedin_id',
                    'company_name', 'location', 'extraction_date', 'search_keyword'
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
                    'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                    'search_keyword': keyword
                })
            
            # Also save to WBL Backend
            synced = self.activity_logger.save_vendor_contact(data)
            
            self.total_saved += 1
            return synced
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
        
        for post in posts:
            post_data = self.extract_post_data(post)
            
            # Skip duplicate
            if post_data['profile_url'] in self.processed_profiles:
                continue
            
            # Must have job keywords
            if not post_data['has_job']:
                continue
            
            # Must be AI/Tech
            if not post_data['is_relevant']:
                continue
            
            found += 1
            print(f"  [{found}] {post_data['name'] or 'Unknown'}")
            
            # Get full profile data
            if post_data['profile_url']:
                print(f"      Extracting full profile...")
                profile_data = self.extract_full_profile_data(post_data['profile_url'])
    
                best_email = profile_data['email'] or post_data['email']
                
                final_data = {
                    'full_name': profile_data['full_name'] or post_data['name'],
                    'email': best_email,  # ONLY ONE EMAIL - best one
                    'phone': profile_data['phone'] or post_data['phone'],
                    'linkedin_id': profile_data['linkedin_id'],
                    'company_name': profile_data['company_name'],
                    'location': profile_data['location']
                }
                
                
                print(f"      Name: {final_data['full_name']}")
                print(f"      Email: {final_data['email'] or 'NOT FOUND'}")
                print(f"      Phone: {final_data['phone'] or 'NOT FOUND'}")
                print(f"      Company: {final_data['company_name'] or 'N/A'}")
                print(f"      Location: {final_data['location'] or 'N/A'}")
                
               
                if self.save_contact(final_data, keyword):
                    print(f"      [SAVED #{self.total_saved}] ✓ (Synced to Backend)")
                else:
                    print(f"      [SAVED #{self.total_saved}] (Local Only)")
                
                
                self.processed_profiles.add(post_data['profile_url'])
            
            time.sleep(2)
        
        print(f"  Keyword complete: {found} contacts")
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
            
            print("\n" + "=" * 60)
            print(f"COMPLETED! Saved {self.total_saved} contacts")
            print("=" * 60)
            
           
            print("\nLogging activity to WBL backend...")
            self.activity_logger.log_activity(
                activity_count=self.total_saved,
                notes=f"LinkedIn extraction completed. Processed {len(self.keywords)} keywords."
            )
            
        except KeyboardInterrupt:
            print(f"\n\n[STOPPED] Saved: {self.total_saved}")
            
            if self.total_saved > 0:
                print("Logging partial activity...")
                self.activity_logger.log_activity(
                    activity_count=self.total_saved,
                    notes="LinkedIn extraction stopped by user (partial run)"
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
    bot = LinkedInBotComplete()
    bot.run()
