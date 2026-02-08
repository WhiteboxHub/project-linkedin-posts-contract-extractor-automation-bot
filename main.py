"""Complete LinkedIn Bot - Extracts ALL required fields for database."""
import time
import random
import os
import config
from datetime import datetime
from selenium.common.exceptions import StaleElementReferenceException

# New Modular Imports
from modules import ScraperModule, ProcessorModule
from modules.browser_manager import BrowserManager
from modules.storage_manager import StorageManager
from modules.logger import logger
from modules.logger import logger
from job_activity_logger import JobActivityLogger
from modules.processed_post_store import ProcessedPostStore


class LinkedInBotComplete:
    """
    Main Orchestrator class. 
    Controls the bot's workflow, manages driver life-cycle via BrowserManager, 
    handles storage via StorageManager, and coordinates between ScraperModule and ProcessorModule.
    """
    def __init__(self, email=None, password=None, candidate_id=None, keywords=None, chrome_profile=None):
        self.linkedin_email = email or config.LINKEDIN_EMAIL
        self.linkedin_password = password or config.LINKEDIN_PASSWORD
        self.candidate_id = candidate_id
        
        # Managers
        self.browser_manager = BrowserManager(chrome_profile=chrome_profile)
        self.storage_manager = StorageManager()
        
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
            
        # Initialize metrics
        from modules.metrics_manager import MetricsTracker
        self.metrics = MetricsTracker()
            
        # Initialize modules
        self.processor = ProcessorModule()
        self.processed_store = ProcessedPostStore()
        self.scraper = None  # Initialized after driver in init_driver
        
    def load_keywords(self):
        # If keywords provided in constructor, use them
        if self.keywords:
            logger.info(f"Loaded {len(self.keywords)} keywords from config", extra={"step_name": "Initialization"})
            return True
            
        try:
            import json
            with open(config.KEYWORDS_FILE, 'r', encoding='utf-8') as f:
                self.keywords = json.load(f)
            
            logger.info(f"Loaded {len(self.keywords)} keywords from JSON file", extra={"step_name": "Initialization"})
            return True
        except FileNotFoundError:
            logger.error("Keywords file not found", extra={"step_name": "Initialization"})
            return False
        except Exception as e:
            logger.error(f"Error loading keywords from JSON: {e}", extra={"step_name": "Initialization"}, exc_info=True)
            return False
            
    def init_driver(self):
        self.browser_manager.init_driver()
        self.scraper = ScraperModule(self.browser_manager, metrics=self.metrics)
        
    def login(self):
        self.browser_manager.login(self.linkedin_email, self.linkedin_password)
    
    def process_keyword(self, keyword):
        """Process keyword."""
        if not self.scraper.search_posts(keyword):
            return 0
        
        posts = self.scraper.get_posts(processed_posts=self.storage_manager.processed_posts)
        if not posts:
            logger.info(f"No posts found.", extra={"step_name": "Keyword Processing", "keyword": keyword})
            return 0
        
        logger.info(f"Processing {len(posts)} posts...", extra={"step_name": "Keyword Processing", "keyword": keyword})
        found = 0
        posts_processed = 0
        
        for post in posts:
            # Check if we've reached the run limit
            if self.total_saved >= config.MAX_CONTACTS_PER_RUN:
                logger.info(f"Stop: Reached MAX_CONTACTS_PER_RUN ({config.MAX_CONTACTS_PER_RUN}).", extra={"step_name": "Keyword Processing"})
                break
            
            self.metrics.increment('posts_seen')
            
            # Extract post ID first
            post_id = self.scraper.extract_post_id(post)
            
            # Skip if we've already processed this post
            if post_id and self.processed_store.is_processed(post_id):
                self.metrics.track_skip("Already Processed")
                continue
            
            self.metrics.increment('posts_attempted')

            # Extract post data
            try:
                post_data = self.scraper.extract_post_data(post, get_full_html=True)
                self.total_seen += 1
                if post_data.get('is_relevant'):
                    self.total_relevant += 1
            except StaleElementReferenceException:
                logger.warning("Element went stale. Attempting recovery...", extra={"step_name": "Post Extraction", "post_id": post_id, "keyword": keyword})
                fresh_post = self.scraper.find_post_by_id(post_id)
                if fresh_post:
                    post_data = self.scraper.extract_post_data(fresh_post, get_full_html=True)
                else:
                    logger.warning("Recovery failed. Skipping.", extra={"step_name": "Post Extraction", "post_id": post_id, "keyword": keyword})
                    self.metrics.track_failure("Stale Element Recovery Failed")
                    continue
            except Exception as e:
                logger.error(f"Extraction failed: {e}", extra={"step_name": "Post Extraction"}, exc_info=True)
                self.metrics.track_failure("Extraction Exception")
                continue
            
            # Construct post URL
            post_url = ""
            if post_id and 'urn:li:activity:' in post_id:
                post_url = f"{config.URLS['POST_BASE']}{post_id}/"
            post_data['post_url'] = post_url
            
            if not post_data.get('name'):
                 self.metrics.track_failure("Name Extraction Failed")
                 # We still try to save what we have or skip? 
                 # Current logic continues, but maybe we should count it as partial?
                 # Let's continue for now.

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
            
            # [DECOUPLED FLOW]
            # We ONLY check for basic relevance (AI/Tech keywords).
            # We do NOT check for email/phone here anymore.
            # We save the raw post and move on.
            
            # Check relevance for full extraction
            is_relevant = post_data['is_relevant']
            
            if is_relevant:
                is_extracted = True 
                found += 1
                self.metrics.increment('posts_extracted')
                self.total_saved += 1
            else:
                 reasons = []
                 if not post_data['is_relevant']: reasons.append("Not matched keywords")
                 logger.info(f"Skip: {', '.join(reasons)}", extra={"step_name": "Relevance Check", "post_id": post_id, "keyword": keyword})
                 self.metrics.track_skip(f"Irrelevant ({', '.join(reasons)})")
            
            # Save ALL posts that match keywords with best available metadata
            if post_id and is_relevant:
                # Save full post content for later rule-based extraction
                full_text = f"{post_data.get('author_headline', '')}\n\n{post_data['post_text']}"
                self.storage_manager.save_full_post(full_text, post_id, keyword, metadata=post_data)
                
                # Save metadata to CSV (raw)
                self.storage_manager.save_post_metadata(post_data, keyword, post_id)
                
                # Mark as processed using crash-safe store
                self.processed_store.add(post_id)
                self.posts_saved += 1
                posts_processed += 1
            else:
                pass # Already skipped logging above

            time.sleep(random.uniform(1.5, 8.0))
        
        logger.info(f"Keyword complete: {posts_processed} posts saved, {found} contacts extracted", extra={"step_name": "Keyword Processing", "keyword": keyword})
        return found
    
    def run(self):
        """Run bot."""
        try:
            logger.info("LinkedIn Complete Data Extractor (Modularized) Started", extra={"step_name": "Startup"})
            logger.info(f"Extracts: Name, Email, Phone, Company, Location. Output: {config.OUTPUT_FILE}", extra={"step_name": "Startup"})
            self.metrics.start_session()
            
            cand_id = getattr(self.activity_logger, 'selected_candidate_id', 0)
            if cand_id != 0:
                logger.info(f"Logging activity for Candidate ID: {cand_id}", extra={"step_name": "Startup"})
            
            if not self.load_keywords():
                return
            
            self.init_driver()
            
            # Check if we are already logged in via profile
            logger.info("Checking session...", extra={"step_name": "Startup"})
            self.browser_manager.navigate(config.URLS['FEED'])
            if not self.browser_manager.login(self.linkedin_email, self.linkedin_password):
                logger.critical("Login failed!", extra={"step_name": "Login"})
                return

            # CRITICAL: Validate UI before proceeding
            if not self.scraper.validate_selectors():
                logger.critical("Aborting: UI does not match expected selectors.", extra={"step_name": "Startup"})
                return

            for idx, keyword in enumerate(self.keywords, 1):
                if self.total_saved >= config.MAX_CONTACTS_PER_RUN:
                    logger.info(f"Stop: Reached MAX_CONTACTS_PER_RUN ({config.MAX_CONTACTS_PER_RUN}).", extra={"step_name": "Keyword Processing"})
                    break
                
                logger.info(f"Starting Keyword {idx}/{len(self.keywords)}: {keyword}", extra={"step_name": "Orchestrator"})
                self.process_keyword(keyword)
                
                # Random delay between keywords
                if idx < len(self.keywords):
                    sleep_time = random.uniform(10, 20)
                    logger.info(f"Sleeping {sleep_time:.1f}s before next keyword...", extra={"step_name": "Orchestrator"})
                    time.sleep(sleep_time)
            
            # Perform bulk sync of extracting contacts to WBL Backend
            if self.storage_manager.extracted_contacts_buffer:
                logger.info(f"Bulk syncing {len(self.storage_manager.extracted_contacts_buffer)} contacts to WBL backend...", extra={"step_name": "Shutdown"})
                success = self.activity_logger.bulk_save_vendor_contacts(
                    self.storage_manager.extracted_contacts_buffer, 
                    source_email=self.linkedin_email
                )
                if success:
                    self.total_synced += len(self.storage_manager.extracted_contacts_buffer)
            
            logger.info("RUN COMPLETE", extra={"step_name": "Shutdown"})
            logger.info(f"Metrics: {self.posts_saved} posts saved, {self.total_saved} contacts extracted", extra={"step_name": "Shutdown"})
            logger.info(f"Storage: {self.storage_manager.posts_dir}/, {config.OUTPUT_FILE}", extra={"step_name": "Shutdown"})

            # Log activity with contact details in notes
            logger.info("Logging activity to WBL backend...", extra={"step_name": "Shutdown"})
            
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
                    logger.warning(f"Could not read CSV for notes: {read_err}", extra={"step_name": "Shutdown"})
                    notes += f"Summary: {self.total_saved} contacts saved to {config.OUTPUT_FILE}"

            self.activity_logger.log_activity(
                activity_count=self.total_saved,
                notes=notes
            )
        
        except KeyboardInterrupt:
            logger.warning(f"STOPPED by user. Saved: {self.total_saved}", extra={"step_name": "Shutdown"})
            
            # Perform bulk sync for contacts collected before interruption
            if self.storage_manager.extracted_contacts_buffer:
                logger.info(f"Syncing {len(self.storage_manager.extracted_contacts_buffer)} contacts to backend before exit...", extra={"step_name": "Shutdown"})
                success = self.activity_logger.bulk_save_vendor_contacts(
                    self.storage_manager.extracted_contacts_buffer, 
                    source_email=self.linkedin_email
                )
                if success:
                    self.total_synced += len(self.storage_manager.extracted_contacts_buffer)
            
            if self.total_saved > 0:
                logger.info("Logging partial activity...", extra={"step_name": "Shutdown"})
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
                    logger.info("Dry Run skipping activity log sync to WBL Backend.", extra={"step_name": "Shutdown"})
                    return

                self.activity_logger.log_activity(
                    activity_count=self.total_saved,
                    notes=notes
                )
                
        except Exception as e:
            logger.critical(f"FATAL ERROR: {e}", extra={"step_name": "Orchestrator"}, exc_info=True)
        
        finally:
            self.browser_manager.quit()
            
            logger.info("SESSION METRICS:", extra={"step_name": "Shutdown"})
            logger.info(f" - Total Posts Seen:     {self.total_seen}", extra={"step_name": "Shutdown"})
            logger.info(f" - AI Relevant Posts:   {self.total_relevant}", extra={"step_name": "Shutdown"})
            logger.info(f" - Contacts Found:       {self.total_saved}", extra={"step_name": "Shutdown"})
            logger.info(f" - Contacts Synced:      {self.total_synced}", extra={"step_name": "Shutdown"})
            logger.info(f" - Posts Saved to Disk: {self.posts_saved}", extra={"step_name": "Shutdown"})
            
            # Print detailed metrics summary
            self.metrics.end_session()
            self.metrics.print_summary()
            
            # [NEW] Run Post-Processing Data Extraction
            try:
                from modules.data_extractor import DataExtractor
                logger.info("Browser closed. Starting offline data extraction...", extra={"step_name": "Shutdown"})
                extractor = DataExtractor()
                extractor.run()
            except Exception as e:
                logger.error(f"Post-processing failed: {e}", extra={"step_name": "Shutdown"})


if __name__ == "__main__":
    import json
    
    # Check for candidates.json
    candidates_file = "candidates.json"
    
    if os.path.exists(candidates_file):
        logger.info(f"Found {candidates_file}. running multi-candidate mode...", extra={"step_name": "Main"})
        try:
            with open(candidates_file, 'r') as f:
                candidates = json.load(f)
            
            if not candidates:
                logger.warning("candidates.json exists but is empty. Falling back to .env settings...", extra={"step_name": "Main"})
                # Fall through to single user block
            else:
                for i, cand in enumerate(candidates, 1):
                    try:
                        logger.info(f"PROCESSING CANDIDATE {i}/{len(candidates)}", extra={"step_name": "Main"})
                        logger.info(f"Email: {cand.get('linkedin_email')}", extra={"step_name": "Main"})
                        logger.info(f"Candidate ID: {cand.get('candidate_id', 'Not Set')}", extra={"step_name": "Main"})
                        
                        if not cand.get('linkedin_email') or not cand.get('linkedin_password'):
                            logger.error("Skipping - missing credentials", extra={"step_name": "Main"})
                            continue
                            
                        bot = LinkedInBotComplete(
                            email=cand.get('linkedin_email'),
                            password=cand.get('linkedin_password'),
                            candidate_id=cand.get('candidate_id'),
                            keywords=cand.get('keywords', []),
                            chrome_profile=cand.get('chrome_profile')
                        )
                        bot.run()
                        
                        # [NEW] Run Post-Processing Data Extraction
                        try:
                            from modules.data_extractor import DataExtractor
                            logger.info("Browser closed. Starting offline data extraction...", extra={"step_name": "Shutdown"})
                            extractor = DataExtractor()
                            extractor.run()
                        except Exception as e:
                            logger.error(f"Post-processing failed: {e}", extra={"step_name": "Shutdown"}, exc_info=True)
                        
                        # Cool down between candidates
                        if i < len(candidates):
                            wait_time = random.randint(30, 60)
                            logger.info(f"Waiting {wait_time} seconds before next candidate...", extra={"step_name": "Main"})
                            time.sleep(wait_time)
                            
                    except Exception as e:
                        logger.error(f"Error processing candidate {i}: {e}", extra={"step_name": "Main"}, exc_info=True)
                        continue
                
                logger.info("All candidates processed.", extra={"step_name": "Main"})
                exit(0)

        except Exception as e:
            logger.error(f"Error reading config: {e}", extra={"step_name": "Main"}, exc_info=True)
            logger.info("Falling back to .env settings...", extra={"step_name": "Main"})

    else:
        logger.info("No candidates.json found. Running in single-user mode using .env settings.", extra={"step_name": "Main"})

    # Fallback to single user .env mode
    bot = LinkedInBotComplete()
    bot.run()
