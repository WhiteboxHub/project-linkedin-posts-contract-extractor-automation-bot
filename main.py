"""Complete LinkedIn Bot - Extracts ALL required fields for database."""
import time
import random
import os
import config
from datetime import datetime
from selenium.common.exceptions import StaleElementReferenceException
from modules import ScraperModule, ProcessorModule
from modules.browser_manager import BrowserManager
from modules.storage_manager import StorageManager
from modules.logger import logger
from job_activity_logger import JobActivityLogger
from modules.processed_post_store import ProcessedPostStore
from modules.metrics_manager import MetricsTracker

class LinkedInBotComplete:
    def __init__(self, email=None, password=None, candidate_id=None, keywords=None, chrome_profile=None):
        self.linkedin_email = email or config.LINKEDIN_EMAIL
        self.linkedin_password = password or config.LINKEDIN_PASSWORD
        self.candidate_id = candidate_id
        
        # Managers
        self.browser_manager = BrowserManager(chrome_profile=chrome_profile)
        self.storage_manager = StorageManager(candidate_id=self.candidate_id, candidate_email=self.linkedin_email)
        
        self.keywords = keywords if keywords else []
        self.total_saved = 0
        self.posts_saved = 0  
        self.total_seen = 0   
        self.total_relevant = 0 
        self.total_synced = 0 
        self.keyword_metrics = {} 
        
        
        self.activity_logger = JobActivityLogger()
        if self.candidate_id:
            self.activity_logger.selected_candidate_id = self.candidate_id
            
        
        
        self.metrics = MetricsTracker()
        self.processor = ProcessorModule()
        self.processed_store = ProcessedPostStore()
        self.scraper = None 
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
        
        
        # Initialize keyword metrics
        self.keyword_metrics[keyword] = {
            'seen': 0,
            'relevant': 0,
            'extracted': 0,
            'saved': 0
        }
        
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
                self.keyword_metrics[keyword]['seen'] += 1
                
                if post_data.get('is_relevant'):
                    self.total_relevant += 1
                    self.keyword_metrics[keyword]['relevant'] += 1
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
            
            
            post_url = ""
            if post_id:
                if 'urn:li:activity:' in post_id:
                    post_url = f"{config.URLS['POST_BASE']}{post_id}/"
                elif post_id.isdigit():
                    post_url = f"{config.URLS['POST_BASE']}urn:li:activity:{post_id}/"
                
            
            if not post_url: 
                 extracted_url = self.scraper.extract_post_url(post)
                 if extracted_url:
                     post_url = extracted_url
            
            post_data['post_url'] = post_url
            
            if not post_data.get('name'):
                 self.metrics.track_failure("Name Extraction Failed")
            current_meta = {
                'full_name': post_data.get('name', ''),
                'email': post_data.get('email', ''),
                'phone': post_data.get('phone', ''),
                'linkedin_id': post_data.get('profile_url', ''), 
                'company_name': post_data.get('company', ''),
                'location': post_data.get('location', ''),
                'post_url': post_url,
                'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                'search_keyword': keyword
            }
            
            is_extracted = False
            
            is_relevant = post_data['is_relevant']
            
            if is_relevant:
                is_extracted = True 
                found += 1
                self.metrics.increment('posts_extracted')
                self.total_saved += 1
                self.keyword_metrics[keyword]['extracted'] += 1
            else:
                 reasons = []
                 if not post_data['is_relevant']: reasons.append("Not matched keywords")
                 logger.info(f"Skip: {', '.join(reasons)}", extra={"step_name": "Relevance Check", "post_id": post_id, "keyword": keyword})
                 self.metrics.track_skip(f"Irrelevant ({', '.join(reasons)})")
            
           
            if post_id and is_relevant:
               
                full_text = f"{post_data.get('author_headline', '')}\n\n{post_data['post_text']}"
                self.storage_manager.save_full_post(full_text, post_id, keyword, metadata=post_data)
                
                
                self.storage_manager.save_post_metadata(post_data, keyword, post_id)
                
                
                self.processed_store.add(post_id)
                self.posts_saved += 1
                posts_processed += 1
                self.keyword_metrics[keyword]['saved'] += 1
            else:
                pass 

            time.sleep(random.uniform(1.5, 8.0))
        
        logger.info(f"Keyword complete: {posts_processed} posts saved, {found} contacts extracted", extra={"step_name": "Keyword Processing", "keyword": keyword})
        return found
    
    def run(self):
        
        try:
            logger.info("LinkedIn Complete Data Extractor (Modularized) Started", extra={"step_name": "Startup"})
            logger.info("Extracts: Name, Email, Phone, Company, Location.", extra={"step_name": "Startup"})
            self.metrics.start_session()
            
            cand_id = getattr(self.activity_logger, 'selected_candidate_id', 0)
            if cand_id != 0:
                logger.info(f"Logging activity for Candidate ID: {cand_id}", extra={"step_name": "Startup"})
            
            if not self.load_keywords():
                return
            
            self.init_driver()
            
            
            logger.info("Checking session...", extra={"step_name": "Startup"})
            self.browser_manager.navigate(config.URLS['FEED'])
            if not self.browser_manager.login(self.linkedin_email, self.linkedin_password):
                logger.critical("Login failed!", extra={"step_name": "Login"})
                return

            
            if not self.scraper.validate_selectors():
                logger.critical("Aborting: UI does not match expected selectors.", extra={"step_name": "Startup"})
                return

            for idx, keyword in enumerate(self.keywords, 1):
                if self.total_saved >= config.MAX_CONTACTS_PER_RUN:
                    logger.info(f"Stop: Reached MAX_CONTACTS_PER_RUN ({config.MAX_CONTACTS_PER_RUN}).", extra={"step_name": "Keyword Processing"})
                    break
                
                logger.info(f"Starting Keyword {idx}/{len(self.keywords)}: {keyword}", extra={"step_name": "Orchestrator"})
                self.process_keyword(keyword)
                
                
                if idx < len(self.keywords):
                    sleep_time = random.uniform(10, 20)
                    logger.info(f"Sleeping {sleep_time:.1f}s before next keyword...", extra={"step_name": "Orchestrator"})
                    time.sleep(sleep_time)
            
            
            logger.info("Collection complete. Post-processing will handle extraction and syncing.", extra={"step_name": "Shutdown"})
            logger.info(f"Metrics: {self.posts_saved} posts saved, {self.total_saved} contacts extracted", extra={"step_name": "Shutdown"})
            logger.info(f"Storage: {self.storage_manager.posts_dir}/", extra={"step_name": "Shutdown"})

           
            logger.info(f"Scan complete. {self.posts_saved} posts cached. Finalizing extraction...", extra={"step_name": "Shutdown"})
            
        except KeyboardInterrupt:
            logger.warning(f"STOPPED by user. Cached: {self.posts_saved}", extra={"step_name": "Shutdown"})
        except Exception as e:
            logger.critical(f"FATAL ERROR: {e}", extra={"step_name": "Orchestrator"}, exc_info=True)
            if self.total_saved > 0:
                notes = f"CRASH: LinkedIn extraction failed: {str(e)}. {self.total_saved} found before crash.\n"
                self.activity_logger.log_activity(self.total_saved, notes=notes)
        
        finally:
            self.browser_manager.quit()
            
            logger.info("SESSION METRICS:", extra={"step_name": "Shutdown"})
            logger.info(f" - Total Posts Seen:     {self.total_seen}", extra={"step_name": "Shutdown"})
            logger.info(f" - AI Relevant Posts:   {self.total_relevant}", extra={"step_name": "Shutdown"})
            logger.info(f" - Contacts Found:       {self.total_saved}", extra={"step_name": "Shutdown"})
            logger.info(f" - Contacts Synced:      {self.total_synced}", extra={"step_name": "Shutdown"})
            logger.info(f" - Posts Saved to Disk: {self.posts_saved}", extra={"step_name": "Shutdown"})
            
            self.metrics.end_session()
            self.metrics.print_summary()

    def send_report(self):
        """Sends the final run report via email."""
        try:
            from modules.bot_reporter import BotReporter
            reporter = BotReporter(self)
            return reporter.send_run_report()
        except Exception as e:
            logger.error(f"Failed to send email report: {e}", extra={"step_name": "Shutdown"}, exc_info=True)
            return False


if __name__ == "__main__":
    import json
    
    
    candidates_file = "candidates.json"
    
    if os.path.exists(candidates_file):
        logger.info(f"Found {candidates_file}. running multi-candidate mode...", extra={"step_name": "Main"})
        try:
            with open(candidates_file, 'r') as f:
                candidates = json.load(f)       
                central_keywords = []
                try:
                    with open(config.KEYWORDS_FILE, 'r', encoding='utf-8') as f:
                        central_keywords = json.load(f)
                except Exception as e:
                    logger.error(f"Failed to load central keywords: {e}", extra={"step_name": "Main"})
                    central_keywords = ["Information Technology"]
                
                # List to collect results for the consolidated report
                all_run_results = []

                for i, cand in enumerate(candidates, 0): 
                    try:
                        logger.info(f"PROCESSING CANDIDATE {i+1}/{len(candidates)}", extra={"step_name": "Main"})
                        logger.info(f"Email: {cand.get('linkedin_email')}", extra={"step_name": "Main"})
                        
                        
                        if not central_keywords:
                             assigned_keywords = ["Information Technology"]
                        else:
                            
                             keyword = central_keywords[i % len(central_keywords)]
                             assigned_keywords = [keyword]
                             
                        logger.info(f"Assigned Keyword: {assigned_keywords[0]}", extra={"step_name": "Main"})
                        
                        if not cand.get('linkedin_email') or not cand.get('linkedin_password'):
                            logger.error("Skipping - missing credentials", extra={"step_name": "Main"})
                            continue
                            
                        bot = LinkedInBotComplete(
                            email=cand.get('linkedin_email'),
                            password=cand.get('linkedin_password'),
                            candidate_id=cand.get('candidate_id'),
                            keywords=assigned_keywords, 
                            chrome_profile=cand.get('chrome_profile')
                        )
                        bot.run()
                        
                        
                        try:
                            from modules.data_extractor import DataExtractor
                            logger.info("Browser closed. Starting offline data extraction...", extra={"step_name": "Shutdown"})
                            extractor = DataExtractor(candidate_id=cand.get('candidate_id'), candidate_email=cand.get('linkedin_email'))
                            synced_count = extractor.run()
                            
                            # Collect metrics for consolidated report
                            all_run_results.append({
                                "candidate_id": cand.get('candidate_id'),
                                "email": cand.get('linkedin_email'),
                                "seen": bot.total_seen,
                                "relevant": bot.total_relevant,
                                "saved": bot.total_saved,
                                "synced": synced_count or 0,
                                "posts_disk": bot.posts_saved
                            })
                            # bot.send_report() # Removed individual reports
                            
                        except Exception as e:
                            logger.error(f"Post-processing failed: {e}", extra={"step_name": "Shutdown"}, exc_info=True)
                        
                       
                        if i < len(candidates):
                            wait_time = random.randint(30, 60)
                            logger.info(f"Waiting {wait_time} seconds before next candidate...", extra={"step_name": "Main"})
                            time.sleep(wait_time)
                            
                    except Exception as e:
                        logger.error(f"Error processing candidate {i}: {e}", extra={"step_name": "Main"}, exc_info=True)
                        continue
                
                
                # Send consolidated report after all candidates are processed
                if all_run_results:
                    try:
                        from modules.bot_reporter import ConsolidatedBotReporter
                        logger.info("Sending consolidated run report...", extra={"step_name": "Shutdown"})
                        reporter = ConsolidatedBotReporter(all_run_results)
                        reporter.send_consolidated_report()
                    except Exception as e:
                        logger.error(f"Failed to send consolidated report: {e}", extra={"step_name": "Shutdown"})

                logger.info("All candidates processed.", extra={"step_name": "Main"})
                exit(0)

        except Exception as e:
            logger.error(f"Error reading config: {e}", extra={"step_name": "Main"}, exc_info=True)
            logger.info("Falling back to .env settings...", extra={"step_name": "Main"})

    else:
        logger.info("No candidates.json found. Running in single-user mode using .env settings.", extra={"step_name": "Main"})

   
    bot = LinkedInBotComplete()
    bot.run()
    
    # Run data extraction and reporting for single-user mode
    try:
        from modules.data_extractor import DataExtractor
        logger.info("Starting offline data extraction...", extra={"step_name": "Shutdown"})
        extractor = DataExtractor()
        synced_count = extractor.run()
        bot.total_synced = synced_count or 0
        
        # Still send a report for single user mode, but use the same logic
        results = [{
            "candidate_id": "Single User",
            "email": bot.linkedin_email,
            "seen": bot.total_seen,
            "relevant": bot.total_relevant,
            "saved": bot.total_saved,
            "synced": bot.total_synced,
            "posts_disk": bot.posts_saved
        }]
        from modules.bot_reporter import ConsolidatedBotReporter
        reporter = ConsolidatedBotReporter(results)
        reporter.send_consolidated_report()
        
    except Exception as e:
        logger.error(f"Post-processing failed: {e}", extra={"step_name": "Shutdown"}, exc_info=True)
