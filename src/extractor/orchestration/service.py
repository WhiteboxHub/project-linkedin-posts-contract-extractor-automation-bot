import logging
import traceback
import json
from typing import Dict, Any, Optional
import config as bot_config
from main import LinkedInBotComplete
from modules.data_extractor import DataExtractor

logger = logging.getLogger(__name__)

class LinkedInPostsService:
    """
    Orchestration service for LinkedIn Posts Extraction.
    Wraps LinkedInBotComplete and manages workflow-level reporting/tracking.
    """
    def __init__(self, 
                 candidate_source, 
                 workflow_manager, 
                 run_id: str, 
                 workflow_id: int, 
                 runtime_parameters: Dict[str, Any]):
        self.candidate_source = candidate_source
        self.workflow_manager = workflow_manager
        self.run_id = run_id
        self.workflow_id = workflow_id
        self.runtime_parameters = runtime_parameters
        
        # Statistics
        self.records_processed = 0
        self.records_failed = 0
        self.total_seen = 0
        self.total_relevant = 0
        self.total_saved = 0 # This will store contacts found
        self.total_synced = 0 # This will store contacts synced
        self.total_jobs_found = 0
        self.total_jobs_synced = 0

    def run(self, candidate_id: Optional[int] = None, candidate_email: Optional[str] = None):
        """
        Executes the extraction process for all configured candidates.
        """
        try:
            # 1. Fetch Candidates
            candidates = self.candidate_source.get_active_candidates(
                candidate_id=candidate_id,
                candidate_email=candidate_email
            )
            
            if not candidates:
                logger.warning("No active candidates found to process.")
                self.workflow_manager.update_run_status(
                    self.run_id, 'success', 
                    error_summary="No candidates found."
                )
                return

            # 2. Load Keywords for distribution
            all_keywords = []
            try:
                with open(bot_config.KEYWORDS_FILE, 'r', encoding='utf-8') as f:
                    all_keywords = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load keywords from {bot_config.KEYWORDS_FILE}: {e}")
                all_keywords = ["Information Technology"]

            num_keywords = len(all_keywords)
            num_cands = len(candidates)
            logger.info(f"Goal: complete {num_keywords} keywords using {num_cands} available candidates.")

            # Set of emails that failed login (blacklist for this run)
            failed_profiles = set()
            cand_idx = 0

            # 3. Iterate through EVERY keyword and ensure it is attempted
            for k_idx, keyword in enumerate(all_keywords):
                keyword_success = False
                attempts_for_this_keyword = 0
                
                # We try up to 'num_cands' candidates for a single keyword if they keep failing
                while not keyword_success and attempts_for_this_keyword < num_cands:
                    cand = candidates[cand_idx % num_cands]
                    display_email = cand.get('email') or cand.get('linkedin_email')
                    
                    if display_email in failed_profiles:
                        logger.info(f"Skipping profile {display_email} (already failed in this run).")
                        cand_idx += 1
                        attempts_for_this_keyword += 1
                        continue

                    logger.info(f"[{k_idx+1}/{num_keywords}] Keyword: '{keyword}' attempt using Profile: {display_email}")
                    
                    # standardize keys for LinkedInBotComplete
                    email = cand.get('email') or cand.get('linkedin_email')
                    password = cand.get('linkedin_password') or cand.get('password')
                    cand_id = cand.get('candidate_id')
                    chrome_profile = cand.get('chrome_profile') or f"Profile_{cand_id}"
                    
                    if not email or not password:
                        logger.error(f"Candidate {display_email} missing credentials. Trying next profile...")
                        failed_profiles.add(display_email)
                        cand_idx += 1
                        attempts_for_this_keyword += 1
                        continue

                    try:
                        # Execute Bot
                        bot = LinkedInBotComplete(
                            email=email,
                            password=password,
                            candidate_id=cand_id,
                            keywords=[keyword],
                            chrome_profile=chrome_profile
                        )
                        
                        # bot.run() now returns True if it completed successfully
                        success = bot.run()
                        
                        # Update local metrics from Bot Phase (Collection)
                        self.total_seen += bot.total_seen
                        self.total_relevant += bot.total_relevant
                        
                        if success:
                            # Sync data (offline extraction/Phase 2)
                            try:
                                extractor = DataExtractor(candidate_id=cand_id, candidate_email=email)
                                extraction_results = extractor.run()
                                
                                self.total_saved += extraction_results.get('contacts_found', 0)
                                self.total_synced += extraction_results.get('contacts_synced', 0)
                                self.total_jobs_found += extraction_results.get('positions_found', 0)
                                self.total_jobs_synced += extraction_results.get('positions_synced', 0)
                            except Exception as e:
                                logger.error(f"Sync failed for {email}: {e}")
                            
                            self.records_processed += 1
                            keyword_success = True
                            # Move to next profile for next keyword automatically
                            cand_idx += 1
                            
                            # Small cooling period between keywords
                            if k_idx < num_keywords - 1:
                                import time, random
                                wait = random.randint(5, 10)
                                logger.info(f"Waiting {wait}s before next keyword...")
                                time.sleep(wait)
                        else:
                            logger.error(f"Bot failed for profile {display_email}. Blacklisting and trying next available profile for keyword '{keyword}'...")
                            failed_profiles.add(display_email)
                            self.records_failed += 1
                            cand_idx += 1
                            attempts_for_this_keyword += 1

                    except Exception as e:
                        logger.error(f"Error processing keyword '{keyword}' with candidate {display_email}: {e}")
                        logger.info(traceback.format_exc())
                        failed_profiles.add(display_email)
                        self.records_failed += 1
                        cand_idx += 1
                        attempts_for_this_keyword += 1

                if not keyword_success:
                    logger.critical(f"Keyword '{keyword}' failed after trying all available valid candidates.")

            logger.info(f"Finished: keyword processing phase complete.")

            # 3. Final Report/Status Update
            status = 'success'
            if self.records_failed > 0:
                status = 'partial_success' if self.records_processed > 0 else 'failed'

            # Execution metadata for later review
            execution_metadata = {
                "total_seen": self.total_seen,
                "total_relevant": self.total_relevant,
                "total_contacts_found": self.total_saved,
                "total_contacts_synced": self.total_synced,
                "total_jobs_found": self.total_jobs_found,
                "total_jobs_synced": self.total_jobs_synced,
                "candidates_processed": self.records_processed,
                "candidates_failed": self.records_failed
            }
            
            self.workflow_manager.update_run_status(
                self.run_id, 
                status, 
                records_processed=self.records_processed,
                records_failed=self.records_failed,
                execution_metadata=execution_metadata
            )
            
            logger.info(f"Workflow execution finished with status: {status}")

        except Exception as e:
            logger.error(f"Fatal error in service run: {e}")
            self.workflow_manager.update_run_status(
                self.run_id, 'failed',
                error_summary=str(e)[:255],
                error_details=traceback.format_exc()
            )
            raise
