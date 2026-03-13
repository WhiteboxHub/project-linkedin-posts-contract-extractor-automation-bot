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
        self.all_iteration_results = []

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

            # 3. Iterate through EVERY candidate and EVERY keyword to ensure full coverage
            # We use max(num_keywords, num_cands) to ensure every profile is used AND every keyword is attempted.
            num_iterations = max(num_keywords, num_cands)
            logger.info(f"Will run {num_iterations} iterations to cover both keywords and candidates.")

            # Set of emails that failed login (blacklist for this run)
            failed_profiles = set()

            for i in range(num_iterations):
                keyword = all_keywords[i % num_keywords]
                cand = candidates[i % num_cands]
                
                display_email = cand.get('email') or cand.get('linkedin_email')
                cand_id = cand.get('candidate_id')
                
                if display_email in failed_profiles:
                    logger.info(f"Skipping profile {display_email} (already failed in this run).")
                    continue

                logger.info(f"[{i+1}/{num_iterations}] Iteration - Keyword: '{keyword}', Profile: {display_email} (ID: {cand_id})")
                
                # standardize keys for LinkedInBotComplete
                email = cand.get('email') or cand.get('linkedin_email')
                password = cand.get('linkedin_password') or cand.get('password')
                chrome_profile = cand.get('chrome_profile') or f"Profile_{cand_id}"
                
                if not email or not password:
                    logger.error(f"Candidate {display_email} missing credentials. Skipping...")
                    failed_profiles.add(display_email)
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
                    
                    # bot.run() returns True if it completed successfully
                    success = bot.run()
                    
                    # Update local metrics from Bot Phase (Collection)
                    self.total_seen += bot.total_seen
                    self.total_relevant += bot.total_relevant
                    
                    # Sync data (offline extraction/Phase 2) 
                    # We run this even if success is False to ensure activity logging (Phase 2 logs identified contacts)
                    try:
                        logger.info(f"Starting Data Extraction/Sync for {email} (Phase 2)...")
                        extractor = DataExtractor(candidate_id=cand_id, candidate_email=email)
                        extraction_results = extractor.run()
                        
                        self.total_saved += extraction_results.get('contacts_found', 0)
                        self.total_synced += extraction_results.get('contacts_synced', 0)
                        self.total_jobs_found += extraction_results.get('positions_found', 0)
                        self.total_jobs_synced += extraction_results.get('positions_synced', 0)
                        
                        # Collect results for consolidated report
                        self.all_iteration_results.append({
                            "candidate_id": cand_id,
                            "email": email,
                            "seen": bot.total_seen,
                            "relevant": bot.total_relevant,
                            "saved": extraction_results.get('contacts_found', 0),
                            "synced": extraction_results.get('contacts_synced', 0),
                            "positions_found": extraction_results.get('positions_found', 0),
                            "positions_synced": extraction_results.get('positions_synced', 0),
                            "posts_disk": bot.posts_saved,
                            "keywords": keyword
                        })
                    except Exception as e:
                        logger.error(f"Sync failed for {email}: {e}")
                    
                    if success:
                        self.records_processed += 1
                    else:
                        logger.error(f"Bot failed/incomplete for profile {display_email}.")
                        failed_profiles.add(display_email)
                        self.records_failed += 1

                    # Small cooling period between iterations
                    if i < num_iterations - 1:
                        import time, random
                        wait = random.randint(10, 20)
                        logger.info(f"Waiting {wait}s before next iteration...")
                        time.sleep(wait)

                except Exception as e:
                    logger.error(f"Error processing iteration {i+1} with candidate {display_email}: {e}")
                    logger.info(traceback.format_exc())
                    failed_profiles.add(display_email)
                    self.records_failed += 1

            logger.info(f"Finished: keyword/candidate processing phase complete.")

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

            # Send consolidated email report
            if self.all_iteration_results:
                try:
                    from modules.bot_reporter import ConsolidatedBotReporter
                    logger.info("Sending consolidated run report to email...")
                    reporter = ConsolidatedBotReporter(self.all_iteration_results)
                    reporter.send_consolidated_report()
                except Exception as e:
                    logger.error(f"Failed to send email report: {e}")
            
            logger.info(f"Workflow execution finished with status: {status}")

        except Exception as e:
            logger.error(f"Fatal error in service run: {e}")
            self.workflow_manager.update_run_status(
                self.run_id, 'failed',
                error_summary=str(e)[:255],
                error_details=traceback.format_exc()
            )
            raise
