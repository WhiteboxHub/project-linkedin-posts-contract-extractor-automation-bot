import logging
import traceback
from typing import Dict, Any, Optional
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
        self.total_saved = 0
        self.total_synced = 0

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

            logger.info(f"Processing {len(candidates)} candidates for LinkedIn Posts Extraction.")
            
            # 2. Iterate and process
            for idx, cand in enumerate(candidates, 1):
                try:
                    logger.info(f"[{idx}/{len(candidates)}] Processing Candidate: {cand.get('email')}")
                    
                    # standardize keys for LinkedInBotComplete
                    email = cand.get('email')
                    password = cand.get('linkedin_password', cand.get('password'))
                    cand_id = cand.get('candidate_id')
                    # Default to a unique profile name based on candidate_id if not provided
                    chrome_profile = cand.get('chrome_profile') or f"Profile_{cand_id}"
                    
                    # Keywords determination logic (similar to main.py but can be enhanced via parameters)
                    # For now, let's just use what's in runtime_parameters if available, otherwise keywords.json
                    keywords = self.runtime_parameters.get("keywords")
                    
                    if not email or not password:
                        logger.error(f"Skipping candidate {email}: missing credentials.")
                        self.records_failed += 1
                        continue

                    # Execute Bot
                    bot = LinkedInBotComplete(
                        email=email,
                        password=password,
                        candidate_id=cand_id,
                        keywords=keywords,
                        chrome_profile=chrome_profile
                    )
                    
                    bot.run()
                    
                    # Update local metrics
                    self.total_seen += bot.total_seen
                    self.total_relevant += bot.total_relevant
                    # total_saved in bot is contacts found
                    self.total_saved += bot.total_saved
                    
                    # Sync data (offline extraction)
                    try:
                        extractor = DataExtractor(candidate_id=cand_id, candidate_email=email)
                        extraction_results = extractor.run()
                        self.total_synced += extraction_results.get('contacts_synced', 0)
                    except Exception as e:
                        logger.error(f"Sync failed for {email}: {e}")
                    
                    self.records_processed += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process candidate {cand.get('email')}: {e}")
                    logger.error(traceback.format_exc())
                    self.records_failed += 1

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
