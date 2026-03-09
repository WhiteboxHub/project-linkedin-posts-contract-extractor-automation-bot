import logging
import json
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
from src.extractor.core.api_client import get_api_client

logger = logging.getLogger(__name__)

class WorkflowManager:
    
    def __init__(self):
        self.api_client = get_api_client()
        # To map run_id (UUID) to log_id (primary key) for updates
        self._log_mapping = {}

    def get_workflow_config(self, workflow_key: str) -> Optional[dict[str, Any]]:
        """
        Fetch active workflow configuration by key using API.
        """
        # API endpoint: GET /orchestrator/workflows/key/{key}
        endpoint = f"/orchestrator/workflows/key/{workflow_key}"
        config = self.api_client.get(endpoint)
        
        if not config:
            logger.error(f"Workflow '{workflow_key}' not found or not active via API.")
            return None
            
        # Parse JSON config if it's a string
        if isinstance(config.get('parameters_config'), str):
             try:
                 config['parameters_config'] = json.loads(config['parameters_config'])
             except json.JSONDecodeError:
                 logger.warning(f"Failed to parse parameters_config for workflow {workflow_key}")

        return config

    def start_run(self, workflow_id: int, schedule_id: Optional[int] = None, parameters: Optional[dict] = None) -> str:
        """
        Create a new log entry via API. Returns the run_id (UUID).
        """
        run_id = str(uuid.uuid4())
        started_at = datetime.now().isoformat()
        
        # API endpoint: POST /orchestrator/logs
        payload = {
            "workflow_id": workflow_id,
            "schedule_id": schedule_id,
            "run_id": run_id,
            "status": "running",
            "started_at": started_at,
            "parameters_used": parameters
        }
        
        try:
            result = self.api_client.post("/orchestrator/logs", payload)
            if result and "id" in result:
                # Store log_id (PK) for subsequent updates
                self._log_mapping[run_id] = result["id"]
                logger.info(f"Started workflow run {run_id} (log_id: {result['id']}) via API.")
                return run_id
            else:
                raise Exception("API did not return a valid log ID.")
        except Exception as e:
            logger.error(f"Failed to start workflow run via API: {e}")
            raise

    def update_run_status(self, run_id: str, status: str, 
                          records_processed: int = 0, 
                          records_failed: int = 0,
                          error_summary: Optional[str] = None,
                          error_details: Optional[str] = None,
                          execution_metadata: Optional[dict] = None):
        """
        Update the status of a running workflow via API.
        """
        log_id = self._log_mapping.get(run_id)
        if not log_id:
            # Attempt to find log_id if not in current session memory
            # (In production, this might need a dedicated find_log endpoint)
            logger.warning(f"No log_id (PK) found in memory for run_id {run_id}. Attempting status update via mapping...")
            # For now, we assume log_id is in _log_mapping.
            return

        payload = {
            "status": status,
            "records_processed": records_processed,
            "records_failed": records_failed,
            "error_summary": error_summary,
            "execution_metadata": execution_metadata or {}
        }
        
        if status in ['success', 'failed', 'partial_success', 'timed_out']:
            payload["finished_at"] = datetime.now().isoformat()
            
        # API endpoint: PUT /orchestrator/logs/{log_id}
        try:
            self.api_client.put(f"/orchestrator/logs/{log_id}", payload)
            logger.info(f"Updated run {run_id} status to {status} via API.")
        except Exception as e:
            logger.error(f"Failed to update run status via API for {run_id}: {e}")

    def update_schedule_status(self, schedule_id: int):
        """
        Update the schedule's last run time via API.
        """
        if not schedule_id:
            return
            
        payload = {
            "last_run_at": datetime.now().isoformat(),
            "is_running": 0
        }
        
        # API endpoint: PUT /orchestrator/schedules/{schedule_id}
        try:
            self.api_client.put(f"/orchestrator/schedules/{schedule_id}", payload)
            logger.info(f"Updated schedule {schedule_id} status via API.")
        except Exception as e:
            logger.error(f"Failed to update schedule {schedule_id} via API: {e}")

    def execute_sql(self, workflow_id: int, sql_query: str, parameters: dict[str, Any] = {}) -> list[dict[str, Any]]:
        """
        Execute a SELECT query via the API proxy.
        """
        payload = {
            "sql_query": sql_query,
            "parameters": parameters
        }
        endpoint = f"/orchestrator/workflows/{workflow_id}/execute-recipient-sql"
        results = self.api_client.post(endpoint, payload)
        return results or []
