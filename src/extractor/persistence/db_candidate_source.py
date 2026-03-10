import logging
from typing import List, Dict, Any, Optional
from src.extractor.core.api_client import get_api_client

logger = logging.getLogger(__name__)

class APICandidateSource:
    """
    Source for fetching candidates via the WBL API proxy.
    """
    def __init__(self, workflow_id: int, sql_query: str):
        self.workflow_id = workflow_id
        self.sql_query = sql_query
        self.api_client = get_api_client()

    def get_active_candidates(self, candidate_id: Optional[int] = None, candidate_email: Optional[str] = None) -> list[dict[str, Any]]:
        """
        Fetch active candidates from the database via API proxy.
        """
        # We still use the same subquery logic, but we pass it to the API
        filtered_query = f"SELECT * FROM ({self.sql_query}) AS candidates WHERE 1=1"
        params = {}

        if candidate_id is not None:
            filtered_query += " AND candidate_id = :candidate_id"
            params["candidate_id"] = candidate_id
        
        if candidate_email:
            filtered_query += " AND email = :candidate_email"
            params["candidate_email"] = candidate_email

        logger.info(f"Fetching candidates via API using SQL query: {self.sql_query[:100]}...")
        payload = {
            "sql_query": filtered_query,
            "parameters": params
        }
        endpoint = f"/orchestrator/workflows/{self.workflow_id}/execute-recipient-sql"
        
        try:
            results = self.api_client.post(endpoint, payload)
            return results or []
        except Exception as e:
            logger.error(f"Failed to fetch candidates via API: {e}")
            return []
