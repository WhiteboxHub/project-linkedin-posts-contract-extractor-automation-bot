import os
import requests
import logging
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

class APIClient:
    def __init__(self):
        self.base_url = os.getenv('WBL_API_URL')
        if not self.base_url:
            # Fallback to local if not set, but usually it should be in .env
            self.base_url = "http://localhost:8000/api"
        
        self.token = os.getenv('WBL_API_TOKEN')
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def get(self, endpoint: str) -> Optional[Any]:
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API GET failed for {url}: {e}")
            return None

    def post(self, endpoint: str, data: Dict[str, Any]) -> Optional[Any]:
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API POST failed for {url}: {e}")
            if response := getattr(e, 'response', None):
                logger.error(f"Status: {response.status_code}, Detail: {response.text}")
            return None

    def put(self, endpoint: str, data: Dict[str, Any]) -> Optional[Any]:
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.put(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"API PUT failed for {url}: {e}")
            return None

def get_api_client():
    return APIClient()
