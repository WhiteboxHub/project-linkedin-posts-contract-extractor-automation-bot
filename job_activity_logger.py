
import requests
from datetime import date
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()


class JobActivityLogger:    
    
    def __init__(self):
        self.api_url = os.getenv('WBL_API_URL', '')
        self.api_token = os.getenv('WBL_API_TOKEN', '')
        self.job_unique_id = os.getenv('JOB_UNIQUE_ID', 'bot_linkedin_post_contact_extractor')
        self.employee_id = int(os.getenv('EMPLOYEE_ID', '353'))
        self.selected_candidate_id = int(os.getenv('SELECTED_CANDIDATE_ID', '0'))
        
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        
        
        if not self.api_token:
            print("  WARNING: WBL_API_TOKEN not set. Activity logging will fail.")

    def _extract_linkedin_id(self, url: str) -> str:

        if not url:
            return ""
        clean_url = url.strip().rstrip('/')
        if '/in/' in clean_url:
            return clean_url.split('/in/')[-1].split('?')[0]
        return clean_url

    def save_vendor_contact(self, data: dict) -> bool:
 
        if not self.api_token:
            return False
        if '/api' in self.api_url:
            base_url = self.api_url.rstrip('/')
        else:
            base_url = f"{self.api_url.rstrip('/')}/api"
            
        endpoint = f"{base_url}/vendor_contact"
        linkedin_id = self._extract_linkedin_id(data.get('linkedin_id'))
        
        payload = {
            "full_name": data.get('full_name') or 'Unknown',
            "email": data.get('email'),
            "phone": data.get('phone'),
            "linkedin_id": linkedin_id,
            "company_name": data.get('company_name'),
            "location": data.get('location'),
            "source_email": os.getenv('LINKEDIN_EMAIL')
        }
        
        try:
            
            if not payload["source_email"] or "@" not in payload["source_email"]:
                payload["source_email"] = None
                
            response = requests.post(endpoint, json=payload, headers=self.headers)
            response.raise_for_status()
            return True
        except Exception:
           
            return False

    def log_activity(
        self, 
        activity_count: int, 
        notes: str = "", 
        candidate_id: int = 0, 
        activity_date: Optional[str] = None
    ) -> bool:
     
        if not self.api_token:
            print(" Cannot log activity: No API token configured")
            return False
            
        if activity_date is None:
            activity_date = date.today().isoformat()
        
      
        if candidate_id == 0 and self.selected_candidate_id != 0:
            candidate_id = self.selected_candidate_id
            
      
        job_type_id = self._get_job_type_id()
        if job_type_id is None:
            print(" Cannot log activity: Job type not found")
            return False
        
        payload = {
            "job_id": job_type_id,
            "employee_id": self.employee_id,
            "activity_count": activity_count,
            "candidate_id": candidate_id if candidate_id != 0 else None,
            "notes": notes,
            "activity_date": activity_date
        }
        
       
        if '/api' in self.api_url:
            base_url = self.api_url.rstrip('/')
        else:
            base_url = f"{self.api_url.rstrip('/')}/api"
            
        endpoint = f"{base_url}/job_activity_logs"
        
        try:
            response = requests.post(endpoint, json=payload, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            print(f" Activity logged: {activity_count} contacts (Activity ID: {result.get('id', 'N/A')})")
            return True
        except requests.exceptions.RequestException as e:
            print(f" Failed to log activity: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    print(f"   Error details: {error_detail}")
                except:
                    print(f"   Response: {e.response.text}")
            return False
    
    def _get_job_type_id(self) -> Optional[int]:
        try:
           
            if '/api' in self.api_url:
                base_url = self.api_url.rstrip('/')
            else:
                base_url = f"{self.api_url.rstrip('/')}/api"
                
            endpoint = f"{base_url}/job-types"
                
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()
            job_types = response.json()
            
            # Find matching job type
            for job_type in job_types:
                if job_type.get('unique_id') == self.job_unique_id:
                    return job_type.get('id')
            
            print(f"WARNING: Job type '{self.job_unique_id}' not found in database")
            return None
        except Exception as e:
            print(f"WARNING: Could not fetch job type ID: {e}")
            return None


# Convenience function for simple usage
def log_job_activity(count: int, notes: str = "") -> bool:
    logger = JobActivityLogger()
    return logger.log_activity(count, notes)
