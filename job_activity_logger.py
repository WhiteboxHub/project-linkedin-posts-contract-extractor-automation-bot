
import requests
from datetime import date
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()


class JobActivityLogger:    
    """
    Sole interface for backend communication with the WBL API.
    Handles all HTTP requests, payload normalization, and error handling for synchronization.
    No browser automation or business logic should reside here.
    """
    
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

    def save_vendor_contact(self, data: dict, source_email: str = None) -> bool:
 
        if not self.api_token:
            return False
        if '/api' in self.api_url:
            base_url = self.api_url.rstrip('/')
        else:
            base_url = f"{self.api_url.rstrip('/')}/api"
            
        endpoint = f"{base_url}/vendor_contact"
        linkedin_id = self._extract_linkedin_id(data.get('linkedin_id'))
        
        # Use provided source_email or fallback to env var
        final_source_email = source_email or os.getenv('LINKEDIN_EMAIL')
        
        payload = {
            "full_name": (data.get('full_name') or 'Unknown')[:250],
            "email": data.get('email'),
            "phone": data.get('phone'),
            "linkedin_id": data.get('linkedin_id') or linkedin_id, 
            "linkedin_internal_id": data.get('linkedin_internal_id') or linkedin_id, 
            "company_name": (data.get('company_name') or '')[:250],
            "location": (data.get('location') or '')[:250],
            "source_email": final_source_email,
            "job_source": "LinkedIn Job Post Extractor Bot"
        }
        
        try:
            
            if not payload["source_email"] or "@" not in payload["source_email"]:
                payload["source_email"] = None
                
            response = requests.post(endpoint, json=payload, headers=self.headers)
            if response.status_code == 401:
                print(f"  [ERROR] Sync failed: 401 Unauthorized. Your API token might be expired.")
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            # Log specific exception type and details
            exc_type = type(e).__name__
            error_msg = f"  [ERROR] Sync failed ({exc_type}): {e}"
            
            # Print response body if available for easier debugging
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_detail = e.response.json()
                    print(f"{error_msg}\n   Response Data: {error_detail}")
                except:
                    print(f"{error_msg}\n   Response Text: {e.response.text}")
            else:
                print(error_msg)
            return False
        except Exception as e:
            print(f"  [ERROR] Sync failed (Unexpected): {e}")
            return False

    def bulk_save_vendor_contacts(self, data_list: list, source_email: str = None) -> bool:
        if not self.api_token or not data_list:
            return False
            
        if '/api' in self.api_url:
            base_url = self.api_url.rstrip('/')
        else:
            base_url = f"{self.api_url.rstrip('/')}/api"
            
        endpoint = f"{base_url}/vendor_contact/bulk"
        final_source_email = source_email or os.getenv('LINKEDIN_EMAIL')
        if not final_source_email or "@" not in final_source_email:
            final_source_email = None
            
        contacts_payload = []
        seen_contacts = set() # Track duplicates within the current batch
        
        for data in data_list:
            # Extract and normalize identifiers
            raw_email = data.get('email')
            email = raw_email.strip().lower() if raw_email else None
            
            raw_linkedin_id = data.get('linkedin_id')
            linkedin_id = self._extract_linkedin_id(raw_linkedin_id) if raw_linkedin_id else None
            
            # Skip if we already have this contact in the current payload
            contact_key = (email, linkedin_id)
            if not email and not linkedin_id:
                continue # Skip records with no identifiers
                
            if contact_key in seen_contacts:
                continue
            seen_contacts.add(contact_key)
            
            # Backend expects specific fields. Truncate long strings to avoid 500 errors
            full_name = (data.get('full_name') or 'Unknown')[:250]
            company_name = (data.get('company_name') or '')[:250]
            location = (data.get('location') or '')[:250]
            
            
            internal_id = data.get('linkedin_internal_id')
            if not internal_id and data.get('linkedin_id'):
                 internal_id = self._extract_linkedin_id(data.get('linkedin_id'))

            # 2. Public ID (Full URL) - Prefer author_linkedin_id or raw linkedin_id
            public_id = data.get('author_linkedin_id') or data.get('linkedin_id')
            
            
            
            contacts_payload.append({
                "full_name": full_name,
                "email": email,
                "phone": data.get('phone'),
                "linkedin_id": public_id,           
                "linkedin_internal_id": internal_id, 
                "company_name": company_name,
                "location": location,
                "source_email": final_source_email,
                "job_source": "LinkedIn Job Post Extractor Bot"
            })
            
        if not contacts_payload:
            print("  [INFO] No new/unique contacts to sync.")
            return True
            
        payload = {"contacts": contacts_payload}
        
        try:
            response = requests.post(endpoint, json=payload, headers=self.headers)
            if response.status_code == 401:
                print(f"  [ERROR] Bulk sync failed: 401 Unauthorized.")
            
            if response.status_code != 200:
                print(f"  [ERROR] Bulk sync failed with status {response.status_code}")
                try:
                    # Capture detail from FastAPI error if available
                    error_json = response.json()
                    print(f"  [ERROR] Details: {error_json.get('detail', response.text)}")
                except:
                    print(f"  [ERROR] Details: {response.text}")
            
            response.raise_for_status()
            result = response.json()
            print(f"  [SUCCESS] Bulk sync complete: {result.get('inserted', 0)} inserted, {result.get('duplicates', 0)} duplicates, {result.get('failed', 0)} failed.")
            return True
        except Exception as e:
            if not isinstance(e, requests.exceptions.HTTPError):
                print(f"  [ERROR] Bulk sync failed: {e}")
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
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 401:
                print(f"WARNING: Could not fetch job type ID: 401 Unauthorized. Please refresh your WBL_API_TOKEN in .env")
            else:
                print(f"WARNING: Could not fetch job type ID: {e}")
            return None


# Convenience function for simple usage
def log_job_activity(count: int, notes: str = "") -> bool:
    logger = JobActivityLogger()
    return logger.log_activity(count, notes)
