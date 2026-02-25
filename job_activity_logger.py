
import requests
from datetime import date, datetime
from typing import Optional
import os
import base64
import json
import pandas as pd
from dotenv import load_dotenv
from modules.email_validator import EmailListValidator

load_dotenv()


class JobActivityLogger:      
    def __init__(self):
        self.api_url = os.getenv('WBL_API_URL', '')
        self.api_token = os.getenv('WBL_API_TOKEN', '')
        self.api_email = os.getenv('WBL_API_EMAIL', '')
        self.api_password = os.getenv('WBL_API_PASSWORD', '')
        self.job_unique_id = os.getenv('JOB_UNIQUE_ID', 'bot_linkedin_post_contact_extractor')
        self.employee_id = int(os.getenv('EMPLOYEE_ID', '353'))
        self.selected_candidate_id = int(os.getenv('SELECTED_CANDIDATE_ID', '0'))
        
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        
        if not self.api_token:
            print("  WARNING: WBL_API_TOKEN not set. Activity logging will fail.")

    def _is_token_expired(self, buffer_seconds=300) -> bool:
        """Decode JWT locally and check if it's expired (or within buffer_seconds of expiry)."""
        try:
            if not self.api_token:
                return True
            parts = self.api_token.split('.')
            if len(parts) != 3:
                return True
            payload_b64 = parts[1]
            # Pad base64 string
            payload_b64 += '=' * (-len(payload_b64) % 4)
            payload = json.loads(base64.b64decode(payload_b64))
            exp = payload.get('exp')
            if not exp:
                return True
            return datetime.utcnow().timestamp() >= (exp - buffer_seconds)
        except Exception:
            return True  # Assume expired if we can't decode

    def _refresh_token(self) -> bool:
        """Login to WBL API and update the token in memory and in .env file."""
        if not self.api_email or not self.api_password:
            print("  [TOKEN] Cannot auto-refresh: WBL_API_EMAIL or WBL_API_PASSWORD not set in .env")
            return False

        if '/api' in self.api_url:
            base_url = self.api_url.rstrip('/')
        else:
            base_url = f"{self.api_url.rstrip('/')}/api"

        login_url = f"{base_url}/login"
        try:
            print(f"  [TOKEN] Refreshing WBL API token...")
            response = requests.post(
                login_url,
                data={'username': self.api_email, 'password': self.api_password},
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=15
            )
            if response.status_code != 200:
                print(f"  [TOKEN] Refresh failed: {response.status_code} - {response.text[:200]}")
                return False

            data = response.json()
            new_token = data.get('access_token')
            if not new_token:
                print(f"  [TOKEN] Refresh failed: no access_token in response")
                return False

            # Update in memory
            self.api_token = new_token
            self.headers['Authorization'] = f"Bearer {new_token}"

            # Update .env file on disk so next run uses the fresh token
            env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
            if os.path.exists(env_path):
                with open(env_path, 'r') as f:
                    lines = f.readlines()
                with open(env_path, 'w') as f:
                    for line in lines:
                        if line.startswith('WBL_API_TOKEN='):
                            f.write(f'WBL_API_TOKEN={new_token}\n')
                        else:
                            f.write(line)

            print(f"  [TOKEN] Token refreshed successfully. New expiry saved to .env.")
            return True

        except Exception as e:
            print(f"  [TOKEN] Refresh error: {e}")
            return False

    def _ensure_valid_token(self):
        """Check token expiry and auto-refresh if needed before making an API call."""
        if self._is_token_expired():
            print("  [TOKEN] Token is expired or expiring soon. Attempting auto-refresh...")
            self._refresh_token()


    def _extract_linkedin_id(self, url: str) -> str:

        if not url:
            return ""
        clean_url = url.strip().rstrip('/')
        if '/in/' in clean_url:
            return clean_url.split('/in/')[-1].split('?')[0]
        return clean_url
    def bulk_save_automation_contacts(self, data_list: list, source_type: str = None) -> bool:
        self._ensure_valid_token()
        if not self.api_token or not data_list:
            return False
        effective_source_type = source_type or self.job_unique_id

        if '/api' in self.api_url:
            base_url = self.api_url.rstrip('/')
        else:
            base_url = f"{self.api_url.rstrip('/')}/api"
            
        endpoint = f"{base_url}/automation-extracts/bulk"
        
        # Validate emails before processing
        print(f"  [VALIDATION] Validating {len(data_list)} contacts...")
        validator = EmailListValidator(data_list=data_list)
        # We run syntax and MX checks. SMTP check can be very slow, so we check if configured.
        # For now, let's run the full pipeline as per user's script logic.
        validator.normalize_emails('email')
        validator.validate_syntax('email')
        validator.validate_mx('email', max_workers=20)
        validator.validate_mailbox('email', max_workers=20) 
        
        valid_df = validator.df[
            (validator.df['syntax_valid']) & 
            (validator.df['mx_valid']) & 
            (validator.df['mailbox_status'].isin(['valid', 'unknown']))
        ]
        
        # Replace NaN/Nat with None for JSON compliance
        valid_data_list = valid_df.where(pd.notnull(valid_df), None).to_dict('records')
        
        print(f"  [VALIDATION] {len(valid_data_list)}/{len(data_list)} contacts passed validation.")
        
        extracts_payload = []
        seen_emails = set()
        
        for data in valid_data_list:
            email = data.get('email', '').strip().lower()
            if not email:
                continue
            
            if email in seen_emails:
                continue
            seen_emails.add(email)
            extracts_payload.append({
                "full_name": data.get('full_name'),
                "email": email,
                "phone": data.get('phone'),
                "company_name": data.get('company') or data.get('company_name'),
                "job_title": data.get('job_title'),
                "city": data.get('city'),
                "state": data.get('state'),
                "country": data.get('country'),
                "postal_code": data.get('postal_code') or data.get('zip'),
                "linkedin_id": data.get('linkedin_id') or data.get('author_linkedin_id'),
                "linkedin_internal_id": data.get('linkedin_internal_id'),
                "source_type": effective_source_type,
                "source_reference": data.get('post_id') or data.get('source_reference'),
                "raw_payload": data
            })
            
        if not extracts_payload:
            return True
            
        payload = {"extracts": extracts_payload}
        
        try:
            response = requests.post(endpoint, json=payload, headers=self.headers)
            if response.status_code != 200:
                print(f"  [ERROR] Bulk automation extracts sync failed with status {response.status_code}")
                try:
                    error_json = response.json()
                    print(f"  [ERROR] Details: {error_json.get('detail', response.text)}")
                except:
                    print(f"  [ERROR] Details: {response.text}")
                    
            response.raise_for_status()
            result = response.json()
            inserted = result.get('inserted', 0)
            duplicates = result.get('duplicates', 0)
            failed = result.get('failed', 0)
            
            print(f"  [SUMMARY] Automation extracts sync: {inserted} inserted, {duplicates} duplicates, {failed} failed.")
            return result
        except Exception as e:
            if not isinstance(e, requests.exceptions.HTTPError):
                print(f"  [ERROR] Bulk automation extracts sync failed: {e}")
            return None

    def bulk_save_raw_positions(self, jobs_list: list) -> bool:
        """Sync identified job posts to the raw_position table in the backend."""
        self._ensure_valid_token()
        if not self.api_token or not jobs_list:
            return False
            
        if '/api' in self.api_url:
            base_url = self.api_url.rstrip('/')
        else:
            base_url = f"{self.api_url.rstrip('/')}/api"
            
        endpoint = f"{base_url}/raw-positions/bulk"
        
        positions_payload = []
        for job in jobs_list:
            # Map bot fields to backend RawPositionCreate schema
            positions_payload.append({
                "candidate_id": job.get('candidate_id') or (self.selected_candidate_id if self.selected_candidate_id != 0 else None),
                "source": self.job_unique_id, # Source is the Bot ID
                "source_uid": job.get('post_id'), # Unique ID within the source (Bot)
                "extractor_version": "v1.0",
                "raw_title": job.get('job_title', 'Unknown Role'),
                "raw_company": job.get('company') or job.get('author_name', 'Unknown Company'),
                "raw_location": job.get('location', ''), 
                "raw_zip": job.get('raw_zip', ''),
                "raw_description": job.get('post_text_preview', ''),
                "raw_contact_info": f"Email: {job.get('contact_email')}, Phone: {job.get('contact_phone')}",
                "raw_notes": f"Score: {job.get('job_score')}, Matches: {job.get('job_matches')}, URL: {job.get('post_url')}, Keyword: {job.get('source_keyword')}",
                "raw_payload": job,
                "processing_status": "new"
            })
            
        if not positions_payload:
            return True
            
        payload = {"positions": positions_payload}
        
        try:
            response = requests.post(endpoint, json=payload, headers=self.headers)
            if response.status_code != 200:
                print(f"  [ERROR] Bulk raw positions sync failed with status {response.status_code}")
                try:
                    error_json = response.json()
                    print(f"  [ERROR] Details: {error_json.get('detail', response.text)}")
                except:
                    print(f"  [ERROR] Details: {response.text}")
                    
            response.raise_for_status()
            result = response.json()
            inserted = result.get('inserted', 0)
            skipped = result.get('skipped', 0)
            print(f"  [SUMMARY] Raw positions sync: {inserted} inserted, {skipped} skipped.")
            return result
        except Exception as e:
            if not isinstance(e, requests.exceptions.HTTPError):
                print(f"  [ERROR] Bulk raw positions sync failed: {e}")
            return None


    def log_activity(
        self, 
        activity_count: int, 
        notes: str = "", 
        candidate_id: int = 0, 
        activity_date: Optional[str] = None
    ) -> bool:
        self._ensure_valid_token()
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
        self._ensure_valid_token()
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
