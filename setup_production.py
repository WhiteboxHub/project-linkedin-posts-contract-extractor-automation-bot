
import requests
import json
import os
import re
from pathlib import Path


DEFAULT_API_URL = "http://localhost:8000"
JOB_UNIQUE_ID = "bot_linkedin_post_contact_extractor"
ENV_FILE = ".env"

def setup_api_connection():
    print("=" * 70)
    print(" WBL JOB ACTIVITY LOGGER SETUP")
    print("=" * 70)
    print("\nThis script will configure the bot with your personal credentials.")
    print("=" * 70)

    # Step 1: Configuration Choice
    print("\n Step 1: Select Environment")
    print("-" * 70)
    print(f"1. Local (Default: {DEFAULT_API_URL})")
    print("2. Production (https://api.whitebox-learning.com/api)")
    choice = input("\nSelect environment [1/2, default: 2]: ").strip() or "1"
    
    if choice == "2":
        api_url = "https://api.whitebox-learning.com/api"
    else:
        api_url = input(f"Enter API URL [default: {DEFAULT_API_URL}]: ").strip() or DEFAULT_API_URL

    print("\n Step 2: Your Credentials")
    print("-" * 70)
    email = input("Enter your WBL email: ")
    password = input("Enter your WBL password: ")
    employee_id = input("Enter your Employee ID (e.g., 353): ").strip()
    
    while not employee_id:
        employee_id = input("Employee ID is required. Please enter it: ").strip()

  
    print("\n Step 3: Getting JWT Token")
    print("-" * 70)
  
    login_url = f"{api_url}/login"
    if "localhost" in api_url and not api_url.endswith("/api"):
        login_url = f"{api_url}/api/login"

    try:
       
        response = requests.post(
            login_url,
            data={
                "username": email,
                "password": password
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        
        if not token:
            print(" ERROR: No access_token in response")
            return
        
        print(f" Token received: {token[:20]}...{token[-20:]}")
        
    except Exception as e:
        print(f" Login failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
             print(f"   Response: {e.response.text}")
        return

    selected_candidate_id = "0"
 
    print("\n Step 4: Updating .env File")
    print("-" * 70)

    try:
        env_content = ""
        if os.path.exists(ENV_FILE):
            with open(ENV_FILE, 'r') as f:
                env_content = f.read()
        elif os.path.exists('.env.example'):
            with open('.env.example', 'r') as f:
                env_content = f.read()
        else:
            
            env_content = (
                "LINKEDIN_EMAIL=\n"
                "LINKEDIN_PASSWORD=\n"
                "WBL_API_URL=\n"
                "WBL_API_TOKEN=\n"
                "JOB_UNIQUE_ID=bot_linkedin_post_contact_extractor\n"
                "EMPLOYEE_ID=\n"
                "SELECTED_CANDIDATE_ID=0\n"
            )

     
        def update_key(content, key, value):
            if f"{key}=" in content:
                return re.sub(rf'^{key}=.*', f'{key}={value}', content, flags=re.MULTILINE)
            else:
                return content.rstrip() + f"\n{key}={value}\n"

        env_content = update_key(env_content, "WBL_API_URL", api_url)
        env_content = update_key(env_content, "WBL_API_TOKEN", token)
        env_content = update_key(env_content, "JOB_UNIQUE_ID", JOB_UNIQUE_ID)
        env_content = update_key(env_content, "EMPLOYEE_ID", employee_id)
        env_content = update_key(env_content, "SELECTED_CANDIDATE_ID", selected_candidate_id)

        with open(ENV_FILE, 'w') as f:
            f.write(env_content)
        
        print(f" Updated {ENV_FILE} with your personal configuration.")
        
    except Exception as e:
        print(f" Failed to update .env: {e}")
        return

   
    print("\n  Step 5: Verifying Job Type in Database")
    print("-" * 70)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

   
    types_url = f"{api_url}/job-types"
    if "localhost" in api_url and not api_url.endswith("/api"):
        types_url = f"{api_url}/api/job-types"

    try:
        response = requests.get(types_url, headers=headers)
        response.raise_for_status()
        existing_jobs = response.json()
        
        job_exists = False
        for job in existing_jobs:
            if job.get('unique_id') == JOB_UNIQUE_ID:
                print(f" Job type already exists (ID: {job.get('id')})")
                job_exists = True
                break
        
        if not job_exists:
            print("Creating missing job type...")
            job_type_data = {
                "unique_id": JOB_UNIQUE_ID,
                "name": "LinkedIn Job Post Contact Extractor",
                "job_owner_id": int(employee_id),
                "description": "Extracts job post contacts from LinkedIn using Selenium automation",
                "notes": "Automated bot that searches LinkedIn for AI/ML job posts and extracts contact information"
            }
            response = requests.post(types_url, json=job_type_data, headers=headers)
            response.raise_for_status()
            print(f" Job type created successfully.")
            
    except Exception as e:
        print(f"  Note: Could not verify/create job type via API: {e}")
        print("   If you have already run the SQL setup script, this is fine.")

    print("\n" + "=" * 70)
    print(" PERSONAL SETUP COMPLETE!")
    print("=" * 70)
    print("\nYour bot is now configured for your account.")
    print(f"Linked to Employee ID: {employee_id}")
    print("\nNext step: Run 'python linkedin_bot_complete.py'")
    print("=" * 70)

def auto_import_from_marketing():
    print("\n" + "="*60)
    print(" AUTO-IMPORT FROM MARKETING TABLE")
    print("="*60)
    
    # Need API connection
    env_vars = {}
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            for line in f:
                if '=' in line:
                    k, v = line.strip().split('=', 1)
                    env_vars[k] = v
    
    api_url = env_vars.get('WBL_API_URL')
    token = env_vars.get('WBL_API_TOKEN')
    
    if not api_url or not token:
        print("[ERROR] .env file missing or incomplete. Run 'Setup API Connection' first.")
        return []

    try:
        print(" Connecting to API...")
        marketing_url = f"{api_url}/candidate/marketing"
        if "localhost" in api_url and not api_url.endswith("/api"):
            marketing_url = f"{api_url}/api/candidate/marketing"
            
        response = requests.get(marketing_url, params={"limit": 1000}, headers={"Authorization": f"Bearer {token}"})
        response.raise_for_status()
        data = response.json()
        records = data.get('data', [])
        
        if not records:
            print(" No marketing candidates found.")
            return []
            
        print(f" Found {len(records)} records.")
        
        # Discovery Phase
        sample = records[0]
        print("\n [SCHEMA DISCOVERY] Here is a sample record:")
        
        # Flatten for display
        def print_keys(d, prefix=''):
            keys = []
            for k, v in d.items():
                if isinstance(v, dict):
                    keys.extend(print_keys(v, prefix + k + '.'))
                else:
                    keys.append(f"{prefix}{k} : {str(v)[:30]}...")
            return keys

        flat_keys = print_keys(sample)
        for fk in flat_keys:
            print(f"   - {fk}")
            
        print("\n Please identify the field names for LinkedIn Email and Password.")
        print(" (Copy the exact key path from above, e.g., 'candidate.linkedin_email')")
        
        email_key = input(" Field for LinkedIn Email: ").strip()
        pass_key = input(" Field for LinkedIn Password: ").strip()
        
        if not email_key or not pass_key:
            print(" valid keys required.")
            return []
            
        imported = []
        
        def get_val(d, path):
            parts = path.split('.')
            curr = d
            for p in parts:
                if isinstance(curr, dict):
                    curr = curr.get(p)
                else:
                    return None
            return curr

        print("\n Importing...")
        for rec in records:
            email = get_val(rec, email_key)
            pwd = get_val(rec, pass_key)
            
            # Try to get candidate ID automatically
            cid = get_val(rec, 'candidate.id')
            
            if email and pwd:
                cand = {
                    "linkedin_email": email,
                    "linkedin_password": pwd,
                    "candidate_id": cid or 0,
                    "keywords": ["AI Engineer hiring"] # Default keyword
                }
                imported.append(cand)
                print(f"  + Queued: {email}")
        
        print(f"\n Found {len(imported)} candidates with credentials.")
        confirm = input(" Add these to your config? (y/n): ").strip().lower()
        
        if confirm == 'y':
            return imported
        return []

    except Exception as e:
        print(f" [ERROR] Import failed: {e}")
        return []

def setup_multi_candidate_config():
    print("\n" + "="*60)
    print(" MULTI-CANDIDATE SETUP (candidates.json)")
    print(" This allows the bot to run sequentially for multiple LinkedIn accounts.")
    print("="*60)
    
    candidates = []
    
    # Load existing if available
    if os.path.exists('candidates.json'):
        try:
            with open('candidates.json', 'r') as f:
                candidates = json.load(f)
            print(f" Loaded {len(candidates)} existing candidates.")
        except:
            pass
            
    while True:
        print(f"\nCurrent candidates configured: {len(candidates)}")
        print(" 1. Add new candidate (Manual)")
        print(" 2. View/Remove candidates")
        print(" 3. Save and Exit")
        print(" 4. Auto-Import from Marketing Table")
        print(" 0. Go Back")
        
        sub = input(" Enter choice: ").strip()
        
        if sub == '1':
            print("\n -- Add Candidate --")
            email = input(" LinkedIn Email: ").strip()
            pwd = input(" LinkedIn Password: ").strip()
            cid = input(" WBL Candidate ID (Optional, press Enter to skip): ").strip()
            kws = input(" Keywords (comma separated, e.g. 'AI Engineer, Data Scientist'): ").strip()
            
            cand_obj = {
                "linkedin_email": email,
                "linkedin_password": pwd
            }
            if cid:
                cand_obj["candidate_id"] = int(cid)
            if kws:
                cand_obj["keywords"] = [k.strip() for k in kws.split(',') if k.strip()]
            
            candidates.append(cand_obj)
            print(" Candidate added.")
            
        elif sub == '2':
            if not candidates:
                print(" No candidates to show.")
                continue
            
            for idx, c in enumerate(candidates):
                print(f" {idx+1}. {c.get('linkedin_email')} (ID: {c.get('candidate_id', 'N/A')})")
                
            rem = input(" Enter number to remove (or Press Enter to go back): ").strip()
            if rem.isdigit() and 1 <= int(rem) <= len(candidates):
                removed = candidates.pop(int(rem)-1)
                print(f" Removed {removed.get('linkedin_email')}")
                
        elif sub == '3':
            with open('candidates.json', 'w') as f:
                json.dump(candidates, f, indent=2)
            print(f"\n[SUCCESS] Saved {len(candidates)} candidates to candidates.json")
            break
            
        elif sub == '4':
            new_cands = auto_import_from_marketing()
            if new_cands:
                candidates.extend(new_cands)
                # Auto-save immediately to prevent data loss
                with open('candidates.json', 'w') as f:
                    json.dump(candidates, f, indent=2)
                print(f" Added {len(new_cands)} candidates and saved to candidates.json.")

        elif sub == '0':
            break

def main():
    while True:
        print("\n" + "="*60)
        print(" LINKEDIN BOT SETUP MENU")
        print("="*60)
        print(" 1. Setup API Connection (.env)")
        print(" 2. Setup Multi-Candidates (candidates.json)")
        print(" 0. Exit")
        
        choice = input("\n Enter choice: ").strip()
        
        if choice == '1':
            setup_api_connection()
        elif choice == '2':
            setup_multi_candidate_config()
        elif choice == '0':
            print(" Exiting.")
            break
        else:
            print(" Invalid choice.")

if __name__ == "__main__":
    main()
