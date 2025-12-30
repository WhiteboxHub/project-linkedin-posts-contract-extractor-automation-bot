
import requests
import json
import os
import re
from pathlib import Path


DEFAULT_API_URL = "http://localhost:8000"
JOB_UNIQUE_ID = "bot_linkedin_post_contact_extractor"
ENV_FILE = ".env"

def run_setup():
    print("=" * 70)
    print(" WBL JOB ACTIVITY LOGGER SETUP")
    print("=" * 70)
    print("\nThis script will configure the bot with your personal credentials.")
    print("=" * 70)

    # Step 1: Configuration Choice
    print("\n Step 1: Select Environment")
    print("-" * 70)
    print(f"1. Local (Default: {DEFAULT_API_URL})")
    print("2. Production (https://whitebox-learning.com/api)")
    choice = input("\nSelect environment [1/2, default: 2]: ").strip() or "1"
    
    if choice == "2":
        api_url = "https://whitebox-learning.com/api"
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

    print("\n Step 3.5: Select Marketing Candidate (Optional)")
    print("-" * 70)
    print(" Fetching marketing candidates...")
    
    selected_candidate_id = "0"
    try:
        marketing_url = f"{api_url}/candidate/marketing"
        if "localhost" in api_url and not api_url.endswith("/api"):
            marketing_url = f"{api_url}/api/candidate/marketing"
            
        response = requests.get(marketing_url, params={"limit": 1000}, headers={"Authorization": f"Bearer {token}"})
        response.raise_for_status()
        marketing_data = response.json()
        
      
        all_candidates = []
        for record in marketing_data.get('data', []):
            if record.get('candidate'):
                all_candidates.append({
                    "id": record['candidate']['id'],
                    "name": record['candidate']['full_name'],
                    "start_date": record.get('start_date', 'N/A'),
                    "status": record.get('status', 'N/A')
                })
        
        if all_candidates:
            print(f"\n Found {len(all_candidates)} marketing candidates:")
            print(f" {'ID':<5} | {'Name':<25} | {'Start Date':<12} | {'Status':<10}")
            print("-" * 60)
            for cand in all_candidates:
                print(f" {cand['id']:<5} | {cand['name']:<25} | {cand['start_date']} | {cand['status']}")
            
            cand_choice = input("\nEnter Candidate ID to associate with this bot (or press Enter to skip): ").strip()
            if cand_choice:
                selected_candidate_id = cand_choice
                print(f" Selected candidate ID: {selected_candidate_id}")
        else:
            print(" No marketing candidates found.")
    except Exception as e:
        print(f" Could not fetch candidates from existing endpoint: {e}")
        print(" Skipping candidate selection.")
 
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

if __name__ == "__main__":
    run_setup()
