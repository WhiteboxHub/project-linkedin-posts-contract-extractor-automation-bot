import os
import json
import csv
from datetime import datetime
from glob import glob
from modules.processor import ProcessorModule
from modules.logger import logger
from job_activity_logger import JobActivityLogger

class DataExtractor:
    def __init__(self, raw_data_dir="data/raw_posts", output_dir="data/output", candidate_id=None, candidate_email=None):
        self.raw_data_dir = raw_data_dir
        self.output_dir = output_dir
        self.candidate_id = candidate_id
        self.candidate_email = candidate_email
        self.processor = ProcessorModule()
        self.activity_logger = JobActivityLogger()
        if self.candidate_id:
            try:
                self.activity_logger.selected_candidate_id = int(self.candidate_id)
            except (ValueError, TypeError):
                pass
        
    def run(self, target_date=None):
        """
        Main entry point:
        1. Identify the raw data folder (defaults to today).
        2. Iterate through JSON files.
        3. Extract contacts and identify job posts.
        4. Save to separate outputs: contacts_extracted.json and jobs.json.
        """
        # Default to today if no date provided
        date_str = target_date or datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Starting Post-Processing Extraction for {date_str}...", extra={"step_name": "Extraction"})
        
        target_dir = os.path.join(self.raw_data_dir, date_str)
        
        if not os.path.exists(target_dir):
            logger.warning(f"No raw data found in {target_dir}", extra={"step_name": "Extraction"})
            return
            
        json_files = glob(os.path.join(target_dir, "*.json"))
        logger.info(f"Found {len(json_files)} JSON files to process in {target_dir}", extra={"step_name": "Extraction"})
        
        all_contacts = []
        all_jobs = []
        
        for dim_file in json_files:
            try:
                with open(dim_file, 'r', encoding='utf-8') as f:
                    posts = json.load(f)
                    
                for post in posts:
                    # Filter: If candidate_id is set for this extractor, only process posts found by this candidate
                    if self.candidate_id and str(post.get('candidate_id')) != str(self.candidate_id):
                        continue
                        
                    contacts, job_info = self._process_single_post(post)
                    if contacts:
                        all_contacts.extend(contacts)
                    if job_info:
                        all_jobs.append(job_info)
                    
            except Exception as e:
                logger.error(f"Error processing file {dim_file}: {e}", extra={"step_name": "Extraction"})
                
        # --- 3. SAVE TO DATE FOLDERS ---
        out_path = os.path.join(self.output_dir, date_str)
        if not os.path.exists(out_path):
            os.makedirs(out_path)
            
        self._save_contacts(all_contacts, out_path, filename="contacts_extracted")
        self._save_jobs(all_jobs, out_path, filename="jobs")
        
        # Consolidated master logic removed as per user request

        
        # --- 5. SYNC TO BACKEND (Bulk Contacts) ---
        if all_contacts:
            logger.info(f"Syncing {len(all_contacts)} contacts to vendor daily contract...", extra={"step_name": "Sync"})
            unique_contacts = list({c['email']: c for c in all_contacts if c.get('email')}.values())
            result = self.activity_logger.bulk_save_vendor_contacts(unique_contacts)
            
            if result:
                inserted = result.get('inserted', 0)
                failed = result.get('failed', 0)
                duplicates = result.get('duplicates', 0)
                
                if inserted > 0:
                    logger.info(f"Successfully synced {inserted} new contacts to backend.", extra={"step_name": "Sync"})
                elif failed > 0:
                    logger.error(f"Sync failed for {failed} contacts. Check the console for details.", extra={"step_name": "Sync"})
                else:
                    logger.info("Sync complete. No new contacts were inserted (all duplicates).", extra={"step_name": "Sync"})
            else:
                logger.error("Failed to connect to backend for contact sync.", extra={"step_name": "Sync"})

        # --- 6. SYNC TO BACKEND (Bulk Raw Positions / Jobs) ---
        if all_jobs:
            logger.info(f"Syncing {len(all_jobs)} jobs to raw positions table...", extra={"step_name": "Sync"})
            result = self.activity_logger.bulk_save_raw_positions(all_jobs)
            
            if result:
                inserted = result.get('inserted', 0)
                if inserted > 0:
                    logger.info(f"Successfully synced {inserted} jobs to backend.", extra={"step_name": "Sync"})
                else:
                    logger.info("Sync complete. No new jobs were inserted.", extra={"step_name": "Sync"})
            else:
                logger.error("Failed to connect to backend for job sync.", extra={"step_name": "Sync"})

        # --- 7. LOG SESSION SUMMARY (Job Activity Log) ---
        summary_note = f"LinkedIn Extraction Complete: {len(all_contacts)} contacts found today, {len(all_jobs)} jobs identified."
        
        # Read the content of the extracted CSV to include in notes
        csv_content = ""
        csv_file_path = os.path.join(out_path, "contacts_extracted.csv")
        if os.path.exists(csv_file_path):
            try:
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
            except Exception as e:
                logger.warning(f"Failed to read CSV for logging: {e}", extra={"step_name": "Logging"})
                csv_content = "[Error reading CSV file]"
        
        # Combine summary and CSV content
        full_notes = f"{summary_note}\n\n--- CSV OUTPUT ---\n{csv_content}"
        
        # Log to backend with full CSV content in notes
        self.activity_logger.log_activity(len(all_contacts), notes=full_notes)
        
        # Save local summary (keeping it lightweight for readability)
        self._save_activity_summary(len(all_contacts), summary_note)

        logger.info(f"Extraction complete. Contacts Found Today: {len(all_contacts)}, Jobs Identified: {len(all_jobs)}.", extra={"step_name": "Extraction"})
        print(f"\n>>> EXTRACTION COMPLETE <<<")
        print(f"Daily Results: {out_path}")
        print(f"Activity Log: {os.path.join(self.output_dir, 'activity_logs.csv')}\n")

    def _process_single_post(self, post):
        """
        Evaluate post for BOTH contacts and job classification.
        Returns: (contacts_list, job_dict_or_None)
        """
        post_text = "\n".join(post.get('post_text', []))
        if not post_text:
            return [], None
            
        contacts = []
        job_info = None
        
        # --- 1. CONTACT EXTRACTION ---
        emails = self.processor.extract_email(post_text)
        phones = self.processor.extract_phone(post_text)
        primary_phone = phones[0] if phones else ""
        
        if emails:
            for email in emails:
                # Rule-based Name Extraction
                name = self.processor.extract_name_from_email(email)
                if not name:
                    name = post.get('author_name', 'Unknown')
                    
                # Rule-based Company Extraction
                company = self.processor.extract_company_from_email(email)
                
                # Use existing post_url if available
                post_url = post.get('post_url', '')
                if not post_url:
                    post_id = post.get('post_id', '')
                    if post_id:
                        if 'urn:li:activity:' in post_id:
                            post_url = f"https://www.linkedin.com/feed/update/{post_id}/"
                        elif post_id.isdigit():
                            post_url = f"https://www.linkedin.com/feed/update/urn:li:activity:{post_id}/"
                        # Explicitly DO NOT construct URL if it's a hash or unknown format
                
                # Extract internal ID from profile URL (e.g. "john-doe" or "ACoAA...")
                profile_url = post.get('linkedin_id', '') or post.get('profile_url', '')
                internal_id = ""
                if profile_url and '/in/' in profile_url:
                    parts = profile_url.rstrip('/').split('/in/')
                    if len(parts) > 1:
                        internal_id = parts[1].split('?')[0] # Get the part after /in/ and before query
                
                contact = {
                    "full_name": name,
                    "email": email,
                    "phone": primary_phone,
                    "author_linkedin_id": profile_url,      # Full Profile URL
                    "linkedin_internal_id": internal_id,    # The handle/ID part
                    "company": company or "Unknown",
                    "linkedin_id": profile_url,             # Keeping for backward compatibility if needed
                    "post_url": post_url,
                    "source_keyword": post.get('search_keyword', ''),
                    "candidate_id": post.get('candidate_id'),
                    "candidate_email": post.get('candidate_email'),
                    "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                contacts.append(contact)

        # --- 2. JOB CLASSIFICATION ---
        is_job, job_details = self.processor.classify_job_post(post_text)
        
        if is_job:
            # Re-calculate post_url for job_info as well
            post_url = post.get('post_url', '')
            if not post_url:
                post_id = post.get('post_id', '')
                if post_id:
                    if 'urn:li:activity:' in post_id:
                        post_url = f"https://www.linkedin.com/feed/update/{post_id}/"
                    elif post_id.isdigit():
                        post_url = f"https://www.linkedin.com/feed/update/urn:li:activity:{post_id}/"
                    # Explicitly DO NOT construct URL if it's a hash or unknown format

            job_info = {
                "post_id": post.get('post_id'),
                "post_url": post_url,
                "author_name": post.get('author_name', 'Unknown'),
                "job_title": self.processor.extract_job_title(post_text),
                "company": (emails and self.processor.extract_company_from_email(emails[0])) or post.get('company', 'Unknown'),
                "linkedin_id": post.get('linkedin_id', ''),
                "source_keyword": post.get('search_keyword', ''),
                "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "job_score": job_details['score'],
                "job_matches": "; ".join(job_details['matched_rules']),
                "contract_type": self.processor.extract_contract_type(post_text),
                "location": post.get('location', ''),
                "raw_zip": self.processor.extract_zip(post_text) or self.processor.extract_zip(post.get('location', '')),
                "candidate_id": post.get('candidate_id'),
                "candidate_email": post.get('candidate_email'),
                # Include contact info if available, even if redundant
                "contact_email": emails[0] if emails else "",
                "contact_phone": primary_phone,
                "post_text_preview": post_text[:500].replace('\n', ' ') 
            }

        return contacts, job_info


    def _save_contacts(self, contacts, out_dir, filename="contacts_extracted"):
        """Save extracted contacts for the current run."""
        # JSON
        json_path = os.path.join(out_dir, f"{filename}.json")
        csv_path = os.path.join(out_dir, f"{filename}.csv")
        
        unique_contacts = list({c['email']: c for c in contacts}.values()) if contacts else []
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(unique_contacts, f, indent=2, ensure_ascii=False)
            
        # CSV
        keys = ["full_name", "email", "phone", "author_linkedin_id", "linkedin_internal_id", "company", "linkedin_id", "post_url", "source_keyword", "extraction_date"]
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            writer.writeheader()
            if unique_contacts:
                writer.writerows(unique_contacts)
            
        if unique_contacts:
            logger.info(f"Saved {len(unique_contacts)} unique contacts to {json_path}", extra={"step_name": "Extraction"})

    def _save_jobs(self, jobs, out_dir, filename="jobs"):
        """Save classified jobs for the current run."""
        json_path = os.path.join(out_dir, f"{filename}.json")
        csv_path = os.path.join(out_dir, f"{filename}.csv")
        
        unique_jobs = list({j['post_id']: j for j in jobs}.values()) if jobs else []
        
        # JSON
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(unique_jobs, f, indent=2, ensure_ascii=False)
            
        # CSV
        keys = [
            "post_id", "post_url", "author_name", "linkedin_id", "source_keyword", 
            "extraction_date", "job_score", "job_matches", "contract_type", "contact_email", "contact_phone", "post_text_preview"
        ]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            writer.writeheader()
            if unique_jobs:
                writer.writerows(unique_jobs)
            
        if unique_jobs:
            logger.info(f"Saved {len(unique_jobs)} unique jobs to {json_path}", extra={"step_name": "Extraction"})

    def _save_activity_summary(self, count, notes):
        """Append session summary to activity_logs.csv."""
        filepath = os.path.join(self.output_dir, 'activity_logs.csv')
        file_exists = os.path.exists(filepath)
        
        try:
            with open(filepath, 'a', newline='', encoding='utf-8') as f:
                fieldnames = ['timestamp', 'contact_count', 'notes']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                writer.writerow({
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'contact_count': count,
                    'notes': notes
                })
        except Exception as e:
            logger.error(f"Failed to save activity summary to CSV: {e}", extra={"step_name": "Extraction"})

if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.run()
