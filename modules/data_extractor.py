import os
import json
import csv
from datetime import datetime
from glob import glob
from modules.processor import ProcessorModule
from modules.nlp_gliner import GLiNERExtractor
from modules.logger import logger
from modules.logger import logger
from job_activity_logger import JobActivityLogger

class DataExtractor:
    def __init__(self, raw_data_dir="data/raw_posts", output_dir="data/output", candidate_id=None, candidate_email=None):
        self.raw_data_dir = raw_data_dir
        self.output_dir = output_dir
        self.candidate_id = candidate_id
        self.candidate_email = candidate_email
        self.processor = ProcessorModule()
        self.ner_extractor = GLiNERExtractor()
        self.activity_logger = JobActivityLogger()
        if self.candidate_id:
            try:
                self.activity_logger.selected_candidate_id = int(self.candidate_id)
            except (ValueError, TypeError):
                pass
        
    def run(self, target_date=None):
        date_str = target_date or datetime.now().strftime('%Y-%m-%d')
        logger.info(f"Starting Post-Processing Extraction for {date_str}...", extra={"step_name": "Extraction"})
        
        target_dir = os.path.join(self.raw_data_dir, date_str)
        
        if not os.path.exists(target_dir):
            logger.warning(f"No raw data found in {target_dir}", extra={"step_name": "Extraction"})
            return 0
            
        json_files = glob(os.path.join(target_dir, "*.json"))
        logger.info(f"Found {len(json_files)} JSON files to process in {target_dir}", extra={"step_name": "Extraction"})
        
        all_contacts = []
        all_jobs = []
        total_inserted = 0
        
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
                
      
        out_path = os.path.join(self.output_dir, date_str)
        if not os.path.exists(out_path):
            os.makedirs(out_path)
            
        self._save_contacts(all_contacts, out_path, filename="contacts_extracted")
        unique_jobs_saved = self._save_jobs(all_jobs, out_path, filename="jobs")
        
        
        if all_contacts:
            logger.info(f"Syncing {len(all_contacts)} contacts to automated daily contacts table...", extra={"step_name": "Sync"})
            unique_contacts = list({c['email']: c for c in all_contacts if c.get('email')}.values())
            result = self.activity_logger.bulk_save_automation_contacts(unique_contacts)
            
            if result:
                inserted = result.get('inserted', 0)
                failed = result.get('failed', 0)
                duplicates = result.get('duplicates', 0)
                
                total_inserted = inserted
                
                if inserted > 0:
                    logger.info(f"Successfully synced {inserted} new contacts to automated daily table.", extra={"step_name": "Sync"})
                elif failed > 0:
                    logger.error(f"Sync failed for {failed} contacts. Check the console for details.", extra={"step_name": "Sync"})
                else:
                    logger.info("Sync complete. No new contacts were inserted (all duplicates).", extra={"step_name": "Sync"})
            else:
                logger.error("Failed to connect to backend for contact sync.", extra={"step_name": "Sync"})

        # --- 6. SYNC TO BACKEND (Split Raw Positions & Email Positions) ---
        jobs_inserted = 0
        email_jobs_inserted = 0
        
        if unique_jobs_saved:
            email_jobs = []
            raw_jobs = []
            
            personal_domains = {
                'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 
                'aol.com', 'live.com', 'icloud.com', 'msn.com', 
                'ymail.com', 'protonmail.com', 'mail.com'
            }
            
            for job in unique_jobs_saved:
                email = str(job.get('contact_email') or '').strip().lower()
                
                if not email:
                    # No email -> Route to Raw Jobs
                    raw_jobs.append(job)
                elif '@' in email:
                    domain = email.split('@')[-1]
                    if domain in personal_domains:
                        # Personal Email -> DROP IT completely
                        logger.info(f"Dropping post {job.get('post_id')} due to personal email: {email}", extra={"step_name": "Sync"})
                        continue
                    elif job.get('job_title') == 'Unknown Role':
                        # Unknown Role -> Route to Raw Jobs instead of Email Positions
                        logger.info(f"Routing post {job.get('post_id')} to raw jobs due to Unknown Role title", extra={"step_name": "Sync"})
                        raw_jobs.append(job)
                    else:
                        # Company Email -> Route to Email Positions
                        email_jobs.append(job)
                else:
                    # Invalid email format, treat as no email
                    raw_jobs.append(job)
            
            if raw_jobs:
                logger.info(f"Syncing {len(raw_jobs)} jobs without emails to raw positions table...", extra={"step_name": "Sync"})
                result = self.activity_logger.bulk_save_raw_positions(raw_jobs)
                if result:
                    jobs_inserted = result.get('inserted', 0)
                    if jobs_inserted > 0:
                        logger.info(f"Successfully synced {jobs_inserted} raw jobs to backend.", extra={"step_name": "Sync"})
                    else:
                        logger.info("Raw jobs sync complete. No new jobs were inserted.", extra={"step_name": "Sync"})
                else:
                    logger.error("Failed to connect to backend for raw job sync.", extra={"step_name": "Sync"})

            if email_jobs:
                logger.info(f"Syncing {len(email_jobs)} jobs WITH emails to email positions table...", extra={"step_name": "Sync"})
                result = self.activity_logger.bulk_save_email_positions(email_jobs)
                if result:
                    email_jobs_inserted = result.get('inserted', 0)
                    if email_jobs_inserted > 0:
                        logger.info(f"Successfully synced {email_jobs_inserted} email positions to backend.", extra={"step_name": "Sync"})
                    else:
                        logger.info("Email positions sync complete. No new positions were inserted.", extra={"step_name": "Sync"})
                else:
                    logger.error("Failed to connect to backend for email positions sync.", extra={"step_name": "Sync"})

        
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
        
        
        full_notes = f"{summary_note}\n\n--- CSV OUTPUT ---\n{csv_content}"
        
        
        self.activity_logger.log_activity(len(all_contacts), notes=full_notes)
        
        
        self._save_activity_summary(len(all_contacts), summary_note)

        logger.info(f"Extraction complete. Contacts Found Today: {len(all_contacts)}, Jobs Identified: {len(all_jobs)}.", extra={"step_name": "Extraction"})
        print(f"\n>>> EXTRACTION COMPLETE <<<")
        print(f"Daily Results: {out_path}")
        print(f"Activity Log: {os.path.join(self.output_dir, 'activity_logs.csv')}\n")

        return {
            "contacts_found": len(all_contacts),
            "contacts_synced": total_inserted,
            "positions_found": len(all_jobs),
            "positions_synced": jobs_inserted,
            "email_positions_synced": email_jobs_inserted
        }

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
                        elif len(post_id) == 32 and all(c in '0123456789abcdefABCDEFxyz' for c in post_id): 
                            post_url = ""
                        elif len(post_id) >= 15: # Support for hashes or GUIDs
                            post_url = f"https://www.linkedin.com/feed/update/{post_id}/"
                       
                
                # Extract internal ID from profile URL (e.g. "john-doe" or "ACoAA...")
                profile_url = post.get('linkedin_id', '') or post.get('profile_url', '')
                internal_id = ""
                if profile_url and '/in/' in profile_url:
                    parts = profile_url.rstrip('/').split('/in/')
                    if len(parts) > 1:
                        internal_id = parts[1].split('?')[0] 
                
                contact = {
                    "full_name": name,
                    "email": email,
                    "phone": primary_phone,
                    "author_linkedin_id": profile_url,      
                    "linkedin_internal_id": internal_id,    
                    "company": company or "Unknown",
                    "linkedin_id": profile_url,             
                    "post_url": post_url,
                    "source_keyword": post.get('search_keyword', ''),
                    "post_id": post.get('post_id'),
                    "candidate_id": post.get('candidate_id'),
                    "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                contacts.append(contact)

        # --- 2. JOB CLASSIFICATION ---
        is_job, job_details = self.processor.classify_job_post(post_text)
        
        if is_job:
            # NER EXTRACTION FOR HIGH CONFIDENCE POSTS
            ner_entities = self.ner_extractor.extract_entities(post_text)

            # Re-calculate post_url for job_info as well
            post_url = post.get('post_url', '')
            if not post_url:
                post_id = post.get('post_id', '')
                if post_id:
                    if 'urn:li:activity:' in post_id:
                        post_url = f"https://www.linkedin.com/feed/update/{post_id}/"
                    elif post_id.isdigit():
                        post_url = f"https://www.linkedin.com/feed/update/urn:li:activity:{post_id}/"
                    elif len(post_id) == 32 and all(c in '0123456789abcdefABCDEFxyz' for c in post_id): 
                        post_url = ""
                    elif len(post_id) >= 15: # Support for hashes or GUIDs
                        post_url = f"https://www.linkedin.com/feed/update/{post_id}/"

                job_info = {
                    "post_id": post.get('post_id'),
                    "post_url": post_url,
                    "author_name": post.get('author_name', 'Unknown'),
                    "job_title": ner_entities.get('job_title') or self.processor.extract_job_title(post_text),
                    "company": ner_entities.get('company') or (emails and self.processor.extract_company_from_email(emails[0])) or post.get('company', 'Unknown'),
                    "linkedin_id": post.get('linkedin_id', ''),
                    "source_keyword": post.get('search_keyword', ''),
                    "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "job_score": job_details['score'],
                    "job_matches": "; ".join(job_details['matched_rules']),
                    "contract_type": ner_entities.get('duration') or self.processor.extract_contract_type(post_text),
                    "location": ner_entities.get('location') or post.get('location', ''),
                    "raw_zip": self.processor.extract_zip(post_text) or self.processor.extract_zip(post.get('location', '')),
                    "candidate_id": post.get('candidate_id'),
                    # Include contact info if available, even if redundant
                    "contact_email": emails[0] if emails else "",
                    "contact_phone": primary_phone,
                    "post_text_preview": post_text[:500].replace('\n', ' '),
                    "job_link_url": post.get('job_link_url', ''),
                    "skills": ", ".join(ner_entities.get('skills', [])),
                    "visa_status": ner_entities.get('visa_status', ''),
                    "pay_rate": ner_entities.get('pay_rate', '')
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
            "post_id", "post_url", "job_link_url", "author_name", "linkedin_id", "source_keyword", 
            "extraction_date", "job_score", "job_matches", "job_title", "company", "location", "contract_type", 
            "contact_email", "contact_phone", "post_text_preview", "skills", "visa_status", "pay_rate"
        ]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            writer.writeheader()
            if unique_jobs:
                writer.writerows(unique_jobs)
            
        if unique_jobs:
            logger.info(f"Saved {len(unique_jobs)} unique jobs to {json_path}", extra={"step_name": "Extraction"})
            
        return unique_jobs

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
