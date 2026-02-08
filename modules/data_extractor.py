import os
import json
import csv
from datetime import datetime
from glob import glob
from modules.processor import ProcessorModule
from modules.logger import logger

class DataExtractor:
    def __init__(self, raw_data_dir="data/raw_posts", output_dir="data/output"):
        self.raw_data_dir = raw_data_dir
        self.output_dir = output_dir
        self.processor = ProcessorModule()
        
    def run(self):
        """
        Main entry point:
        1. Identify today's raw data folder (or scan all recent).
        2. Iterate through JSON files.
        3. Extract contacts and identify job posts.
        4. Save to separate outputs: contacts_extracted.json and jobs.json.
        """
        logger.info("Starting Post-Processing Extraction (Contacts & Jobs)...", extra={"step_name": "Extraction"})
        
        # We default to processing *today's* folder, but logic could be expanded
        today_str = datetime.now().strftime('%Y-%m-%d')
        target_dir = os.path.join(self.raw_data_dir, today_str)
        
        if not os.path.exists(target_dir):
            logger.warning(f"No raw data found for today ({target_dir})", extra={"step_name": "Extraction"})
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
                    contacts, job_info = self._process_single_post(post)
                    if contacts:
                        all_contacts.extend(contacts)
                    if job_info:
                        all_jobs.append(job_info)
                    
            except Exception as e:
                logger.error(f"Error processing file {dim_file}: {e}", extra={"step_name": "Extraction"})
                
        # Validate output dir
        out_path = os.path.join(self.output_dir, today_str)
        if not os.path.exists(out_path):
            os.makedirs(out_path)
            
        self._save_contacts(all_contacts, out_path)
        self._save_jobs(all_jobs, out_path)
        
        logger.info(f"Extraction complete. Contacts: {len(all_contacts)}, Jobs: {len(all_jobs)}.", extra={"step_name": "Extraction"})

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
                
                contact = {
                    "full_name": name,
                    "email": email,
                    "phone": primary_phone,
                    "company": company or "Unknown",
                    "linkedin_id": post.get('linkedin_id', ''),
                    "post_url": f"https://www.linkedin.com/feed/update/{post.get('post_id')}/",
                    "source_keyword": post.get('search_keyword', ''),
                    "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                contacts.append(contact)

        # --- 2. JOB CLASSIFICATION ---
        is_job, job_details = self.processor.classify_job_post(post_text)
        
        if is_job:
            job_info = {
                "post_id": post.get('post_id'),
                "post_url": f"https://www.linkedin.com/feed/update/{post.get('post_id')}/",
                "author_name": post.get('author_name', 'Unknown'),
                "linkedin_id": post.get('linkedin_id', ''),
                "source_keyword": post.get('search_keyword', ''),
                "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "job_score": job_details['score'],
                "job_matches": "; ".join(job_details['matched_rules']),
                # Include contact info if available, even if redundant
                "contact_email": emails[0] if emails else "",
                "contact_phone": primary_phone,
                "post_text_preview": post_text[:500].replace('\n', ' ') 
            }

        return contacts, job_info

    def _save_contacts(self, contacts, out_dir):
        """Save extracted contacts."""
        if not contacts: return

        # deduplicate by email
        unique_contacts = {c['email']: c for c in contacts}.values()
        
        # JSON
        json_path = os.path.join(out_dir, "contacts_extracted.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(list(unique_contacts), f, indent=2, ensure_ascii=False)
            
        # CSV
        csv_path = os.path.join(out_dir, "contacts_extracted.csv")
        keys = ["full_name", "email", "phone", "company", "linkedin_id", "post_url", "source_keyword", "extraction_date"]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(unique_contacts)
            
        logger.info(f"Saved {len(unique_contacts)} unique contacts to {json_path}", extra={"step_name": "Extraction"})

    def _save_jobs(self, jobs, out_dir):
        """Save classified jobs."""
        if not jobs: return

        # deduplicate by post_id
        unique_jobs = {j['post_id']: j for j in jobs}.values()
        
        # JSON
        json_path = os.path.join(out_dir, "jobs.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(list(unique_jobs), f, indent=2, ensure_ascii=False)
            
        # CSV
        csv_path = os.path.join(out_dir, "jobs.csv")
        keys = [
            "post_id", "post_url", "author_name", "linkedin_id", "source_keyword", 
            "extraction_date", "job_score", "job_matches", "contact_email", "contact_phone", "post_text_preview"
        ]
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(unique_jobs)
            
        logger.info(f"Saved {len(unique_jobs)} unique jobs to {json_path}", extra={"step_name": "Extraction"})

if __name__ == "__main__":
    extractor = ContactExtractor()
    extractor.run()
