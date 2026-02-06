import os
import csv
import duckdb
from datetime import datetime
import config
from modules.logger import logger

class StorageManager:
    """
    Manages data persistence including DuckDB, CSV exports, 
    and file system storage for post content.
    """
    def __init__(self):
        self.output_date_dir = os.path.join("output", datetime.now().strftime('%Y-%m-%d'))
        if not os.path.exists(self.output_date_dir):
            os.makedirs(self.output_date_dir)
            
        self.posts_dir = "saved_posts" # Keep legacy if needed, or point to new one
        self.processed_profiles = set()
        self.processed_posts = set()
        self.extracted_contacts_buffer = []
        self.db_file = 'linkedin_data.db'
        
        self.load_processed_posts()
        self.profile_cache = {}
        self.load_processed_profiles()

    def _ensure_db_schema(self, con):
        """Create the posts table with exact fieldnames from legacy all_posts.csv."""
        con.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                post_id VARCHAR PRIMARY KEY,
                post_url VARCHAR,
                keyword VARCHAR,
                author_name VARCHAR,
                post_text_preview TEXT,
                profile_url VARCHAR,
                has_email BOOLEAN,
                has_phone BOOLEAN,
                is_job_post BOOLEAN,
                is_ai_related BOOLEAN,
                extraction_date TIMESTAMP
            )
        """)

    def load_processed_posts(self):
        """Load previously processed post IDs from DuckDB to avoid duplicates."""
        try:
            con = duckdb.connect(self.db_file)
            self._ensure_db_schema(con)
            
            results = con.execute("SELECT post_id FROM posts").fetchall()
            for row in results:
                self.processed_posts.add(row[0])
            
            logger.info(f"Loaded {len(self.processed_posts)} previously processed post IDs from DuckDB ({self.db_file})", extra={"step_name": "Storage Init"})
            con.close()
        except Exception as e:
            logger.error(f"Could not load processed posts from DuckDB: {e}", extra={"step_name": "Storage Init"}, exc_info=True)
            self.processed_posts = set()
    
    def save_processed_post_id(self, post_id):
        """Track locally."""
        self.processed_posts.add(post_id)

    def load_processed_profiles(self):
        """Load already processed profile data from output CSV for reuse."""
        try:
            if os.path.exists(config.OUTPUT_FILE):
                with open(config.OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        url = row.get('linkedin_id')
                        if url:
                            url = url.strip().rstrip('/')
                            # Store full data for reuse
                            self.profile_cache[url] = {
                                'full_name': row.get('full_name', ''),
                                'email': row.get('email', ''),
                                'phone': row.get('phone', ''),
                                'company_name': row.get('company_name', ''),
                                'location': row.get('location', '')
                            }
                            self.processed_profiles.add(url)
                logger.info(f"Loaded {len(self.processed_profiles)} profiles into cache from {config.OUTPUT_FILE}", extra={"step_name": "Storage Init"})
        except Exception as e:
            logger.error(f"Could not load profiles for cache: {e}", extra={"step_name": "Storage Init"}, exc_info=True)

    def save_full_post(self, text, post_id, keyword, metadata=None):
        """Save the actual post content to a separate file for storage."""
        if config.DRY_RUN:
            logger.info(f"[DRY RUN] Would save full post text for {post_id} to file", extra={"step_name": "Persistence"})
            return True
            
        try:
            if not os.path.exists(self.posts_dir):
                os.makedirs(self.posts_dir)
            
            safe_keyword = keyword.replace(' ', '_').replace('/', '_')
            filename = f"{safe_keyword}_posts.txt"
            filepath = os.path.join(self.posts_dir, filename)
            
            meta_text = ""
            if metadata:
                meta_text = (
                    f"Full Name: {metadata.get('full_name', 'N/A')}\n"
                    f"Email: {metadata.get('email', 'N/A')}\n"
                    f"Phone: {metadata.get('phone', 'N/A')}\n"
                    f"LinkedIn ID: {metadata.get('linkedin_id', 'N/A')}\n"
                    f"Company: {metadata.get('company_name', 'N/A')}\n"
                    f"Location: {metadata.get('location', 'N/A')}\n"
                    f"Extraction Date: {metadata.get('extraction_date', 'N/A')}\n"
                    f"Search Keyword: {metadata.get('search_keyword', 'N/A')}\n"
                )
            
            with open(filepath, 'a', encoding='utf-8') as f:
                f.write("\n" + "=" * 80 + "\n")
                f.write(f"POST ID: {post_id}\n")
                f.write("-" * 80 + "\n")
                if meta_text:
                    f.write("METADATA:\n")
                    f.write(meta_text)
                    f.write("-" * 80 + "\n")
                f.write("POST CONTENT:\n\n")
                f.write(text)
                f.write("\n\n")
            
            # Also save to CSV as per new requirement
            if metadata:
                # Ensure post_id is in metadata if not already
                if 'post_id' not in metadata: metadata['post_id'] = post_id
                self.save_post_to_csv(metadata, keyword)
            
            return True
        except Exception as e:
            logger.error(f"Error saving post: {e}", extra={"step_name": "Persistence", "post_id": post_id}, exc_info=True)
            return False

    def save_post_metadata(self, post_data, keyword, post_id):
        """Save post metadata to DuckDB database."""
        if config.DRY_RUN:
            logger.info(f"[DRY RUN] Would save metadata for post {post_id} to DuckDB", extra={"step_name": "Persistence"})
            return True
            
        try:
            con = duckdb.connect(self.db_file)
            self._ensure_db_schema(con)
            
            post_preview = post_data.get('post_text', '')[:500]
            
            post_url = post_data.get('post_url', '')
            if not post_url and post_id and 'urn:li:activity:' in post_id:
                post_url = f"{config.URLS['POST_BASE']}{post_id}/"
            
            has_email = bool(post_data.get('email'))
            has_phone = bool(post_data.get('phone'))
            is_job_post = bool(post_data.get('has_job'))
            is_ai_related = bool(post_data.get('is_relevant'))
            
            con.execute("""
                INSERT OR REPLACE INTO posts 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post_id, post_url, keyword, post_data.get('name', ''),
                post_preview, post_data.get('profile_url', ''),
                has_email, has_phone, is_job_post, is_ai_related,
                datetime.now()
            ))
            
            con.close()
            return True
        except Exception as e:
            logger.error(f"Error saving metadata to DuckDB: {e}", extra={"step_name": "Persistence", "post_id": post_id}, exc_info=True)
            return False

    def save_post_to_csv(self, post_data, keyword):
        """Save post data to posts.csv."""
        if config.DRY_RUN: 
            logger.info(f"[DRY RUN] Would save post CSV row for {post_data.get('post_id')}", extra={"step_name": "Persistence"})
            return True
        
        try:
            filepath = os.path.join(self.output_date_dir, 'posts.csv')
            file_exists = os.path.exists(filepath)
            
            with open(filepath, 'a', newline='', encoding='utf-8') as f:
                fieldnames = ['post_id', 'post_url', 'author_name', 'post_text', 'profile_url', 'search_keyword', 'extraction_date']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                # Extract post ID from metadata if available later, or pass it in. 
                # Ideally this method should take raw data. Assumes post_data has keys.
                # post_data is from Scraper.extract_post_data
                writer.writerow({
                    'post_id': post_data.get('post_id', 'N/A'), # Requires scraper to inject ID
                    'post_url': post_data.get('post_url', ''),
                    'author_name': post_data.get('name', ''),
                    'post_text': post_data.get('post_text', ''),
                    'profile_url': post_data.get('profile_url', ''),
                    'search_keyword': keyword,
                    'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
            return True
        except Exception as e:
            logger.error(f"Error saving to posts.csv: {e}", extra={"step_name": "Persistence"}, exc_info=True)
            return False

    def save_contact(self, data, keyword):
        """Save to emails.csv."""
        if config.DRY_RUN:
            logger.info(f"Dry Run skipping CSV and Sync for: {data.get('full_name')}", extra={"step_name": "Persistence"})
            return True
            
        filepath = os.path.join(self.output_date_dir, 'emails.csv')
        file_exists = os.path.exists(filepath)
        
        try:
            with open(filepath, 'a', newline='', encoding='utf-8') as f:
                fieldnames = [
                    'full_name', 'email', 'phone', 'linkedin_id',
                    'company_name', 'location', 'post_url', 'extraction_date', 'search_keyword'
                ]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow({
                    'full_name': data.get('full_name', '') or 'Unknown',
                    'email': data.get('email', ''),
                    'phone': data.get('phone', ''),
                    'linkedin_id': data.get('linkedin_id', ''),
                    'company_name': data.get('company_name', ''),
                    'location': data.get('location', ''),
                    'post_url': data.get('post_url', ''),
                    'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                    'search_keyword': keyword
                })
            
            self.extracted_contacts_buffer.append(data)
            return True
        except Exception as e:
            logger.error(f"ERROR SAVING contact: {e}", extra={"step_name": "Persistence"}, exc_info=True)
            return False
