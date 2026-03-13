import os
import csv
import json
import duckdb
from datetime import datetime
import config
from modules.logger import logger

class StorageManager:
    """
    Manages data persistence including DuckDB, CSV exports, 
    and file system storage for post content.
    """
    def __init__(self, candidate_id=None, candidate_email=None):
        self.current_date_str = datetime.now().strftime('%Y-%m-%d')
        self.candidate_id = candidate_id
        self.candidate_email = candidate_email
        
        # [UPDATED] Date-based structure for RAW posts
        self.base_raw_dir = "data/raw_posts"
        self.current_raw_dir = os.path.join(self.base_raw_dir, self.current_date_str)
        
        if not os.path.exists(self.current_raw_dir):
            os.makedirs(self.current_raw_dir)
            
        # Output directory for CSVs (also date based)
        self.output_date_dir = os.path.join("data/output", self.current_date_str)
        if not os.path.exists(self.output_date_dir):
            os.makedirs(self.output_date_dir)
            
        self.posts_dir = self.current_raw_dir # Redirect posts_dir to the daily folder
            
        self.processed_profiles = set()
        self.processed_posts = set()
        self.extracted_contacts_buffer = []
        self.db_file = 'linkedin_data.db'
        
        # In-memory cache for JSON data to avoid repeated file reads
        self.posts_json_cache = {}
        
        self.load_processed_posts()
        self.profile_cache = {}
        self.load_processed_profiles()

    def cleanup_old_data(self, days=7):
        """Remove raw_posts folders older than 'days'."""
        try:
            if not os.path.exists(self.base_raw_dir):
                return
                
            cutoff_time = datetime.now().timestamp() - (days * 86400)
            
            for item in os.listdir(self.base_raw_dir):
                item_path = os.path.join(self.base_raw_dir, item)
                if os.path.isdir(item_path):
                    # Check if it matches YYYY-MM-DD format
                    try:
                        folder_date = datetime.strptime(item, '%Y-%m-%d')
                        # Check modification time or just date? Date in name is safer.
                        folder_ts = folder_date.timestamp()
                        
                        if folder_ts < cutoff_time:
                            import shutil
                            shutil.rmtree(item_path)
                            logger.info(f"Cleaned up old data folder: {item}", extra={"step_name": "Storage Cleanup"})
                    except ValueError:
                        pass # Not a date folder
        except Exception as e:
            logger.error(f"Cleanup failed: {e}", extra={"step_name": "Storage Cleanup"})
        self.db_file = 'linkedin_data.db'
        
        # In-memory cache for JSON data to avoid repeated file reads
        self.posts_json_cache = {}
        
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
        """Profile caching is now handled via DuckDB or skipped if not required."""
        pass

    def save_full_post(self, text, post_id, keyword, metadata=None):
        """Save the actual post content to JSON file (text only, no images)."""
        if config.DRY_RUN:
            logger.info(f"[DRY RUN] Would save full post text for {post_id} to JSON", extra={"step_name": "Persistence"})
            return True
            
        try:
            # Load existing posts for this keyword
            posts_data = self._load_posts_json(keyword)
            
            # Check if this post_id already exists in the JSON file
            existing_post_ids = {post.get('post_id') for post in posts_data}
            if post_id in existing_post_ids:
                logger.debug(f"Post {post_id} already exists in JSON, skipping", extra={"step_name": "Persistence"})
                return True
            
            # Split text into lines for vertical format in JSON
            post_lines = text.split('\n') if text else []
            
            # Extract author information from metadata if available
            author_name = metadata.get('full_name', '') if metadata else ''
            linkedin_id = metadata.get('linkedin_id', '') if metadata else ''
            
            # Create new post entry (text only, no images or other media)
            new_post = {
                "post_id": post_id,
                "post_url": metadata.get('post_url', '') if metadata else '',
                "author_name": author_name,
                "linkedin_id": linkedin_id,
                "post_text": post_lines,  # Store as array of lines
                "extraction_date": datetime.now().strftime('%Y-%m-%d'),
                "search_keyword": keyword,
                "candidate_id": self.candidate_id,
                "candidate_email": self.candidate_email
            }
            
            # Append to posts array
            posts_data.append(new_post)
            
            # Save back to JSON file
            self._save_posts_json(keyword, posts_data)
            
            # Also save to CSV as per existing requirement
            if metadata:
                # Ensure post_id is in metadata if not already
                if 'post_id' not in metadata: 
                    metadata['post_id'] = post_id
                self.save_post_to_csv(metadata, keyword)
            
            return True
        except Exception as e:
            logger.error(f"Error saving post to JSON: {e}", extra={"step_name": "Persistence", "post_id": post_id}, exc_info=True)
            return False

    def _load_posts_json(self, keyword):
        safe_keyword = keyword.replace(' ', '_').replace('/', '_')
        # Check cache first
        if safe_keyword in self.posts_json_cache:
            return self.posts_json_cache[safe_keyword]
        filename = f"{safe_keyword}_posts.json"
        filepath = os.path.join(self.posts_dir, filename)
        if not os.path.exists(filepath):
            self.posts_json_cache[safe_keyword] = []
            return []
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                posts_data = json.load(f)
                self.posts_json_cache[safe_keyword] = posts_data
                return posts_data
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON file {filepath}: {e}", extra={"step_name": "Persistence"})
            # Return empty list on parse error
            return []
        except Exception as e:
            logger.error(f"Error loading JSON file {filepath}: {e}", extra={"step_name": "Persistence"})
            return []
    
    def _save_posts_json(self, keyword, posts_data):
        safe_keyword = keyword.replace(' ', '_').replace('/', '_')
        filename = f"{safe_keyword}_posts.json"
        filepath = os.path.join(self.posts_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(posts_data, f, indent=2, ensure_ascii=False)
            
            # Update cache
            self.posts_json_cache[safe_keyword] = posts_data
            logger.debug(f"Saved {len(posts_data)} posts to {filename}", extra={"step_name": "Persistence"})
            return True
        except Exception as e:
            logger.error(f"Error writing JSON file {filepath}: {e}", extra={"step_name": "Persistence"})
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
                writer.writerow({
                    'post_id': post_data.get('post_id', 'N/A'), 
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
