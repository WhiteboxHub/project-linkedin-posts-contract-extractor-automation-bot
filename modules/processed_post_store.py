import os
import sys
import atexit
import signal
import threading
from pathlib import Path
from datetime import datetime
import time # Added time for example usage
import random # Added random for example usage

class ProcessedPostStore:
    """
    A crash-safe, daily, in-memory deduplication system for processed items.
    
    Features:
    - Maintains in-memory set of processed IDs.
    - Persists to daily files (e.g., data/processed_posts/2023-10-27.txt).
    - Uses atomic writes to prevent corruption.
    - Thread-safe operations.
    - Registers shutdown hooks for safe exit.
    """
    
    def __init__(self, base_dir="data/processed_posts"):
        self.base_dir = Path(base_dir)
        self.lock = threading.Lock()
        self.processed_ids = set()
        self.dirty = False
        
        # Set initialization time for daily file
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.file_path = self.base_dir / f"{self.current_date}.txt"
        
        # Ensure directory exists
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing state
        self._load()
        
        # Register handlers
        self._register_handlers()
        
    def _load(self):
        """Load processed IDs from today's file if it exists."""
        with self.lock:
            if self.file_path.exists():
                try:
                    with open(self.file_path, "r", encoding="utf-8") as f:
                        for line in f:
                            cleaned = line.strip()
                            if cleaned:
                                self.processed_ids.add(cleaned)
                    print(f"[ProcessedPostStore] Loaded {len(self.processed_ids)} IDs from {self.file_path}")
                except Exception as e:
                    print(f"[ProcessedPostStore] Error loading file {self.file_path}: {e}", file=sys.stderr)
            else:
                print(f"[ProcessedPostStore] No existing file for today ({self.current_date}). Starting fresh.")

    def add(self, post_id):
        """
        Mark a post ID as processed. 
        Returns True if newly added, False if already present.
        """
        if not post_id:
            return False
            
        with self.lock:
            if post_id in self.processed_ids:
                return False
            
            self.processed_ids.add(post_id)
            self.dirty = True
            return True

    def is_processed(self, post_id):
        """Check if a post ID has been processed."""
        if not post_id:
            return False
        with self.lock:
            return post_id in self.processed_ids

    def save(self):
        """
        Persist current state to disk using atomic write.
        Rewrites the entire file from memory to ensure consistency.
        """
        with self.lock:
            # Optimized: only save if dirty or if file doesn't exist yet but we have data
            if not self.dirty and self.file_path.exists():
                return
            
            # If no data and no file, maybe skip? But requirement says "On exit, always update".
            # If we have data, we save.

            # Atomic write pattern: write to .tmp then rename
            # Use same dir as target for atomic rename
            temp_path = self.file_path.with_suffix(".tmp")
            
            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    for pid in self.processed_ids:
                        f.write(f"{pid}\n")
                        
                    # Force flush to disk
                    f.flush()
                    os.fsync(f.fileno())
                        
                # Atomic replace
                os.replace(temp_path, self.file_path)
                self.dirty = False
                print(f"[ProcessedPostStore] Saved {len(self.processed_ids)} IDs to {self.file_path}")
            except Exception as e:
                print(f"[ProcessedPostStore] Failed to save state: {e}", file=sys.stderr)
                # Attempt cleanup of temp file
                if temp_path.exists():
                    try:
                        os.remove(temp_path)
                    except: pass

    def _register_handlers(self):
        """Register signal and exit handlers to ensure data is flushed."""
        
        # 1. Normal Exit via atexit
        atexit.register(self.save)
        
        # 2. Signal Handlers (SIGTERM, SIGINT)
        def _signal_handler(signum, frame):
            print(f"\n[ProcessedPostStore] Shutdown signal received ({signum}). Saving state...")
            self.save()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
        
        # 3. Unhandled Exceptions
        # Wrap original hook to not break other exception handling
        # Only set if not already custom, or chain it? simplest is chain.
        original_excepthook = sys.excepthook
        
        def _exception_handler(exc_type, exc_value, exc_traceback):
            print(f"\n[ProcessedPostStore] Unhandled exception: {exc_value}. Saving state...")
            self.save()
            original_excepthook(exc_type, exc_value, exc_traceback)
            
        sys.excepthook = _exception_handler

# ==========================================
# Example Usage & Verification
# ==========================================
if __name__ == "__main__":
    
    print("--- Starting ProcessedPostStore Demo ---")
    
    # 1. Initialize Store
    store = ProcessedPostStore()
    
    # Example Scraper Function Simulation
    def run_mock_scraper():
        print("Scraper started. loops=50. Press Ctrl+C to test crash safety.")
        
        keywords = ["AI", "Python", "Data"]
        
        try:
            for i in range(1, 51):
                # Simulate finding a post
                # Use predictable IDs for testing reloading
                mock_post_id = f"urn:li:activity:10000{i}" 
                keyword = random.choice(keywords)
                
                print(f"[{i}] Processing {mock_post_id}...")
                
                # Check duplication
                if store.is_processed(mock_post_id):
                    print(f" -> SKIPPING: Already processed.")
                    continue
                
                # Simulate "Processing" work
                time.sleep(0.05) 
                
                # Mark as processed
                if store.add(mock_post_id):
                    print(f" -> SUCCESS: Added.")
                
                # Occasional manual save
                if i % 10 == 0:
                    store.save()
                    
        except KeyboardInterrupt:
            print("\n[Main] User interrupted.")
            
    # Run the mock scraper
    run_mock_scraper()
    
    print("--- End of Demo ---")
