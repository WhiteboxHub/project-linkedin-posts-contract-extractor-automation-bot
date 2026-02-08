"""Configuration settings."""
import os
from dotenv import load_dotenv

load_dotenv()

# LinkedIn Credentials
LINKEDIN_EMAIL = os.getenv('LINKEDIN_EMAIL')
LINKEDIN_PASSWORD = os.getenv('LINKEDIN_PASSWORD')

# Proxy Settings
# PROXY_URL = os.getenv('PROXY_URL') # Format: http://user:pass@host:port or host:port

# LinkedIn URLs
URLS = {
    "FEED": "https://www.linkedin.com/feed/",
    "LOGIN": "https://www.linkedin.com/login",
    "SEARCH": "https://www.linkedin.com/search/results/content/",
    "POST_BASE": "https://www.linkedin.com/feed/update/"
}

# Search Filters (URL Parameters)
SEARCH_FILTERS = {
    "DATE_POSTED": {
        "past-24h": "%5B%22past-24h%22%5D",
        "past-week": "%5B%22past-week%22%5D",
        "past-month": "%5B%22past-month%22%5D"
    },
    "SORT_BY": {
        "date_posted": "%5B%22date_posted%22%5D",
        "relevance": "relevance" # Usually empty or different, but keeping strict param structure if needed. 
        # Actually, looking at scraper.py, the default sort was just hardcoded. 
        # Let's verify the "relevance" param or leave it as a placeholder if not strictly used in URL same way.
        # But for now, putting the encoded value for date_posted.
    }
}

# Search Settings
KEYWORDS_FILE = os.getenv('KEYWORDS_FILE', 'keywords.json')
DATE_FILTER = os.getenv('DATE_FILTER', 'past-24h') # 'past-24h', 'past-week', 'past-month'
SORT_BY = 'date_posted' # Forced to date_posted as per user request
MAX_CONTACTS_PER_RUN = int(os.getenv('MAX_CONTACTS_PER_RUN', '100'))
DRY_RUN = os.getenv('DRY_RUN', 'False').lower() == 'true'

# Output Settings
OUTPUT_FILE = os.getenv('OUTPUT_FILE', 'output.csv')

# AI/ML Related Keywords to filter posts
AI_KEYWORDS = [
    'ai', 'artificial intelligence', 'machine learning', 'ml', 'mlops',
    'llm', 'large language model', 'rag', 'gen ai', 'generative ai', 'agentic',
    'deep learning', 'neural network', 'nlp', 'computer vision',
    'developer','data engineer','data scientist', 'ml engineer', 'ai engineer', 'pytorch', 'tensorflow','python','Fine tuning'
]

# Job-related keywords (must have at least one)
JOB_KEYWORDS = [
    'hiring', 'job', 'position', 'opportunity', 'opening',
    'w2', 'c2c', 'corp-to-corp', '1099', 'bench', 'full time', 'full-time', 
    'contract', 'immediate', 'looking for', 'seeking', 'recruiting', 
    'join our team', 'apply', 'careers', 'employment'
]



# Chrome Profile Settings for persistent login
# Path to your Chrome User Data (e.g., C:/Users/YourName/AppData/Local/Google/Chrome/User Data)
CHROME_PROFILE_PATH = os.getenv('CHROME_PROFILE_PATH')
# Name of the profile folder (e.g., "Default" or "Profile 1")
CHROME_PROFILE_NAME = os.getenv('CHROME_PROFILE_NAME', 'Default')
# Force a specific Chrome version for the driver (e.g., 144)
CHROME_VERSION = os.getenv('CHROME_VERSION') # Leave empty for auto-detection

# LinkedIn Selectors
# LinkedIn Selectors - Loaded from JSON
import json
try:
    with open('selectors.json', 'r') as f:
        SELECTORS = json.load(f)
except FileNotFoundError:
    print("Error: selectors.json not found. Creating default empty selectors.")
    SELECTORS = {}
except json.JSONDecodeError as e:
    print(f"Error parsing selectors.json: {e}")
    SELECTORS = {}
