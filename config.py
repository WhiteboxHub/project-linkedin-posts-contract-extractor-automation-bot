"""Configuration settings."""
import os
from dotenv import load_dotenv

load_dotenv()

# LinkedIn Credentials
LINKEDIN_EMAIL = os.getenv('LINKEDIN_EMAIL')
LINKEDIN_PASSWORD = os.getenv('LINKEDIN_PASSWORD')

# Search Settings
SEARCH_KEYWORDS = os.getenv('SEARCH_KEYWORDS', 'AI AND Engineer AND hiring AND W2')
KEYWORDS_FILE = os.getenv('KEYWORDS_FILE', 'keywords.txt')
DATE_FILTER = os.getenv('DATE_FILTER', 'past-week')
SORT_BY = os.getenv('SORT_BY', 'latest') # 'latest' or 'relevance'

# Output Settings
OUTPUT_FORMAT = os.getenv('OUTPUT_FORMAT', 'csv')
OUTPUT_FILE = os.getenv('OUTPUT_FILE', 'output.csv')

# AI/ML Related Keywords to filter posts
AI_KEYWORDS = [
    'ai', 'artificial intelligence', 'machine learning', 'ml', 'mlops',
    'llm', 'large language model', 'rag', 'gen ai', 'generative ai',
    'deep learning', 'neural network', 'nlp', 'computer vision',
    'data scientist', 'ml engineer', 'ai engineer', 'pytorch', 'tensorflow'
]

# Job-related keywords (must have at least one)
JOB_KEYWORDS = [
    'hiring', 'job', 'position', 'opportunity', 'opening',
    'w2', 'full time', 'full-time', 'contract', 'immediate',
    'looking for', 'seeking', 'recruiting', 'join our team',
    'apply', 'careers', 'employment'
]

# Location keywords (priority for US)
USA_KEYWORDS = [
    'usa', 'united states', 'us', 'america', 'u.s.',
    'us citizen', 'us citizens', 'citizenship',
    'remote', 'remote us', 'us remote'
]
