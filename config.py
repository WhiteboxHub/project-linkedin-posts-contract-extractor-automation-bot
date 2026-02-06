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
SELECTORS = {
    "login": {
        "username": ["username", "session_key", "email-or-phone"],
        "password": ["password", "session_password"]
    },
    "search": {
        "global_input": [
            "//input[@data-view-name='search-global-typeahead-input']",
            "//input[contains(@class, 'search-global-typeahead__input')]",
            "//input[@placeholder='Search']"
        ],
        "posts_tab_button": [
            "//button[contains(., 'Posts')]",
            "//div[@id='search-reusables__filters-bar']//button[contains(., 'Posts')]"
        ],
        "sort_filter": {
             "dropdown_button": [
                 "//button[contains(., 'Sort by')]",
                 "//button[@aria-label='Sort by']"
             ],
             "option_latest": [
                 "//label[contains(., 'Latest')]",
                 "//span[contains(., 'Latest')]"
             ],
             "option_relevance": [
                 "//label[contains(., 'Top match')] | //label[contains(., 'Relevance')]",
                 "//span[contains(., 'Top match')]"
             ],
             "show_results_button": [
                 "//button[contains(., 'Show results')]",
                 "//button[contains(@class, 'search-reusables__filter-show-results-button')]"
             ]
        }
    },
    "post": {
        "containers": [
            "//div[@data-component-type='LazyColumn']//div[@role='listitem']", # [NEW] User specified LazyColumn
            "//div[@role='listitem']", # [NEW] Generic listitem
            "//div[contains(@class, 'feed-shared-update-v2')]",
            "//div[@data-view-name='feed-full-update']",
            "//div[contains(@class, 'reusable-search__result-container')]",
            "//li[contains(@class, 'reusable-search__result-container')]",
            "//div[contains(@id, 'ember') and contains(@class, 'search-results__list-item')]",
            "//*[contains(@data-view-name, 'feed-commentary')]/ancestor::div[contains(@class, 'update-v2')]",
            "//*[contains(@data-testid, 'expandable-text-box')]/ancestor::div[contains(@class, 'update-v2')]",
            "//div[contains(@class, 'feed-shared-update-v3')]",
            "//div[contains(@class, 'card-container')]"
        ],
        "see_more_button": [
             "//button[@data-testid='expandable-text-button']", # [NEW] User specified
            ".//button[@data-testid='expandable-text-button']",
            ".//button[contains(@class, 'see-more')]",
            ".//button[contains(., 'more')]",
            ".//span[contains(., '...more')]",
            ".//button[contains(@aria-label, 'see more')]"
        ],
        "load_more_results": [
            "//button[.//span[contains(text(), 'Load more')]]",
            "//button[contains(., 'Load more')]",
            "//button[contains(., 'Show more results')]",
            "//button[contains(@class, 'infinite-scroll')]"
        ],
        "author_name": [
            ".//span[@aria-hidden='true']",
            ".//span[contains(@class, 'update-components-actor__name')]",
            ".//span[contains(@class, 'entity-result__title-text')]",
            ".//div[contains(@class, 't-black')]//span",
            ".//div[contains(@class, 'actor')]//span",
            ".//p[contains(@class, 'actor')]//span",
            ".//a[contains(@href, '/in/')]//span"
        ],
        "author_headline": [
            ".//span[contains(@class, 'update-components-actor__description')]",
            ".//div[contains(@class, 'entity-result__primary-subtitle')]",
            ".//p[contains(@class, 't-12')]",
            ".//div[contains(@class, 'update-components-text-view')]"
        ],
        "content_text": [
            ".//div[@data-view-name='feed-commentary']", # [NEW] Prioritize per user
            ".//p[@data-view-name='feed-commentary']",   # [NEW] Prioritize per user
            ".//p[@data-view-name='feed-commentary']//span[@data-testid='expandable-text-box']",
            ".//div[@data-view-name='feed-commentary']//span[@data-testid='expandable-text-box']",
            ".//span[@data-testid='expandable-text-box']",
            ".//div[contains(@class, 'update-components-text')]",
            ".//span[contains(@class, 'break-words')]",
            ".//div[contains(@class, 'feed-shared-update-v2__description')]",
            ".//div[contains(@class, 'feed-shared-update-v2__commentary')]",
            ".//div[@data-view-name='feed-full-update']//span"
        ],
        "profile_link": [
            ".//a[contains(@href, '/in/')]",
            ".//a[contains(@class, 'update-components-actor__container-link')]",
            ".//a[contains(@class, 'app-aware-link') and contains(@href, '/in/')]",
            ".//a[contains(@data-test-app-aware-link, '') and contains(@href, '/in/')]"
        ],
        "extract_id": {
            "urn_component": [
                ".//*[@componentkey or @data-urn]",
                ".//*[@data-activity-urn]"
            ],
            "time_link": [
                ".//a[contains(@href, 'feed/update/urn:li:activity:')]",
                ".//a[contains(@href, '/feed/update/')]"
            ],
            "copy_link_text": [
                ".//*[contains(text(), 'Copy link to post')]",
                ".//li-icon[contains(@type, 'link')]"
            ]
        }
    },
    "profile": {
        "full_name": [
            "//h1[contains(@class, 't-24')]",
            "//h1"
        ],
        "location": [
            "//span[contains(@class, 'text-body-small') and contains(., ',')]",
            "//div[contains(@class, 'mt2')]//span[contains(text(), ',')]"
        ],
        "company": [
            "//div[contains(@class, 'inline-show-more-text')]//div[contains(@class, 'inline-show-more-text--is-collapsed')]",
            "//section[contains(@id, 'experience')]//div[contains(@class, 'pvs-entity__path-node')]//span[contains(@aria-hidden, 'true')]",
            "//section[contains(@id, 'experience')]//span[contains(@class, 't-bold')]",
            "//div[@id='experience']//span[@aria-hidden='true']"
        ],
        "contact_info_link": "//a[contains(@href, 'overlay/contact-info')]",
        "email_mailto": "//a[contains(@href, 'mailto:')]",
        "phone_section": "//section[contains(@class, 'pv-contact-info')]"
    }
}
