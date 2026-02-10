import config
from modules.scraper import ScraperModule
from unittest.mock import MagicMock
import urllib.parse

def test_url_construction():
    # Mock browser manager
    mock_browser = MagicMock()
    scraper = ScraperModule(mock_browser)
    
    # Test keyword
    keyword = "test keyword"
    encoded_kw = urllib.parse.quote(keyword)
    
    # Expected components
    base_url = config.URLS['SEARCH']
    sort_param = config.SEARCH_FILTERS['SORT_BY']['date_posted']
    date_param = config.SEARCH_FILTERS['DATE_POSTED']['past-24h'] # default
    
    expected_url = f"{base_url}?keywords={encoded_kw}&sortBy={sort_param}&datePosted={date_param}"
    
    # Mock navigate to capture URL
    mock_browser.navigate.return_value = True
    mock_browser.get_current_url.return_value = '/search/results/content'
    
    scraper.search_posts(keyword)
    
    # Check if navigate was called with expected URL
    mock_browser.navigate.assert_called()
    called_arg = mock_browser.navigate.call_args[0][0]
    
    print(f"Expected URL: {expected_url}")
    print(f"Called URL:   {called_arg}")
    
    assert called_arg == expected_url
    print("SUCCESS: URL construction matches config!")

if __name__ == "__main__":
    test_url_construction()
