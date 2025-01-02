import cloudscraper
import time

def test_access():
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False
        }
    )
    
    # Test URL (use one that worked before)
    test_url = "YOUR_WORKING_TRANSCRIPT_URL"
    
    print("Testing access...")
    response = scraper.get(test_url)
    print(f"Status code: {response.status_code}")
    
    if 'raeda_efni' in response.text:
        print("Transcript div found!")
    else:
        print("Transcript div not found!")
        print("First 500 chars of response:")
        print(response.text[:500])

test_access()