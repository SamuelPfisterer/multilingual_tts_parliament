from playwright.sync_api import sync_playwright
from typing import Union

def get_transcript_link(page) -> Union[str, None]:
    """Extract the transcript link from a session page."""
    try:
        # Get all matching tables
        tables = page.query_selector_all('table.table.table-bordered')
        
        for table in tables:
            rows = table.query_selector_all('tr')
            
            for row in rows:
                cell = row.query_selector('td')
                if cell and "Ülésnap összes felszólalás szövege" in cell.inner_text().strip():
                    link = row.query_selector('td:nth-child(2) a')
                    if link:
                        return link.get_attribute('href')
        return None
    except Exception as e:
        raise ValueError(f"Error in get_transcript_link: {e}")

def extract_text_from_page(page) -> str:
    """Extract text content from a transcript page."""
    try:
        # Wait for the content to load
        page.wait_for_selector('div[xmlns="http://www.w3.org/1999/xhtml"]')
        
        # Get all text elements using Playwright
        text_elements = page.query_selector_all(
            'div[xmlns="http://www.w3.org/1999/xhtml"] p, ' +
            'div[xmlns="http://www.w3.org/1999/xhtml"] h1, ' +
            'div[xmlns="http://www.w3.org/1999/xhtml"] h2, ' +
            'div[xmlns="http://www.w3.org/1999/xhtml"] h3, ' +
            'div[xmlns="http://www.w3.org/1999/xhtml"] h4, ' +
            'div[xmlns="http://www.w3.org/1999/xhtml"] h5, ' +
            'div[xmlns="http://www.w3.org/1999/xhtml"] h6'
        )
        
        # Extract text from each element
        texts = [element.inner_text().strip() for element in text_elements if element.inner_text().strip()]
        return '\n\n'.join(texts)
    except Exception as e:
        raise ValueError(f"Error in extract_text_from_page: {e}")

def process_transcript_html(url: str) -> str:
    """
    Process Hungarian parliament transcript and return HTML content.
    
    Args:
        url: The URL to the session page
        
    Returns:
        str: The HTML content of the transcript
        
    Raises:
        ValueError: If processing fails
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            # First get the transcript link from the session page
            page.goto(url, wait_until='networkidle')
            transcript_url = get_transcript_link(page)
            if not transcript_url:
                raise ValueError(f"No transcript link found for session: {url}")
            
            # Then get the actual transcript content
            page.goto(transcript_url, wait_until='networkidle')
            content = page.content()
            return content
            
        except Exception as e:
            raise ValueError(f"Failed to process HTML transcript: {str(e)}")
        finally:
            browser.close()

def process_transcript_text(url: str) -> str:
    """
    Process Hungarian parliament transcript and return text content.
    
    Args:
        url: The URL to the session page
        
    Returns:
        str: The extracted text content
        
    Raises:
        ValueError: If processing fails
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            # First get the transcript link from the session page
            page.goto(url, wait_until='networkidle')
            transcript_url = get_transcript_link(page)
            if not transcript_url:
                raise ValueError(f"No transcript link found for session: {url}")
            
            # Then get the actual transcript content
            page.goto(transcript_url, wait_until='networkidle')
            content = extract_text_from_page(page)
            return content
            
        except Exception as e:
            raise ValueError(f"Failed to process text transcript: {str(e)}")
        finally:
            browser.close() 