# main.py
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup
import re
from pathlib import Path
import time
import argparse
import logging
import random  

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_transcript_text(soup):
    """Extract transcript text from the speech content div"""
    transcript_div = soup.find('div', id='raeda_efni')
    if transcript_div:
        # Get all paragraphs and join their text
        paragraphs = transcript_div.find_all('p')
        transcript_text = ' '.join(p.get_text(strip=True) for p in paragraphs)
        return transcript_text
    return None

def process_single_transcript(scraper, row):
    """Process a single transcript URL and return its media links and transcript"""
    try:
        # sleep 
        time.sleep(random.uniform(3, 7))  # 3-7 seconds between requests
        # Get the main transcript page
        response = scraper.get(row['transcript_url'])
        if response.status_code != 200:
            return {'success': False, 'row': row, 'error': f"Status code: {response.status_code}"}
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get transcript text
        transcript_text = get_transcript_text(soup)
        
        # Find the "Horfa" link
        watch_link = soup.find('a', text='Horfa')
        if not watch_link:
            return {'success': False, 'row': row, 'error': "No 'Horfa' link found"}
            
        video_page_url = watch_link['href']

        # Add another delay before getting video page
        time.sleep(random.uniform(1, 3))  # Add delay between page requests

        
        # Get the video page
        video_response = scraper.get(video_page_url)
        if video_response.status_code != 200:
            return {'success': False, 'row': row, 'error': f"Video page status code: {video_response.status_code}"}
            
        video_html = video_response.text
        
        # Extract MP4 link
        mp4_match = re.search(r'(https?://[^"]+\.mp4)', video_html)
        mp4_url = mp4_match.group(1) if mp4_match else None
        
        # Find audio link
        audio_soup = BeautifulSoup(video_html, 'html.parser')
        audio_link = audio_soup.find('a', text=re.compile('Hljóðskrá'))
        
        if audio_link:
            audio_url = audio_link['href']
            if not audio_url.startswith('http'):
                audio_url = f"https://www.althingi.is{audio_url}"
            
            audio_response = scraper.get(audio_url, allow_redirects=True)
            mp3_url = audio_response.url
        else:
            mp3_url = None
        
        return {
            'success': True,
            'data': {
                'transcript_unique_id': row['transcript_unique_id'],
                'mp4_url': mp4_url,
                'mp3_url': mp3_url,
                'transcript_text': transcript_text
            }
        }
                
    except Exception as e:
        return {'success': False, 'row': row, 'error': str(e)}

def process_batch(start_idx, end_idx):
    # Read the full transcript links file
    transcript_links_df = pd.read_csv('all_failed_links.csv')
    
    # Get the batch to process
    batch_df = transcript_links_df.iloc[start_idx:end_idx]
    
    # Initialize cloudscraper
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False
        }
    )
    
    successful_results = []
    failed_results = []
    
    # Process each transcript in the batch
    for idx, row in batch_df.iterrows():
        logger.info(f"Processing transcript {idx - start_idx + 1}/{len(batch_df)}")
        result = process_single_transcript(scraper, row)
        
        if result['success']:
            successful_results.append(result['data'])
        else:
            failed_results.append({
                'transcript_unique_id': result['row']['transcript_unique_id'],
                'transcript_url': result['row']['transcript_url'],
                'error': result['error']
            })
        
    
    # Save results for this batch
    batch_id = f"{start_idx:05d}-{end_idx:05d}"
    
    if successful_results:
        # Save media links
        media_df = pd.DataFrame([{
            'transcript_unique_id': r['transcript_unique_id'],
            'mp4_url': r['mp4_url'],
            'mp3_url': r['mp3_url']
        } for r in successful_results])
        media_df.to_csv(f'results/media_links_{batch_id}.csv', index=False)
        
        # Save transcripts in batch CSV file
        transcripts_df = pd.DataFrame([{
            'transcript_unique_id': r['transcript_unique_id'],
            'transcript_text': r['transcript_text']
        } for r in successful_results])
        transcripts_df.to_csv(f'results/transcripts_{batch_id}.csv', index=False)
    
    if failed_results:
        pd.DataFrame(failed_results).to_csv(f'results/failed_{batch_id}.csv', index=False)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_idx', type=int, required=True)
    parser.add_argument('--end_idx', type=int, required=True)
    args = parser.parse_args()
    
    # Create necessary directories
    Path('results').mkdir(exist_ok=True)
    Path('transcripts').mkdir(exist_ok=True)
    
    # Process the batch
    process_batch(args.start_idx, args.end_idx)

if __name__ == "__main__":
    main()