# Icelandic Parliament (Althingi) Data Collection Project

This project aims to collect and process parliamentary data from the Icelandic Parliament (Althingi) website, including transcripts, audio, and video recordings.

## Current State

### Data Sources and Collection Statistics
- Total transcript links collected: 72,925
- Successfully retrieved media links:
  - MP4 video links: 59,186 (confirmed in danish_mp4_media_links_since_2018.csv)
  - MP3 audio links: 59,350
  - Success rate for media link extraction: ~81%
- Source: Althingi website (www.althingi.is)
- Time period: Includes session 149 (2018) onwards

### Media Links Structure
- Video files are stored at vod.althingi.is
- URL pattern: `https://vod.althingi.is/upptokur/old/YYYYMMDDTHHMMSS.mp4`
- Multiple transcripts can reference the same video file
  - Each video file typically contains a complete session
  - Different transcript_ids may point to different segments of the same video
- Links are stored in CSV format with transcript_id and mp4_video_link columns

### Directory Structure
- `getting_links/`: Scripts for retrieving media links and transcript texts
  - Contains scripts for scraping transcript pages and extracting media URLs
  - Successfully processed ~59k transcripts for media links
- `downloaded_audio/`: Successfully downloaded audio files (2 files)
- `downloaded_transcript/`: Successfully downloaded transcripts (1 file)
- `downloaded_subtitle/`: Directory for subtitles (currently empty)
- `links/`: Contains media link files and dataset preparation scripts
- `scripts/`: Various processing scripts
- `logs/`: Log files from download attempts

### Download Status
- Successfully downloaded:
  - Audio files: 2
  - Transcripts: 1
  - Subtitles: 0
- Failed downloads: 58,177 (recorded in download_results.csv)
  - Primary failure type: audio component downloads
  - Main error: Connection timeouts to vod.althingi.is:443
- Download challenges appear to be related to rate limiting or connection timeouts
  - Success might be improved with better rate limiting and longer timeouts
  - Some videos are shared across multiple transcripts, potentially reducing the total number of required downloads

### Scripts and Processing
1. `getting_links/main.py`: Main script for:
   - Scraping transcript pages
   - Extracting MP4 video URLs
   - Extracting MP3 audio URLs
   - Collecting transcript texts

2. `job.sh` and `test_job.sh`: Scripts for managing download jobs

### Transcript Collection Process
- Transcripts are extracted directly from the parliament website
- Process:
  1. Scrapes the 'raeda_efni' div from each transcript page
  2. Extracts all paragraphs within this div
  3. Joins the paragraphs with spaces to create the full transcript text
- Implementation:
  - Uses cloudscraper to bypass any scraping protections
  - Includes rate limiting (3-7 seconds between requests)
  - Processes in batches (1000 transcripts per batch)
  - Results stored in CSV files:
    - Each batch creates a transcripts_XXXXX-YYYYY.csv file
    - Contains transcript_unique_id and transcript_text
- Success rate for transcript extraction is very high:
  - 59,433 transcripts successfully extracted
  - Most batch files contain between 500-1001 transcripts
  - Larger batches (1000+ transcripts) show consistent success
  - Some smaller batches (500-700 transcripts) in higher ranges (40000+)
  - Overall success rate: ~81.5% (59,433 out of 72,925)

## Known Issues and Challenges
1. Link Retrieval Issues:
   - About 13k transcripts (~19%) failed during initial link retrieval
   - Common error: "No 'Horfa' link found" for some transcripts

2. Download Problems:
   - Very low download success rate (3 successful out of 58k+ attempts)
   - Primary issues:
     - Connection timeouts to video server (consistently after 5 seconds)
     - All failures are connection timeouts to vod.althingi.is:443
     - FFmpeg errors during media download
     - Network connectivity issues with vod.althingi.is
   - Current configuration limitations:
     - FFmpeg timeout too short (5 seconds)
     - No exponential backoff between retries
     - Too many concurrent download attempts
     - No session handling or download resumption capability

## Data Collection Process
1. Initial Phase: Transcript Link Collection
   - Gathered 72,925 transcript links from the Althingi website
   - Each link corresponds to a parliamentary speech

2. Media Link Extraction
   - Process each transcript page to extract:
     - Transcript text
     - Associated video links (MP4)
     - Associated audio links (MP3)
   - Successfully extracted ~59k media links

3. Download Attempts
   - Automated download process for media files
   - Current success rate is extremely low
   - Failed downloads are logged in download_results.csv

## Next Steps and Recommendations
1. Download Improvement Strategies:
   - Investigate and address connection timeout issues
   - Implement more robust retry mechanism with longer timeouts
   - Consider rate limiting to avoid server restrictions
   - Test alternative download methods

2. Process Optimization:
   - Consider parallel downloading with proper rate limiting
   - Implement better error handling and recovery
   - Add comprehensive logging for better debugging

3. Success Metrics:
   - Monitor success rates for different types of content
   - Track server response patterns
   - Document successful download configurations

## Current State Summary (as of November 2024)

### Successfully Completed
1. Transcript Collection:
   - 59,433 transcripts successfully extracted (81.5% of 72,925 total)
   - Stored in multiple CSV files in getting_links/results/
   - High success rate for text extraction

2. Media Link Identification:
   - 59,186 MP4 video links identified
   - Links stored in danish_mp4_media_links_since_2018.csv
   - Multiple transcripts may share the same video file
   - All links follow pattern: https://vod.althingi.is/upptokur/old/YYYYMMDDTHHMMSS.mp4

### Current Bottlenecks
1. Download Success:
   - Only 2 audio files downloaded
   - Only 1 transcript downloaded
   - 58,177 failed download attempts
   - All failures due to connection timeouts

## Immediate Next Steps

1. Media File Analysis:
   - Count unique video files in the dataset
   - Identify which videos are shared across multiple transcripts
   - Create prioritized download list

2. Download Script Improvements:
   - Increase FFmpeg timeout (from 5 seconds to 30+ seconds)
   - Implement exponential backoff between retry attempts
   - Reduce concurrent downloads to 1-2 at a time
   - Add proper session handling
   - Implement download resumption capability

3. Testing and Validation:
   - Test modified download parameters with small batch
   - Monitor server response patterns
   - Document successful download configurations
   - Implement progress tracking

## Current Download Configuration

### Parallel Processing Setup
- Using SLURM array jobs with up to 40 concurrent jobs
- Each job processes 1000 links
- Total workload: ~59k links split into 60 batches

### Retry Mechanism
- 4 retry attempts per download
- Exponential backoff between retries:
  - 1st retry: 2 minutes wait
  - 2nd retry: 4 minutes wait
  - 3rd retry: 8 minutes wait
  - 4th retry: 16 minutes wait

### Media Processing Configuration
- FFmpeg settings:
  - No explicit timeout setting (defaulting to 5 seconds)
  - Audio conversion to Opus codec
  - Bitrate: 96kbps
  - Mono audio (1 channel)
  - 24kHz sample rate
  - Optimized for speech

### Download Timing Analysis
- Failed attempts:
  - Consistently times out after 5.026 seconds
  - No delay between retry attempts (only milliseconds)
  - With 40 concurrent jobs, ~480 connection attempts per minute
  - Server consistently drops connection at 5-second mark

- Successful downloads:
  - Full session videos (not individual speeches):
    - Each video contains multiple speeches/transcripts
    - Same video being downloaded multiple times unnecessarily
    - Example: 20231106T150544.mp4 downloaded 6+ times for different transcript IDs
  - Download times for full session videos:
    - Average download time: ~34 minutes per session video
    - Range: 12 minutes to 56 minutes
    - Most common duration: 35-45 minutes
  - Progress tracking shows consistent average:
    - Initial downloads slower (50-55 minutes)
    - Stabilizes around 35-40 minutes per session
  - Success rate extremely low (only a few out of 58k+ attempts)

### Critical Issues Identified
1. Redundant Downloads:
   - Same session video downloaded multiple times
   - No deduplication of video requests
   - Each transcript triggers its own download attempt
   - Example: Single session video "20231106T150544.mp4" downloaded 6+ times

2. Download Strategy Problems:
   - Current approach treats each transcript as needing a separate video
   - No mapping of transcripts to unique session videos
   - No tracking of already downloaded sessions
   - Wastes bandwidth and server resources

### Recommended Strategy Change
1. Session-Based Download Approach:
   - First, map all transcripts to their session videos
   - Create list of unique session videos needed
   - Download each session video only once
   - Track which transcripts belong to each session

2. Download Optimization:
   - Reduce redundant downloads by tracking session videos
   - Implement video file existence checking
   - Create session-to-transcript mapping
   - Store timing information for each speech within session

## Recommended Solutions

### Immediate Configuration Changes
1. Reduce Concurrent Load
   - Decrease concurrent jobs from 40 to 2-3
   - Implement proper rate limiting (1 request per 5-10 seconds)
   - Add delays between consecutive downloads within each job

2. Improve Connection Handling
   - Set explicit FFmpeg timeout to 30-60 seconds
   - Add connection pooling for better session management
   - Implement download resumption for interrupted transfers

3. Optimize Retry Strategy
   - Adjust retry intervals to shorter initial waits:
     - 1st retry: 30 seconds
     - 2nd retry: 2 minutes
     - 3rd retry: 5 minutes
     - Final retry: 10 minutes
   - Add connection health checks before retries

### Long-term Improvements
1. Smart Batching
   - Group downloads by video file to avoid duplicate requests
   - Prioritize shorter videos in initial batches
   - Implement success rate monitoring per batch

2. Resilient Download Manager
   - Add download state tracking
   - Implement checkpointing for batch progress
   - Add bandwidth monitoring and dynamic rate limiting
   - Create a download queue with priority handling

3. Monitoring and Analytics
   - Track success rates per time window
   - Monitor server response patterns
   - Log detailed connection metrics
   - Implement automatic rate adjustment based on success rates
