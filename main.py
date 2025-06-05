import os
import json
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re

# Load environment variables from .env file
load_dotenv()

# Retrieve API keys and database IDs from environment variables
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")

def load_channel_ids():
    """
    Loads YouTube channel IDs from a 'channels.txt' file.
    Each channel ID should be on a new line.
    Handles FileNotFoundError if 'channels.txt' does not exist.
    """
    try:
        with open('channels.txt', 'r') as file:
            # Read each line, strip whitespace, and filter out empty lines
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print("Error: 'channels.txt' not found. Please create this file in the same directory "
              "as the script and add YouTube channel IDs, one per line.")
        return []

# Load channel IDs globally when the script starts
CHANNEL_IDS = load_channel_ids()

def get_existing_video_ids():
    """
    Retrieves all existing video IDs from the Notion database to prevent duplicates.
    
    Returns:
        set: A set of existing video IDs from URLs in the database.
    """
    existing_ids = set()
    has_more = True
    next_cursor = None
    
    while has_more:
        url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
        headers = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        
        payload = {"page_size": 100}  # Maximum page size for efficiency
        if next_cursor:
            payload["start_cursor"] = next_cursor
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # Extract video IDs from URLs
            for page in data.get('results', []):
                url_property = page.get('properties', {}).get('URL', {})
                if url_property.get('type') == 'url' and url_property.get('url'):
                    video_url = url_property['url']
                    # Extract video ID from YouTube URL
                    video_id_match = re.search(r'(?:v=|/)([a-zA-Z0-9_-]{11})', video_url)
                    if video_id_match:
                        existing_ids.add(video_id_match.group(1))
            
            has_more = data.get('has_more', False)
            next_cursor = data.get('next_cursor')
            
        except Exception as e:
            print(f"Error retrieving existing videos: {e}")
            break
    
    print(f"Found {len(existing_ids)} existing videos in database")
    return existing_ids

def parse_duration(duration_str):
    """
    Parses YouTube's ISO 8601 duration format (PT#M#S) to seconds.
    
    Args:
        duration_str (str): Duration in ISO 8601 format (e.g., "PT5M30S", "PT1H2M3S")
    
    Returns:
        int: Duration in seconds, or 0 if parsing fails
    """
    if not duration_str:
        return 0
    
    # Parse ISO 8601 duration format
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_str)
    
    if not match:
        return 0
    
    hours, minutes, seconds = match.groups()
    total_seconds = 0
    
    if hours:
        total_seconds += int(hours) * 3600
    if minutes:
        total_seconds += int(minutes) * 60
    if seconds:
        total_seconds += int(seconds)
    
    return total_seconds

def format_duration(seconds):
    """
    Converts duration in seconds to MM:SS or HH:MM:SS format.
    
    Args:
        seconds (int): Duration in seconds
    
    Returns:
        str: Formatted duration string (e.g., "12:14" or "1:05:30")
    """
    if seconds == 0:
        return "0:00"
    
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes}:{seconds:02d}"

def get_last_24h_videos_with_duration(youtube_service, channel_id, existing_video_ids):
    """
    Fetches videos published in the last 24 hours for a given YouTube channel ID,
    filters by duration (>5 minutes), and excludes existing videos.

    Args:
        youtube_service: An initialized YouTube API client object.
        channel_id (str): The ID of the YouTube channel.
        existing_video_ids (set): Set of existing video IDs to avoid duplicates.

    Returns:
        list: A list of dictionaries containing video details for videos >5 minutes.
    """
    now = datetime.now(timezone.utc)
    twenty_four_hours_ago = now - timedelta(hours=24)

    # Format timestamps for YouTube API compatibility
    start_time = twenty_four_hours_ago.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        # Step 1: Get video list from search API
        search_request = youtube_service.search().list(
            part='snippet',
            channelId=channel_id,
            publishedAfter=start_time,
            publishedBefore=end_time,
            maxResults=50,
            order='date',
            type='video'
        )
        search_response = search_request.execute()
        
        # Filter out existing videos and extract video IDs
        new_video_ids = []
        video_snippets = {}
        
        for item in search_response.get('items', []):
            if 'videoId' in item['id']:
                video_id = item['id']['videoId']
                if video_id not in existing_video_ids:
                    new_video_ids.append(video_id)
                    video_snippets[video_id] = item['snippet']
        
        if not new_video_ids:
            print(f"No new videos found for channel {channel_id}")
            return []
        
        print(f"Found {len(new_video_ids)} new videos, checking durations...")
        
        # Step 2: Get video details including duration (batch request for efficiency)
        videos_request = youtube_service.videos().list(
            part='contentDetails',
            id=','.join(new_video_ids)  # Batch request for up to 50 videos
        )
        videos_response = videos_request.execute()
        
        # Step 3: Filter by duration and prepare final list
        valid_videos = []
        for video in videos_response.get('items', []):
            video_id = video['id']
            duration_str = video.get('contentDetails', {}).get('duration', '')
            duration_seconds = parse_duration(duration_str)
            
            # Only include videos longer than 5 minutes (300 seconds)
            if duration_seconds > 300:
                snippet = video_snippets.get(video_id, {})
                valid_videos.append({
                    'title': snippet.get('title', ''),
                    'video_id': video_id,
                    'channel_title': snippet.get('channelTitle', ''),
                    'published_at': snippet.get('publishedAt', ''),
                    'duration_seconds': duration_seconds,
                    'duration_formatted': format_duration(duration_seconds)
                })
        
        print(f"Found {len(valid_videos)} videos longer than 5 minutes")
        return valid_videos
        
    except HttpError as e:
        print(f"Error fetching videos for channel {channel_id}: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while fetching videos for channel {channel_id}: {e}")
        return []

def add_videos_to_notion_batch(videos):
    """
    Adds multiple videos to Notion database efficiently.
    
    Args:
        videos (list): List of video dictionaries to add.
    
    Returns:
        tuple: (success_count, failed_count)
    """
    if not videos:
        return 0, 0
    
    success_count = 0
    failed_count = 0
    
    # Process videos individually (Notion doesn't support true batch creation)
    # But we optimize by reusing the session and preparing data efficiently
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    })
    
    url = "https://api.notion.com/v1/pages"
    
    for video in videos:
        data = {
            "parent": {"database_id": NOTION_DB_ID},
            "properties": {
                "Title": {"title": [{"text": {"content": video['title']}}]},
                "URL": {"url": f"https://www.youtube.com/watch?v={video['video_id']}"},
                "Channel": {"rich_text": [{"text": {"content": video['channel_title']}}]},
                "Date": {"date": {"start": video['published_at']}},
                "Length": {"rich_text": [{"text": {"content": video['duration_formatted']}}]}
            }
        }
        
        try:
            response = session.post(url, json=data)
            response.raise_for_status()
            success_count += 1
            print(f"✅ Added: {video['title'][:50]}... ({video['duration_formatted']})" if len(video['title']) > 50 else f"✅ Added: {video['title']} ({video['duration_formatted']})")
        except requests.exceptions.RequestException as e:
            failed_count += 1
            print(f"❌ Failed: {video['title'][:50]}..." if len(video['title']) > 50 else f"❌ Failed: {video['title']}")
            print(f"   Error: {e}")
    
    return success_count, failed_count

def main():
    """
    Main function to orchestrate fetching videos and adding them to Notion.
    """
    # Validate that all required environment variables are set
    if not YOUTUBE_API_KEY:
        print("Error: YOUTUBE_API_KEY environment variable not found. Please set it.")
        return
    if not NOTION_TOKEN:
        print("Error: NOTION_TOKEN environment variable not found. Please set it.")
        return
    if not NOTION_DB_ID:
        print("Error: NOTION_DB_ID environment variable not found. Please set it.")
        return

    # Initialize the YouTube API service
    youtube_service = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    # Check if any channel IDs were loaded
    if not CHANNEL_IDS:
        print("No channel IDs loaded. Please ensure 'channels.txt' exists and contains channel IDs.")
        return

    # Get existing video IDs to prevent duplicates
    print("Retrieving existing videos from Notion database...")
    existing_video_ids = get_existing_video_ids()

    total_success = 0
    total_failed = 0

    # Process each channel
    for channel_id in CHANNEL_IDS:
        print(f"\n--- Processing channel: {channel_id} ---")
        
        # Get new videos (>5 minutes, not in database)
        videos = get_last_24h_videos_with_duration(youtube_service, channel_id, existing_video_ids)

        if videos:
            print(f"Adding {len(videos)} new videos to Notion...")
            success, failed = add_videos_to_notion_batch(videos)
            total_success += success
            total_failed += failed
            
            # Add new video IDs to existing set to prevent duplicates across channels
            for video in videos:
                existing_video_ids.add(video['video_id'])
        else:
            print(f"No new videos >5 minutes found for channel {channel_id} in the last 24 hours")

    print(f"\n=== Summary ===")
    print(f"✅ Successfully added: {total_success} videos")
    print(f"❌ Failed to add: {total_failed} videos")

if __name__ == "__main__":
    main()