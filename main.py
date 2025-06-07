import os
import json
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import re
import time

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
            channel_ids = [line.strip() for line in file if line.strip()]
            print(f"DEBUG: Loaded {len(channel_ids)} channel IDs: {channel_ids}")
            return channel_ids
    except FileNotFoundError:
        print("Error: 'channels.txt' not found. Please create this file in the same directory "
              "as the script and add YouTube channel IDs, one per line.")
        return []

# Load channel IDs globally when the script starts
CHANNEL_IDS = load_channel_ids()

def get_channel_type_mappings():
    """
    Retrieves channel-to-type mappings from the Notion database by looking at 
    entries from the previous week that have both Channel and Type properties set.
    
    Returns:
        dict: A dictionary mapping channel names to their types.
    """
    print("DEBUG: Starting to fetch channel-type mappings from previous week...")
    channel_type_map = {}
    has_more = True
    next_cursor = None
    
    # Calculate date range for the previous week
    now = datetime.now(timezone.utc)
    one_week_ago = now - timedelta(days=7)
    
    while has_more:
        url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
        headers = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        
        # Filter for entries from the previous week
        payload = {
            "page_size": 100,
            "filter": {
                "and": [
                    {
                        "property": "Date",
                        "date": {
                            "on_or_after": one_week_ago.strftime('%Y-%m-%d')
                        }
                    },
                    {
                        "property": "Type",
                        "select": {
                            "is_not_empty": True
                        }
                    },
                    {
                        "property": "Channel",
                        "rich_text": {
                            "is_not_empty": True
                        }
                    }
                ]
            }
        }
        
        if next_cursor:
            payload["start_cursor"] = next_cursor
        
        try:
            print(f"DEBUG: Making request to Notion API for channel-type mappings...")
            response = requests.post(url, json=payload, headers=headers)
            print(f"DEBUG: Notion API response status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"DEBUG: Notion API error response: {response.text}")
                
            response.raise_for_status()
            data = response.json()
            
            print(f"DEBUG: Found {len(data.get('results', []))} pages with type mappings in this batch")
            
            # Extract channel-type mappings
            for page in data.get('results', []):
                properties = page.get('properties', {})
                
                # Get channel name
                channel_property = properties.get('Channel', {})
                channel_name = None
                if channel_property.get('type') == 'rich_text' and channel_property.get('rich_text'):
                    channel_name = channel_property['rich_text'][0].get('text', {}).get('content', '').strip()
                
                # Get type
                type_property = properties.get('Type', {})
                type_value = None
                if type_property.get('type') == 'select' and type_property.get('select'):
                    type_value = type_property['select'].get('name', '').strip()
                
                # Store mapping if both channel and type are present
                if channel_name and type_value:
                    channel_type_map[channel_name] = type_value
                    print(f"DEBUG: Found mapping: '{channel_name}' -> '{type_value}'")
            
            has_more = data.get('has_more', False)
            next_cursor = data.get('next_cursor')
            
        except Exception as e:
            print(f"Error retrieving channel-type mappings: {e}")
            print(f"DEBUG: Full error details: {type(e).__name__}: {str(e)}")
            break
    
    print(f"Found {len(channel_type_map)} channel-type mappings from previous week")
    print(f"DEBUG: Channel-type mappings: {channel_type_map}")
    return channel_type_map

def get_existing_video_ids():
    """
    Retrieves all existing video IDs from the Notion database to prevent duplicates.
    
    Returns:
        set: A set of existing video IDs from URLs in the database.
    """
    print("DEBUG: Starting to fetch existing video IDs...")
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
            print(f"DEBUG: Making request to Notion API...")
            response = requests.post(url, json=payload, headers=headers)
            print(f"DEBUG: Notion API response status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"DEBUG: Notion API error response: {response.text}")
                
            response.raise_for_status()
            data = response.json()
            
            print(f"DEBUG: Found {len(data.get('results', []))} pages in this batch")
            
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
            print(f"DEBUG: Full error details: {type(e).__name__}: {str(e)}")
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

def calculate_duration_decimal(seconds):
    """
    Converts duration in seconds to decimal format (e.g., 14.75 for 14:45).
    
    Args:
        seconds (int): Duration in seconds
    
    Returns:
        float: Duration as decimal number (minutes + seconds/60)
    """
    if seconds == 0:
        return 0.0
    
    total_minutes = seconds / 60
    return round(total_minutes, 2)

def get_last_24h_videos_with_duration(youtube_service, channel_id, existing_video_ids):
    """
    Fetches videos published in the last 24 hours for a given YouTube channel ID,
    filters by duration (>5 minutes), and excludes existing videos.
    OPTIMIZED for lower API quota usage.

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
    
    print(f"DEBUG: Searching for videos between {start_time} and {end_time}")

    try:
        # OPTIMIZATION 1: Use playlistItems API instead of search API (lower quota cost)
        # First get the channel's uploads playlist ID
        print(f"DEBUG: Getting channel info for {channel_id}")
        channel_request = youtube_service.channels().list(
            part='contentDetails',
            id=channel_id
        )
        channel_response = channel_request.execute()
        
        if not channel_response.get('items'):
            print(f"Channel {channel_id} not found")
            return []
            
        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        print(f"DEBUG: Found uploads playlist: {uploads_playlist_id}")
        
        # OPTIMIZATION 2: Get recent videos from uploads playlist (costs 1 unit vs 100 units for search)
        # Reduce maxResults to minimize quota usage
        playlist_request = youtube_service.playlistItems().list(
            part='snippet',
            playlistId=uploads_playlist_id,
            maxResults=10,  # Reduced from 50 to 10 to save quota
        )
        playlist_response = playlist_request.execute()
        
        print(f"DEBUG: Playlist returned {len(playlist_response.get('items', []))} recent videos")
        
        # Filter videos by date and exclude existing ones
        new_video_ids = []
        video_snippets = {}
        
        for item in playlist_response.get('items', []):
            video_id = item['snippet']['resourceId']['videoId']
            published_at = item['snippet']['publishedAt']
            video_title = item['snippet'].get('title', 'Unknown')
            
            # Parse the published date
            published_date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            
            # Check if video was published in the last 24 hours
            if published_date >= twenty_four_hours_ago:
                print(f"DEBUG: Found recent video: {video_title} (ID: {video_id})")
                
                if video_id not in existing_video_ids:
                    new_video_ids.append(video_id)
                    video_snippets[video_id] = item['snippet']
                    print(f"DEBUG: Video is new, added to processing list")
                else:
                    print(f"DEBUG: Video already exists in database, skipping")
            else:
                print(f"DEBUG: Video is older than 24 hours, stopping search")
                break  # Since videos are ordered by date, we can stop here
        
        if not new_video_ids:
            print(f"No new videos found for channel {channel_id}")
            return []
        
        print(f"Found {len(new_video_ids)} new videos, checking durations...")
        
        # OPTIMIZATION 3: Process videos in smaller batches to avoid unnecessary API calls
        # If no new videos, don't make the videos API call
        if not new_video_ids:
            return []
        
        # Get video details including duration (batch request for efficiency)
        videos_request = youtube_service.videos().list(
            part='contentDetails',
            id=','.join(new_video_ids)  # Batch request
        )
        videos_response = videos_request.execute()
        
        print(f"DEBUG: Got duration details for {len(videos_response.get('items', []))} videos")
        
        # Filter by duration and prepare final list
        valid_videos = []
        for video in videos_response.get('items', []):
            video_id = video['id']
            duration_str = video.get('contentDetails', {}).get('duration', '')
            duration_seconds = parse_duration(duration_str)
            
            snippet = video_snippets.get(video_id, {})
            video_title = snippet.get('title', 'Unknown')
            
            print(f"DEBUG: Video '{video_title}' duration: {duration_str} ({duration_seconds} seconds)")
            
            # Only include videos longer than 5 minutes (300 seconds)
            if duration_seconds > 300:
                print(f"DEBUG: Video is longer than 5 minutes, adding to final list")
                valid_videos.append({
                    'title': snippet.get('title', ''),
                    'video_id': video_id,
                    'channel_title': snippet.get('channelTitle', ''),
                    'published_at': snippet.get('publishedAt', ''),
                    'duration_seconds': duration_seconds,
                    'duration_decimal': calculate_duration_decimal(duration_seconds)
                })
            else:
                print(f"DEBUG: Video is 5 minutes or shorter, skipping")
        
        print(f"Found {len(valid_videos)} videos longer than 5 minutes")
        return valid_videos
        
    except HttpError as e:
        print(f"Error fetching videos for channel {channel_id}: {e}")
        print(f"DEBUG: YouTube API error details: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while fetching videos for channel {channel_id}: {e}")
        print(f"DEBUG: Full error details: {type(e).__name__}: {str(e)}")
        return []

def add_videos_to_notion_batch(videos, channel_type_mappings):
    """
    Adds multiple videos to Notion database efficiently with automatic type assignment.
    
    Args:
        videos (list): List of video dictionaries to add.
        channel_type_mappings (dict): Dictionary mapping channel names to types.
    
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
        print(f"DEBUG: Preparing to add video: {video['title']}")
        
        # Determine type based on channel mapping
        channel_name = video['channel_title']
        video_type = channel_type_mappings.get(channel_name)
        
        if video_type:
            print(f"DEBUG: Found type mapping for '{channel_name}' -> '{video_type}'")
        else:
            print(f"DEBUG: No type mapping found for '{channel_name}', leaving Type empty")
        
        # Prepare properties
        properties = {
            "Title": {"title": [{"text": {"content": video['title']}}]},
            "URL": {"url": f"https://www.youtube.com/watch?v={video['video_id']}"},
            "Channel": {"rich_text": [{"text": {"content": video['channel_title']}}]},
            "Date": {"date": {"start": video['published_at']}},
            "Length": {"number": video['duration_decimal']}
        }
        
        # Add Type property only if we have a mapping for this channel
        if video_type:
            properties["Type"] = {"select": {"name": video_type}}
        
        data = {
            "parent": {"database_id": NOTION_DB_ID},
            "properties": properties
        }
        
        print(f"DEBUG: Notion payload: {json.dumps(data, indent=2)}")
        
        try:
            response = session.post(url, json=data)
            print(f"DEBUG: Notion response status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"DEBUG: Notion error response: {response.text}")
                
            response.raise_for_status()
            success_count += 1
            type_info = f" [Type: {video_type}]" if video_type else " [Type: Empty]"
            print(f"✅ Added: {video['title'][:50]}... ({video['duration_decimal']} min){type_info}" if len(video['title']) > 50 else f"✅ Added: {video['title']} ({video['duration_decimal']} min){type_info}")
        except requests.exceptions.RequestException as e:
            failed_count += 1
            print(f"❌ Failed: {video['title'][:50]}..." if len(video['title']) > 50 else f"❌ Failed: {video['title']}")
            print(f"   Error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"   Response text: {e.response.text}")
    
    return success_count, failed_count

def main():
    """
    Main function to orchestrate fetching videos and adding them to Notion.
    OPTIMIZED for lower API quota usage with automatic type assignment.
    """
    print("DEBUG: Starting main function...")
    
    # Debug environment variables
    print(f"DEBUG: YOUTUBE_API_KEY present: {bool(YOUTUBE_API_KEY)}")
    print(f"DEBUG: NOTION_TOKEN present: {bool(NOTION_TOKEN)}")
    print(f"DEBUG: NOTION_DB_ID: {NOTION_DB_ID}")
    
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
    try:
        print("DEBUG: Initializing YouTube API service...")
        youtube_service = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
        print("DEBUG: YouTube API service initialized successfully")
    except Exception as e:
        print(f"ERROR: Failed to initialize YouTube API: {e}")
        return

    # Check if any channel IDs were loaded
    if not CHANNEL_IDS:
        print("No channel IDs loaded. Please ensure 'channels.txt' exists and contains channel IDs.")
        return

    # Get channel-type mappings from previous week
    print("Retrieving channel-type mappings from previous week...")
    channel_type_mappings = get_channel_type_mappings()

    # Get existing video IDs only once and reuse
    print("Retrieving existing videos from Notion database...")
    existing_video_ids = get_existing_video_ids()

    total_success = 0
    total_failed = 0
    api_calls_made = 0

    # Process each channel with rate limiting
    for i, channel_id in enumerate(CHANNEL_IDS):
        print(f"\n--- Processing channel {i+1}/{len(CHANNEL_IDS)}: {channel_id} ---")
        
        # Add small delay between channels to avoid rate limits
        if i > 0:
            print("DEBUG: Adding delay between channels to respect rate limits...")
            time.sleep(1)  # 1 second delay between channels
        
        # Get new videos (>5 minutes, not in database)
        videos = get_last_24h_videos_with_duration(youtube_service, channel_id, existing_video_ids)
        api_calls_made += 2  # channels().list + playlistItems().list
        
        if videos:
            api_calls_made += 1  # videos().list call
            print(f"Adding {len(videos)} new videos to Notion...")
            success, failed = add_videos_to_notion_batch(videos, channel_type_mappings)
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
    print(f"📋 Channel-type mappings used: {len(channel_type_mappings)}")
    print(f"🔧 API calls made: ~{api_calls_made} (estimated)")
    print(f"💰 Quota used: ~{api_calls_made * 1} units (playlist method) vs ~{len(CHANNEL_IDS) * 100} units (search method)")
    print(f"📊 Quota saved: ~{(len(CHANNEL_IDS) * 100) - api_calls_made} units ({((len(CHANNEL_IDS) * 100 - api_calls_made) / (len(CHANNEL_IDS) * 100)) * 100:.1f}% reduction)")

if __name__ == "__main__":
    main()