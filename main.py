import os
import json
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError # Import for handling HTTP errors from Google APIs

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

def get_last_48h_videos(youtube_service, channel_id):
    """
    Fetches videos published in the last 48 hours for a given YouTube channel ID.

    Args:
        youtube_service: An initialized YouTube API client object.
        channel_id (str): The ID of the YouTube channel.

    Returns:
        list: A list of dictionaries, each containing video details (title, video_id, channel_title, published_at).
              Returns an empty list if an error occurs or no videos are found.
    """
    now = datetime.now(timezone.utc)
    forty_eight_hours_ago = now - timedelta(hours=48)

    # Calculate start time for the last 48 hours in UTC
    # Format timestamps for YouTube API compatibility (RFC 3339 format with Z suffix)
    start_time = forty_eight_hours_ago.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = now.strftime('%Y-%m-%dT%H:%M:%SZ')

    try:
        # Make the YouTube Data API search request
        request = youtube_service.search().list(
            part='snippet',          # Request snippet details for each video
            channelId=channel_id,    # Filter by channel ID
            publishedAfter=start_time, # Videos published after this time (48 hours ago)
            publishedBefore=end_time,  # Videos published before this time (now)
            maxResults=50,           # Increased to capture more videos in 48h period
            order='date',            # Order results by date
            type='video'             # Only return video results (not channels or playlists)
        )
        response = request.execute() # Execute the API request

        # Extract relevant video information from the API response
        return [{
            'title': item['snippet']['title'],
            'video_id': item['id']['videoId'],
            'channel_title': item['snippet']['channelTitle'],
            'published_at': item['snippet']['publishedAt']
        } for item in response.get('items', []) if 'videoId' in item['id']] # Ensure videoId exists
    except HttpError as e:
        # Handle errors specific to Google API calls (e.g., invalid API key, rate limits)
        print(f"Error fetching videos for channel {channel_id}: {e}")
        return []
    except Exception as e:
        # Handle any other unexpected errors
        print(f"An unexpected error occurred while fetching videos for channel {channel_id}: {e}")
        return []

def add_to_notion(video):
    """
    Adds a video entry to a Notion database.

    Args:
        video (dict): A dictionary containing video details (title, video_id, channel_title, published_at).

    Returns:
        bool: True if the video was successfully added to Notion, False otherwise.
    """
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28" # Specify the Notion API version
    }

    # Construct the payload for the Notion API request
    data = {
        "parent": { "database_id": NOTION_DB_ID },
        "properties": {
            "Title": { "title": [{ "text": { "content": video['title'] } }] },
            # CORRECTED: Use the standard YouTube video URL format
            "Link": { "url": f"https://www.youtube.com/watch?v={video['video_id']}" },
            "Channel": { "rich_text": [{ "text": { "content": video['channel_title'] } }] },
            "Date": { "date": { "start": video['published_at'] } }
        }
    }

    try:
        # Send the POST request to the Notion API
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        return True
    except requests.exceptions.RequestException as e:
        # Handle errors related to the HTTP request (e.g., network issues, invalid URL)
        print(f"Error adding video '{video['title']}' to Notion: {e}")
        if 'response' in locals(): # Check if response object exists
            print(f"Notion API response status code: {response.status_code}")
            print(f"Notion API response body: {response.text}")
        return False
    except Exception as e:
        # Handle any other unexpected errors
        print(f"An unexpected error occurred while adding video '{video['title']}' to Notion: {e}")
        return False

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
    # The 'build' function handles downloading the discovery document internally if needed.
    youtube_service = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    # Check if any channel IDs were loaded
    if not CHANNEL_IDS:
        print("No channel IDs loaded. Please ensure 'channels.txt' exists and contains channel IDs.")
        return

    # Iterate through each channel ID
    for channel_id in CHANNEL_IDS:
        print(f"\n--- Processing channel: {channel_id} ---")
        # Get videos published in the last 48 hours for the current channel
        videos = get_last_48h_videos(youtube_service, channel_id)

        if videos:
            print(f"Found {len(videos)} videos for channel {channel_id} published in the last 48 hours. Adding to Notion...")
            # Add each found video to Notion
            for video in videos:
                success = add_to_notion(video)
                print(f"{'✅ Added' if success else '❌ Failed'}: {video['title']}")
        else:
            print(f"No videos found for channel {channel_id} in the last 48 hours or an error occurred.")

if __name__ == "__main__":
    main()