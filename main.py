import os
import json
from googleapiclient.discovery import build_from_document
from datetime import datetime, timedelta, timezone
import requests
from dotenv import load_dotenv

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")

def load_channel_ids():
    with open('channels.txt', 'r') as file:
        return [line.strip() for line in file if line.strip()]

CHANNEL_IDS = load_channel_ids()

# Load the discovery document
with open("youtube-v3-discovery.json", "r") as f:
    doc = json.load(f)

# Build the YouTube service using API key and static discovery
youtube = build_from_document(doc, developerKey=YOUTUBE_API_KEY)

def get_yesterday_videos(channel_id):
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

    request = youtube.search().list(
        part='snippet',
        channelId=channel_id,
        publishedAfter=yesterday_start.isoformat(),
        publishedBefore=yesterday_end.isoformat(),
        maxResults=10,
        order='date',
        type='video'
    )
    response = request.execute()
    return [ {
        'title': item['snippet']['title'],
        'video_id': item['id']['videoId'],
        'channel_title': item['snippet']['channelTitle'],
        'published_at': item['snippet']['publishedAt']
    } for item in response.get('items', [])]

def add_to_notion(video):
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    data = {
        "parent": { "database_id": NOTION_DB_ID },
        "properties": {
            "Title": { "title": [{ "text": { "content": video['title'] } }] },
            "Link": { "url": f"https://www.youtube.com/watch?v={video['video_id']}" },
            "Channel": { "rich_text": [{ "text": { "content": video['channel_title'] } }] },
            "Date": { "date": { "start": video['published_at'] } }
        }
    }
    response = requests.post(url, json=data, headers=headers)
    return response.status_code == 200

def main():
    for channel_id in CHANNEL_IDS:
        videos = get_yesterday_videos(channel_id)
        for video in videos:
            success = add_to_notion(video)
            print(f"{'✅ Added' if success else '❌ Failed'}: {video['title']}")

if __name__ == "__main__":
    main()
