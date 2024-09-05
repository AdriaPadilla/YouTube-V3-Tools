# Import necessary libraries
import os
import pandas as pd
from tqdm import tqdm  # For showing progress bars in loops
import time
import json
import glob
from datetime import date, timedelta
from isodate import parse_duration  # For parsing ISO 8601 duration strings
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set up your YouTube API Key and the channel ID
API_KEY = 'YOUR_API_KEY'

# QUERY DETAILS AS LIST OF TUPLES
# FOR EACH TUPLE, indicate ("channel_id", "Query_name") 
# Next QUERYS is an example. You can indicate a list of tuples.
QUERYS = [("UC4eYXhJI4-7wSWc8UNRwD4A", "NPR-MUSIC")]

# Initialize the YouTube API client
youtube = build('youtube', 'v3', developerKey=API_KEY)

def channel_info(channel_id, alias):
    # Fetch channel information (snippet, statistics, content details) using the channel ID
    request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=channel_id
        ).execute()

    # Save the channel info to a JSON file
    output_filename = f"outputs/{alias}/{alias}-info.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(request, f, ensure_ascii=False, indent=4)

def get_playlist_items(channel_id, alias):
    # Load previously saved channel info to get the uploads playlist ID
    with open(f"outputs/{alias}/{alias}-info.json") as f:
        data = json.load(f)
        uploads_playlists = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        video_items = []
        next_page_token = None
        
        # Fetch all videos from the uploads playlist
        while True:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlists,
                maxResults=50,
                pageToken=next_page_token,
            ).execute()
            print(f"Requests OK | Total videos extracted {len(video_items)}")

            video_items += request.get("items", [])
            next_page_token = request.get('nextPageToken')
            if not next_page_token:
                break

        # Save the list of video items to a JSON file
        output_filename = f"outputs/{alias}/{alias}-playlistItems.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(video_items, f, ensure_ascii=False, indent=4)

def video_data(alias):
    # Load the playlist items (videos) from the JSON file
    with open(f"outputs/{alias}/{alias}-playlistItems.json") as f:
        videos = json.load(f)

        # Create a directory for individual video data if it doesn't exist
        if not os.path.exists(f"outputs/{alias}/individual_video_data/"):
            os.makedirs(f"outputs/{alias}/individual_video_data/")

        # Iterate over each video in the playlist
        for video in tqdm(videos):
            video_id = video["contentDetails"]["videoId"]

            # Check if video data already exists to avoid duplicates
            if os.path.exists(f"outputs/{alias}/individual_video_data/{video_id}.json"):
                print(f"video with ID {video_id} already exists in outputs/{alias}/individual_video_data/ directory")
                pass
            else:
                individual_video_data = {}
                try:
                    # Fetch detailed data for each video (snippet, statistics, content details)
                    yt_api_video = youtube.videos().list(part="snippet,statistics,contentDetails", id=video_id).execute()
                    individual_video_data["VIDEO_BASIC_DATA"] = video
                    individual_video_data["VIDEO_INFO"] = yt_api_video
                    output_filename = f"outputs/{alias}/individual_video_data/{video_id}.json"
                    
                    # Save individual video data to a JSON file
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(individual_video_data, f, ensure_ascii=False, indent=4)

                except HttpError as err:
                    # Handle HTTP errors, retry on specific errors with a delay
                    if err.resp.status in [403, 500, 503]:
                        print(f"==> ¡ALERT! API ERROR {HttpError}")
                        print(f"==> ¡ALERT! SLEEPING FOR 5 SECONDS AND RETRY")
                        time.sleep(5)
                        query(item)  # Retry fetching the video data
                    else:
                        print(f"============> ¡ALERT! RETRY FAILED")
                        raise  # Raise the error if retry fails

def parser():
    # Helper function to convert ISO 8601 duration to seconds
    def iso8601_to_seconds(duration):
        duration_obj = parse_duration(duration)
        total_seconds = duration_obj.total_seconds()
        return total_seconds

    # Helper function to convert ISO 8601 date string to datetime object
    def iso8601_to_datetime(date_str):
        date_format = '%Y-%m-%dT%H:%M:%SZ'
        datetime_obj = datetime.strptime(date_str, date_format)
        return datetime_obj

    # Retrieve all JSON files containing individual video data
    video_files = glob.glob(f"outputs/{alias}/individual_video_data/*.json")

    general_list = []
    # Process each video file to extract relevant data
    for v in tqdm(video_files):
        with open(v) as f:
            pj = json.load(f)
            data = {}
            data["video_id"] = pj["VIDEO_BASIC_DATA"]["contentDetails"]["videoId"]
            data["video_link"] = f"https://www.youtube.com/watch?v={data['video_id']}"
            data["video_published_at"] = iso8601_to_datetime(pj["VIDEO_BASIC_DATA"]["contentDetails"]["videoPublishedAt"])
            data["channel_id"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["channelId"]
            data["channel_title"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["channelTitle"]
            data["video_title"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["title"]
            data["video_description"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["description"]
            data["video_duration_sec"] = int(round(iso8601_to_seconds(pj["VIDEO_INFO"]["items"][0]["contentDetails"]["duration"]), 0))

            # Attempt to fetch video likes, handle cases where data is missing
            try:
                data["video_likes"] = pj["VIDEO_INFO"]["items"][0]["statistics"]["likeCount"]
            except:
                data["video_likes"] = 0

            data["video_views"] = pj["VIDEO_INFO"]["items"][0]["statistics"]["viewCount"]
            data["video_comments"] = pj["VIDEO_INFO"]["items"][0]["statistics"]["commentCount"]
            data["video_category_id"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["categoryId"]

            # Attempt to fetch video tags, handle cases where data is missing
            try:
                data["video_tags"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["title"]
            except:
                data["video_tags"] = "no tags"

            # Attempt to fetch video default language, handle cases where data is missing
            try:
                data["video_default_language"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["defaultAudioLanguage"]
            except:
                data["video_default_language"] = "no data"

            # Append processed data to the general list
            general_list.append(data)

    # Convert the list of data into a pandas DataFrame
    df = pd.DataFrame.from_records(general_list)

    # CLEAN DATA TO AVOID PROBLEMS
    df.replace({'\t': ' '}, regex=True)
    df.replace({'\r': ' '}, regex=True)
    df.replace({'\n': ' '}, regex=True)
    df.replace({'"': "'"}, regex=True)
    df = df[df['video_published_at'].notna()]
    df = df[df['video_duration_sec'].notna()]

    # Export the cleaned data to an Excel file
    print(f"Exporting data to XLX file (outputs/{alias}/{alias}-dataset.xlsx)")
    df.to_excel(f"outputs/{alias}/{alias}-dataset.xlsx", index=False)

if __name__ == "__main__":
    # Iterate over each query (channel ID and alias pair)
    for query in QUERYS:
        channel_id = query[0]
        alias = query[1]

        # Create directory for the channel alias if it doesn't exist
        if not os.path.exists(f"outputs/{alias}"):
            os.makedirs(f"outputs/{alias}")

        # MAIN FUNCTIONGS 
        channel_info(channel_id, alias)
        get_playlist_items(channel_id, alias)
        video_data(alias)
        
        # ENDING Process: Parse the video data and export to Excel
        parser()
