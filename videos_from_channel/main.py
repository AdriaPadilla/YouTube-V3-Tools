import os
import pandas as pd
from tqdm import tqdm
import time
import json
import glob
from datetime import date, timedelta
from isodate import parse_duration
from datetime import datetime
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# INPUT YOUR YOUTUBE V3 API KEY IN HERE
API_KEY = 'YOUR_API_KEY'

## You can extract one single channel, or a list of them in the same process
## FOR EACH TUPLE, indicate ("channel_id", "Qeery_name")

QUERYS = [
            ("@luisitocomunica", "tfg"),
            ]

youtube = build('youtube', 'v3', developerKey=API_KEY)

def channel_info(channel_id, alias):
    output_filename = f"outputs/{alias}/{channel_id}/channelinfo-{channel_id}.json"
    if not os.path.exists(output_filename):
        request = youtube.channels().list(
                part="snippet,statistics,contentDetails",
                forHandle=channel_id,
            ).execute()


        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(request, f, ensure_ascii=False, indent=4)
    else:
        print(f"{output_filename} exists > PASS")
def get_playlist_items(channel_id, alias):

    output_filename = f"outputs/{alias}/{channel_id}/playlistItems-{channel_id}.json"
    if not os.path.exists(output_filename):
        with open(f"outputs/{alias}/{channel_id}/channelinfo-{channel_id}.json") as f:
            data = json.load(f)
            uploads_playlists = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

            video_items = []
            next_page_token = None
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


            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(video_items, f, ensure_ascii=False, indent=4)
    else:
        print(f"{output_filename} exists > PASS")
def video_data(alias, channel_id):

    with open(f"outputs/{alias}/{channel_id}/playlistItems-{channel_id}.json") as f:
        videos = json.load(f)

        if not os.path.exists(f"outputs/{alias}/{channel_id}/individual_video_data/"):
            os.makedirs(f"outputs/{alias}/{channel_id}/individual_video_data/")


        for video in tqdm(videos):

            video_id = video["contentDetails"]["videoId"]

            if os.path.exists(f"outputs/{alias}/{channel_id}/individual_video_data/{video_id}.json"):
                pass

            else:
                individual_video_data = {}
                try:
                    yt_api_video = youtube.videos().list(part="snippet,statistics,contentDetails", id=video_id).execute()
                    individual_video_data["VIDEO_BASIC_DATA"] = video
                    individual_video_data["VIDEO_INFO"] = yt_api_video
                    output_filename = f"outputs/{alias}/{channel_id}/individual_video_data/{video_id}.json"
                    with open(output_filename, 'w', encoding='utf-8') as f:
                        json.dump(individual_video_data, f, ensure_ascii=False, indent=4)

                except HttpError as err:
                    if err.resp.status in [403, 500, 503]:
                        print(f"==> ¡ALERT! API ERROR {HttpError}")
                        print(f"==> ¡ALERT! SLEEPING FOR 5 SECONDS AND RETRY")
                        time.sleep(5)
                        query(item)
                    else:
                        print(f"============> ¡ALERT! RETRY FAILED")
                        raise

def parser():

    def iso8601_to_seconds(duration):
        duration_obj = parse_duration(duration)
        total_seconds = duration_obj.total_seconds()
        return total_seconds

    def iso8601_to_datetime(date_str):
        date_format = '%Y-%m-%dT%H:%M:%SZ'
        datetime_obj = datetime.strptime(date_str, date_format)
        return datetime_obj

    video_files = glob.glob(f"outputs/{alias}/{channel_id}/individual_video_data/*.json")

    general_list = []
    for v in tqdm(video_files):
        with open(v) as f:
            pj = json.load(f)
            data = {}
            data["video_id"] = pj["VIDEO_BASIC_DATA"]["contentDetails"]["videoId"]
            data["video_link"] = f"https://www.youtube.com/watch?v={data["video_id"]}"
            data["video_published_at"] = iso8601_to_datetime(pj["VIDEO_BASIC_DATA"]["contentDetails"]["videoPublishedAt"])
            data["channel_id"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["channelId"]
            data["channel_title"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["channelTitle"]
            data["video_title"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["title"]
            data["video_description"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["description"]
            data["category_id"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["categoryId"]
            data["video_duration_sec"] = int(round(iso8601_to_seconds(pj["VIDEO_INFO"]["items"][0]["contentDetails"]["duration"]), 0))

            try:
                data["video_likes"] = pj["VIDEO_INFO"]["items"][0]["statistics"]["likeCount"]
            except:
                data["video_likes"] = 0
            try:
                data["video_views"] = pj["VIDEO_INFO"]["items"][0]["statistics"]["viewCount"]
            except:
                data["video_views"] = 0
            try:
                data["video_comments"] = pj["VIDEO_INFO"]["items"][0]["statistics"]["commentCount"]
            except KeyError:
                data["video_comments"] = 0
            data["category_id"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["categoryId"]
            try:
                data["video_tags"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["title"]
            except:
                data["video_tags"] = "no tags"

            try:
                data["video_default_language"] = pj["VIDEO_INFO"]["items"][0]["snippet"]["defaultAudioLanguage"]
            except:
                data["video_default_language"] = "no data"

            general_list.append(data)

    df = pd.DataFrame.from_records(general_list)

    # CLEAN DATA TO AVOID PROBLEMS
    df.replace({'\t': ' '}, regex=True)
    df.replace({'\r': ' '}, regex=True)
    df.replace({'\n': ' '}, regex=True)
    df.replace({'"': "'"}, regex=True)
    df = df[df['video_published_at'].notna()]
    df = df[df['video_duration_sec'].notna()]
    df['category_id'] = df['category_id'].astype(str)
    df['video_likes'] = df['video_likes'].astype(int)
    df['video_comments'] = df['video_comments'].astype(int)
    df['video_views'] = df['video_views'].astype(int)
    df['video_duration_sec'] = df['video_duration_sec'].astype(int)

    print(f"Exporting data to XLX file (outputs/{alias}/{channel_id}/{channel_id}-dataset.xlsx)")

    ## Merge cats

    cat_df = pd.read_csv("category.csv", sep=",")
    cat_df['category_id'] = cat_df['category_id'].astype(str)
    df_merged = pd.merge(df, cat_df, on="category_id", how="left")
    df_merged.to_excel(f"outputs/{alias}/{channel_id}/{channel_id}-dataset.xlsx", index=False)

if __name__ == "__main__":
    for query in QUERYS:
        channel_id = query[0]
        alias = query[1]
        print(f"working on {channel_id}")
        if not os.path.exists(f"outputs/{alias}/{channel_id}"):
            os.makedirs(f"outputs/{alias}/{channel_id}")

        channel_info(channel_id, alias)
        get_playlist_items(channel_id, alias)
        video_data(alias,channel_id)
        parser()
