#!/usr/bin/env python
# coding: utf-8
# reference: https://github.com/mitchelljy/Trending-YouTube-Scraper

import sys
import time
import requests
from google.cloud import storage
from pyspark.sql import SparkSession

snippet_features = ["title",
                    "publishedAt",
                    "channelId",
                    "channelTitle",
                    "categoryId"]
header = ["video_id"] + snippet_features + ["trending_date", "tags", "view_count", "likes", "dislikes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled"
                                            ]
unsafe_characters = ['\n', '"']
BUCKET = ''  # enter your bucket name
spark = SparkSession.builder.appName("CommentsScraper").getOrCreate()
api_key = ''  # enter your api key


def prepare_feature(feature):
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    op = f'"{feature}"'
    return op


def api_request(page_token, country_code):
    global api_key
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
    request = requests.get(request_url)
    if request.status_code == 403:
        print("excess requests")
        sys.exit()
    return request.json()


def get_tags(tags_list):
    # Takes a list of tags, prepares each tag and joins them into a string by the pipe character
    return prepare_feature("|".join(tags_list))


def get_videos(items):
    lines = []
    for video in items:
        comments_disabled = False
        ratings_disabled = False

        if "statistics" not in video:
            continue

        video_id = prepare_feature(video['id'])

        snippet = video['snippet']
        statistics = video['statistics']

        features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]

        thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
        trending_date = time.strftime("%y.%d.%m")
        tags = get_tags(snippet.get("tags", ["[none]"]))
        view_count = statistics.get("viewCount", 0)

        # This may be unclear, essentially the way the API works is that if a video has comments or ratings disabled
        # then it has no feature for it, thus if they don't exist in the statistics dict we know they are disabled
        if 'likeCount' in statistics and 'dislikeCount' in statistics:
            likes = statistics['likeCount']
            dislikes = statistics['dislikeCount']
        else:
            ratings_disabled = True
            likes = 0
            dislikes = 0

        if 'commentCount' in statistics:
            comment_count = statistics['commentCount']
        else:
            comments_disabled = True
            comment_count = 0

        line = [video_id] + features + [prepare_feature(x) for x in [trending_date, tags, view_count, likes, dislikes,
                                                                     comment_count, thumbnail_link, comments_disabled,
                                                                     ratings_disabled
                                                                     ]]
        lines.append(",".join(line))
    return lines


def get_pages(country_code, cursor="&"):
    country_data = []

    while cursor is not None:
        video_data_page = api_request(cursor, country_code)

        cursor = video_data_page.get("nextPageToken", None)
        if cursor is not None:
            cursor = f"&pageToken={cursor}&"

        items = video_data_page.get('items', [])
        country_data += get_videos(items)

    return country_data


def write_to_file(country_code, country_data):
    print(f"Writing {country_code} data to file...")

    client = storage.Client()
    bucket = client.get_bucket(BUCKET)
    target_blob = bucket.blob(f"videos/{time.strftime('%y_%d_%m')}_{country_code}_videos.csv")

    local_tmp_path = f"{country_code}_videos.csv"

    with open(local_tmp_path, "w+") as file:
        for row in country_data:
            file.write(f"{row}\n")

    target_blob.upload_from_filename(local_tmp_path)
    return


def scraper(country_code):
    country_data = [",".join(header)] + get_pages(country_code)
    write_to_file(country_code, country_data)
    return len(country_data)


scraper('US')  # only scraping the videos for US region
