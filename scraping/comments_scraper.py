#!/usr/bin/env python
# coding: utf-8

import datetime
import sys
import time
import requests
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

unsafe_characters = ['\n', '"']
header = ["video_id", "comment_id", "textOriginal", "publishedAt", "likeCount", "totalReplyCount"]
BUCKET = ''  # enter your bucket name
output_dir = 'output/'
spark = SparkSession.builder.appName("CommentsScraper").getOrCreate()
api_keys = ['', '']  # enter set of api keys
idx = 0
api_key = api_keys[idx]

path = f''  # enter path of videos.csv in bucket

us_videos_df = spark.read.option("header", "true").csv(path)
video_ids = us_videos_df.rdd.map(lambda x: x.video_id).collect()
video_ids_list = [[video_ids[i]] for i in range(len(video_ids))]


def prepare_feature(feature):
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    op = f'"{feature}"'
    return op


def api_request(page_token, video_id):
    global api_key, idx, api_keys
    request_url = f"https://www.googleapis.com/youtube/v3/commentThreads?part=snippet{page_token}videoId={video_id}&maxResults=50&key={api_key}"
    request = requests.get(request_url)
    if request.status_code == 403:
        print("excess requests")
        idx += 1
        if idx < len(api_keys):
            api_key = api_keys[idx]
            print(api_key)
            return api_request(page_token, video_id)
        else:
            print("excess requests again")
            sys.exit()
    else:
        return request.json()


def get_comments(items, video_id):
    lines = []
    for comment in items:
        if "snippet" not in comment:
            continue

        comment_id = prepare_feature(comment['id'])
        textOriginal = ''
        likeCount = 0
        publishedAt = ''
        snippet = comment['snippet']
        totalReplyCount = prepare_feature(snippet['totalReplyCount'])
        if 'topLevelComment' in snippet and 'snippet' in snippet['topLevelComment']:
            snippet_2 = snippet['topLevelComment']['snippet']
            textOriginal = prepare_feature(snippet_2['textOriginal'])
            date_time_obj = datetime.datetime.strptime(snippet_2['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            timeobj = date_time_obj.time()
            publishedAt = str(timeobj.hour) + ':' + str(timeobj.minute)
            likeCount = prepare_feature(snippet_2['likeCount'])

        line = [video_id, comment_id, textOriginal, publishedAt, likeCount, totalReplyCount]
        lines.append(",".join(line))
    return lines


def get_pages(video_id, cursor="&"):
    comments_data = []

    while cursor is not None:
        video_data_page = api_request(cursor, video_id)

        cursor = video_data_page.get("nextPageToken", None)
        if cursor is not None:
            cursor = f"&pageToken={cursor}&"

        items = video_data_page.get('items', [])
        comments_data += get_comments(items, video_id)

    return comments_data


def write_to_file(video_id, country_data):
    print(f"Writing {video_id} data to file...")

    client = storage.Client()
    bucket = client.get_bucket(BUCKET)
    target_blob = bucket.blob(f"comments/{time.strftime('%y_%d_%m')}/{video_id}_comments.csv")

    local_tmp_path = f"{video_id}_comments.csv"

    with open(local_tmp_path, "w+") as file:
        for row in country_data:
            file.write(f"{row}\n")

    target_blob.upload_from_filename(local_tmp_path)
    return


def scraper(video_id):
    country_data = [",".join(header)] + get_pages(video_id)
    write_to_file(video_id, country_data)
    return len(country_data)


if __name__ == "__main__":
    udf_scraper = udf(scraper, IntegerType())
    df = spark.createDataFrame(video_ids_list, ['video_id']).withColumn("execute", udf_scraper(col("video_id")))
    df.rdd.count()  # evaluate the dataframe to execute all the api calls
