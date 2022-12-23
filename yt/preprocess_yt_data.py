#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession

from textblob import TextBlob
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
from pyspark.sql.functions import when
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when, count
import time


# create spark session and load necessary data
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
sqlContext = SQLContext(spark)
bucket = '' #Enter your bucket name
comments_path = f"gs://{bucket}/comments/{time.strftime('%y_%d_%m')}/"
table_name = f"bda.preprocessed_yt_data_{time.strftime('%y_%d_%m')}"
preprocessed_path = f"gs://{bucket}/yt_data/preprocessed_yt_data_{time.strftime('%y_%d_%m')}.csv"


comments_df = spark.read.format("csv").option(
    "header", "true").load(comments_path)
comments_df.show(3)
comments_df.count()

# data preprocessing steps
comments_df.printSchema()
comments_df.select([count(when(col(c).isNull(), c)).alias(c)
                   for c in comments_df.columns]).show()
comments_df = comments_df.na.drop()
comments_df.select(*(count(when(col(c).isNull(), c)).alias(c)
                   for c in comments_df.columns)).show()
comments_df = comments_df.withColumnRenamed("textOriginal","comment_text")

# perform data labeling using textblob library
sentiment = []
for row in comments_df.collect():
    sentiment.append(TextBlob(row["comment_text"]).sentiment.polarity)

sentiment_df = sqlContext.createDataFrame(
    [(l,) for l in sentiment], ['sentiment'])

# Concatenate the data and labels generated from textblob and store the final data to bucket
comments_df = comments_df.withColumn("row_idx", row_number().over(
    Window.orderBy(monotonically_increasing_id())))
sentiment_df = sentiment_df.withColumn("row_idx", row_number().over(
    Window.orderBy(monotonically_increasing_id())))

final_df = comments_df.join(
    sentiment_df, comments_df.row_idx == sentiment_df.row_idx).drop("row_idx")
final_df.show()

df = final_df.withColumn("sentiment_value", when(final_df.sentiment == "0.0", "0").when(
    final_df.sentiment >= 0, "1").when(final_df.sentiment <= 0, "-1"))
df.write.mode('overwrite').option("header",True).csv(preprocessed_path)
