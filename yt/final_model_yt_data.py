#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql import Window
from pyspark.sql.functions import when
from pyspark.ml import PipelineModel
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when, count

# create spark session and load necessary data
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
sqlContext = SQLContext(spark)
bucket = '' #Enter your bucket name
videos_path = f"gs://{bucket}/videos/{time.strftime('%y_%d_%m')}_US_videos.csv"
accuracy_table_name = 'bda.daily_accuracy_yt'
final_table_name = f"bda.final_yt_data_{time.strftime('%y_%d_%m')}"

# load the videos data into dataframe 
videos_df = spark.read.format("csv").option("header", "true").load(videos_path)
videos_df.select([count(when(col(c).isNull(), c)).alias(c)
                 for c in videos_df.columns]).show()

spark.conf.set('temporaryGcsBucket', bucket)

# read accuracy of all models and choose the best performing model for that day
accuracy_df = spark.read.format('bigquery') \
    .option('table', accuracy_table_name) \
    .load()
accuracy_df.createOrReplaceTempView('accuracy_table')
best_accuracy_model_df = spark.sql(
    f"with cte as (SELECT model_name, row_number() over \
     (partition by date order by accuracy desc) as r \
      from accuracy_table where date = '{time.strftime('%y_%d_%m')}') \
       SELECT model_name from cte where r = 1")
best_accuracy_model_df.show()
best_accuracy_model_df.printSchema()
best_model = best_accuracy_model_df.select("model_name").collect()[0][0]

# load the best model and perform data classification for comments by performing sentiment analysis
model_pipeline_path = f"gs://{bucket}/models/static/{best_model}_model_pipeline"

preprocessed_path = f"gs://{bucket}/yt_data/preprocessed_yt_data_{time.strftime('%y_%d_%m')}.csv"
preprocessed_df = spark.read.format("csv").option("header", "true").load(preprocessed_path)

comments_comment_text_df = preprocessed_df.select(preprocessed_df.comment_text)
persistedModel = PipelineModel.load(model_pipeline_path)
sentiment_df = persistedModel.transform(comments_comment_text_df)

# Combine the sentiment labels with the other features of data needed for data visualization
comments_df = preprocessed_df.withColumn("row_idx", row_number().over(
    Window.orderBy(monotonically_increasing_id())))
sentiment_df = sentiment_df.withColumn("row_idx", row_number().over(
    Window.orderBy(monotonically_increasing_id())))
sentiment_df.printSchema()
comments_sentiment_df = comments_df.join(
    sentiment_df, comments_df.row_idx == sentiment_df.row_idx).drop("row_idx")
comments_sentiment_df.printSchema()

# data preprocessing on the prediction column to convert it into categorical column
comments_sentiment_df = comments_sentiment_df.withColumn("prediction", when(comments_sentiment_df.prediction == 0.0, 0).when(
    comments_sentiment_df.prediction == 1.0, 1).when(comments_sentiment_df.prediction == 2.0, -1))

# perform data aggregation to find total number of positive, neutral and negative comments
comments_sentiment_aggregated_df = comments_sentiment_df.groupBy(
    'video_id').pivot('prediction').count()
comments_sentiment_aggregated_df = comments_sentiment_aggregated_df.withColumnRenamed(
    '-1', 'negative_count').withColumnRenamed('1', 'positive_count').withColumnRenamed('0', 'neutral_count')

comments_sentiment_aggregated_df.printSchema()
comments_sentiment_aggregated_df.show(3)
comments_sentiment_aggregated_df.count()

# generate a final combined dataframe that contains general information about the video as well as aggregated information in the previous step
combined_df = comments_sentiment_aggregated_df.join(
    videos_df, comments_sentiment_aggregated_df.video_id == videos_df.video_id).drop(videos_df.video_id)
combined_df.show(3)
combined_df.count()

spark.conf.set('temporaryGcsBucket', bucket)

# store this aggregated information into bigquery for later use
combined_df.createOrReplaceTempView("yt_data")
test_op = spark.sql('SELECT * FROM yt_data')
test_op.show()
test_op.write.format('bigquery').option(
    'table', final_table_name).mode("overwrite").save()
