

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SQLContext
import time
from pyspark.sql.types import StructType,StructField, StringType, FloatType

#create spark session and load necessary files
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
sqlContext = SQLContext(spark)
bucket = '' #Enter your bucket name
model_pipeline_path = 'gs://{}/models/static/lr_model_pipeline'.format(bucket)
table_name = f"bda.preprocessed_yt_data_{time.strftime('%y_%d_%m')}"
op_table_name = 'bda.daily_accuracy_yt'

# load the preprocessed data stored in bucket
preprocessed_path = f"gs://{bucket}/yt_data/preprocessed_yt_data_{time.strftime('%y_%d_%m')}.csv"
preprocessed_df = spark.read.format("csv").option("header", "true").load(preprocessed_path)
spark.conf.set('temporaryGcsBucket', bucket)

preprocessed_df = preprocessed_df.select(preprocessed_df.comment_text, preprocessed_df.sentiment_value)

# Divide the dataset into train and test set for evaluation
(train_set, val_set, test_set) = preprocessed_df.randomSplit([0.6, 0.2, 0.2], seed = 2000)

# load pipeline for performing sentiment predictions on realtime data
persistedModel = PipelineModel.load(model_pipeline_path)

predictions_val = persistedModel.transform(val_set)
predictions_test = persistedModel.transform(test_set)

evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction",
                            labelCol='label', metricName='areaUnderROC')
val_accuracy = evaluator.evaluate(predictions_val)
test_accuracy = evaluator.evaluate(predictions_test)

# save the trained model for future use
persistedModel.write().overwrite().save(model_pipeline_path)

# write the model accuracy to bigquery
data = [(f"{time.strftime('%y_%d_%m')}",test_accuracy,"lr")]

schema = StructType([ \
    StructField("date",StringType(),True), \
    StructField("accuracy",FloatType(),True), \
    StructField("model_name",StringType(),True) \
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()

df.createOrReplaceTempView("accuracy_data")
test_op = spark.sql('SELECT * FROM accuracy_data')
test_op.show()
test_op.write.format('bigquery').option(
    'table', op_table_name).mode("append").save()




