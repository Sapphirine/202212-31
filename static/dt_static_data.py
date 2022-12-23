import time
import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import (IDF, CountVectorizer, StringIndexer,
                                Tokenizer)
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import FloatType, StringType, StructField, StructType


#create spark session and load necessary files
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()
sqlContext = SQLContext(spark)
bucket = '' #Enter your bucket name
model_pipeline_path = 'gs://{}/models/static/dt_model_pipeline'.format(bucket)
table_name = 'bda.preprocessed_static_data'
op_table_name = 'bda.daily_accuracy'


# load the preprocessed data stored in bucket
preprocessed_path = 'gs://{}/static_data/preprocessed_static_data.csv'.format(bucket)
preprocessed_df = spark.read.format("csv").option("header", "true").load(preprocessed_path)

spark.conf.set('temporaryGcsBucket', bucket)
preprocessed_df = preprocessed_df.select(preprocessed_df.comment_text, preprocessed_df.sentiment_value)
preprocessed_df.show()

# Divide the dataset into train and test set for evaluation
(train_set, val_set, test_set) = preprocessed_df.randomSplit([0.98, 0.01, 0.01], seed = 2000)

# create the modules like vectorizer, ml model and tokenizer needed for pipeline building
tokenizer = Tokenizer(inputCol="comment_text", outputCol="words")
cv = CountVectorizer(vocabSize=2**16, inputCol="words", outputCol='cv')
idf = IDF(inputCol='cv', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
label_stringIdx = StringIndexer(inputCol = "sentiment_value", outputCol = "label")
dt_model = DecisionTreeClassifier(labelCol = "label", featuresCol = "features")

# Create a pipeline for training the model and performing predictions
pipeline = Pipeline(stages=[tokenizer, cv, idf, label_stringIdx, dt_model])
pipelineFit = pipeline.fit(train_set)
predictions = pipelineFit.transform(val_set)
accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(val_set.count())
print("Accuracy Score: {0:.4f}".format(accuracy))

# dump accuracy of model into bigquery
pipelineFit.write().overwrite().save(model_pipeline_path)

# write accuracy to bigquery for later use
data = [(f"{time.strftime('%y_%d_%m')}",accuracy,"dt")]

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
