"""Run a linear regression using Apache Spark ML.

In the following PySpark (Spark Python API) code, we take the following actions:

  * Load a previously created linear regression (Google BigQuery) input table
    into our Google Cloud Dataproc Spark cluster as an RDD (Resilient
    Distributed Dataset)
  * Transform the RDD into a Spark Dataframe
  * Vectorize the features on which the model will be trained
  * Compute a linear regression using Spark ML

"""

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F


def extract(spark, conf):
    # Read the data from BigQuery into Spark as an RDD.
    table_data = spark.sparkContext.newAPIHadoopRDD(
        "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "com.google.gson.JsonObject",
        conf=conf)

    # Extract the JSON strings from the RDD.
    table_json = table_data.map(lambda x: x[1])

    features = [
        "mother_age", 
        "father_age", 
        "gestation_weeks", 
        "weight_gain_pounds", 
        "apgar_5min"
    ]
    # Load the JSON strings as a Spark Dataframe.
    natality_data = spark.read.json(table_json)
    natality_data.printSchema()
    natality_data = natality_data.select(
        ["weight_pounds"] + 
        [F.col(x).cast('double').alias(x) for x in features]
    )
    natality_data.printSchema()

    assembler = VectorAssembler(inputCols=features, outputCol="features")

    # Create an input DataFrame for Spark ML using the above function.
    training_data = (
        assembler
        .transform(natality_data)
        .selectExpr("weight_pounds AS label", "features")
    )

    return training_data


def transform_show(training_data):
    training_data.cache()

    # Construct a new LinearRegression object and fit the training data.
    lr = LinearRegression(maxIter=5, regParam=0.2, solver="normal")
    model = lr.fit(training_data)
    # Print the model summary.
    print("Coefficients:" + str(model.coefficients))
    print("Intercept:" + str(model.intercept))
    print("R^2:" + str(model.summary.r2))
    model.summary.residuals.show()

