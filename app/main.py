from datetime import datetime

import click
from pyspark.sql import SparkSession
from google.cloud.bigquery import SchemaField

from .bigquery import create_input_table
from .dataproc import extract, train_show


SCHEMA = [
    SchemaField('weight_pounds', 'float'),
    SchemaField('mother_age', 'integer'),
    SchemaField('father_age', 'integer'),
    SchemaField('gestation_weeks', 'integer'),
    SchemaField('weight_gain_pounds', 'integer'),
    SchemaField('apgar_5min', 'integer'),
]


@click.command()
@click.option("dataset-id", default="natality_regression")
@click.option("table-id", default="regression_input")
def main(dataset_id, table_id):

    create_input_table(dataset_id, table_id, SCHEMA)

    spark = SparkSession.builder.getOrCreate()
    
    # Use Cloud Dataprocs automatically propagated configurations to get
    # the Google Cloud Storage bucket and Google Cloud Platform project for this
    # cluster.
    bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
    project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")

    # Set an input directory for reading data from Bigquery.
    todays_date = datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S")
    input_directory = "gs://{}/tmp/natality-{}".format(bucket, todays_date)

    # Set the configuration for importing data from BigQuery.
    # Specifically, make sure to set the project ID and bucket for Cloud Dataproc,
    # and the project ID, dataset, and table names for BigQuery.

    conf = {
        # Input Parameters
        "mapred.bq.project.id": project,
        "mapred.bq.gcs.bucket": bucket,
        "mapred.bq.temp.gcs.path": input_directory,
        "mapred.bq.input.project.id": project,
        "mapred.bq.input.dataset.id": dataset_id,
        "mapred.bq.input.table.id": table_id,
    }

    train = extract(spark, conf)
    train_show(train)
