#!/usr/bin/python
"""Create a Google BigQuery linear regression input table.

In the code below, the following actions are taken:
  * A new dataset is created "natality_regression."
  * A new table "regression_input" is created to hold the inputs for our linear
    regression.
  * A query is run against the public dataset,
    bigquery-public-data.samples.natality, selecting only the data of interest
    to the regression, the output of which is stored in the "regression_input"
    table.
  * The output table is moved over the wire to the user's default project via
    the built-in BigQuery Connector for Spark that bridges BigQuery and Cloud
    Dataproc.
"""

from google.cloud import bigquery


def create_input_table(dataset_id, table_id, table_schema):
    # Create a new Google BigQuery client using Google Cloud Platform project
    # defaults.
    bigquery_client = bigquery.Client()

    # Prepare a reference to the new dataset.
    dataset_ref = bigquery_client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)

    # Create the new BigQuery dataset.
    dataset = bigquery_client.create_dataset(dataset)

    # In the new BigQuery dataset, create a new table.
    table_ref = dataset.table(table_id)

    # Assign the schema to the table and create the table in BigQuery.
    table = bigquery.Table(table_ref, schema=table_schema)
    table = bigquery_client.create_table(table)

    # Set up a query in Standard SQL.
    # The query selects the fields of interest.
    QUERY = """
    SELECT weight_pounds, mother_age, father_age, gestation_weeks,
    weight_gain_pounds, apgar_5min
    FROM `bigquery-public-data.samples.natality`
    WHERE weight_pounds is not null
    and mother_age is not null and father_age is not null
    and gestation_weeks is not null
    and weight_gain_pounds is not null
    and apgar_5min is not null
    """

    # Configure the query job.
    job_config = bigquery.QueryJobConfig()
    # Set the output table to the table created above.
    dest_dataset_ref = bigquery_client.dataset(dataset_id)
    dest_table_ref = dest_dataset_ref.table(table_id)
    job_config.destination = dest_table_ref
    # Allow the results table to be overwritten.
    job_config.write_disposition = 'WRITE_TRUNCATE'
    # Use Standard SQL.
    job_config.use_legacy_sql = False
    # Run the query.
    query_job = bigquery_client.query(QUERY, job_config=job_config)

