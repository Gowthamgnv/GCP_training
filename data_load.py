from google.cloud import bigquery
# from google.oauth2 import service_account
# from google.oauth2 import service_account
project_id = 'bigqueryproject-386417'
key_path = 'ServiceAccountKey.json'

# Initialize the BigQuery client with the credentials
client = bigquery.Client.from_service_account_json(key_path)

# Construct a BigQuery client object.

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "bigqueryproject-386417.BQ_Dataset.sample_load"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("post_abbr", "STRING"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))