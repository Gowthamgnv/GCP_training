from google.cloud import bigquery
# Create a BigQuery client object.
# client = bigquery.Client()

client = bigquery.Client(project='bigqueryproject-386417')

# Set the ID of the dataset to create.
dataset_id = "BQ_Data"
# table="table"
# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
1
# Specify the geographic location where the dataset should reside.
dataset.location = "US"
# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset, timeout=30) # Make an API request.
print(f"Created dataset: {dataset.full_dataset_id}")