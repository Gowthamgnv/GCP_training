import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
from apache_beam.options.pipeline_options import SetupOptions
import os
import logging
from simple_salesforce import Salesforce

# from main import
from credentials import * 

os.environ['GOOGLE_APPLICATION_CREDENTIAL'] = 'ServiceAccountKey.json'

sf = Salesforce(username= username, password= password, security_token= security_token)

query = "SELECT Id, Name, Email, Level__c, Title FROM Contact"
response = sf.query(query)

records = response['records']
data = []

for record in records:
    d = {"Id": record['Id'], "Name": record['Name'], "Email": record['Email'], "Level__c": record['Level__c'], "Title": record['Title']}
    data.append(d)

# Set up the pipeline options
def run(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project='bigqueryproject-386417',
    job_name='salesforce-bigquery-job',
    region='us-central1',
    temp_location='gs://gcp_training1/tmp/'
)

    pipeline_options.view_as(SetupOptions).save_main_session = True

# Define the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read the data from GCS
        salesforce_records = pipeline | beam.Create(data)

        salesforce_records | 'WriteToBigQuery' >> WriteToBigQuery(table='bigqueryproject-386417.BQ_Data.salesforce_data',
                                                      schema='Id:STRING,Name:STRING,Email:STRING,Level__c:STRING,Title:STRING',
                                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()









