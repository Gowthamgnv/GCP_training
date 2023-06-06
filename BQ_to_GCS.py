import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
import os
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.gcsio import GcsIO
import datetime
import logging
os.environ['GOOGLE_CLOUD_PROJECT'] = 'bigqueryproject-386417'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceAccountKey.json'

def run_dataflow_job(project_id, temp_location,destination_file,file_name,save_main_session=True):
    # Create pipeline options
    options = PipelineOptions(
        runner='DataflowRunner',
        job_name = 'bq-to-gcs',
        project = project_id,
        region='us-central1',
        staging_location='gs://gcp_training1/',
        temp_location = temp_location
    )
    options.view_as(SetupOptions).save_main_session = save_main_session
    # Create the pipeline
    with beam.Pipeline(options=options) as pipeline:
        class DeleteFile(beam.DoFn):
            def process(self, element):
                gcs = GcsIO()
                gcs.delete([element])
                yield
        # Read data from the source table
        source_data = (
            pipeline
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query='SELECT * FROM bigqueryproject-386417.BQ_Data.ml_dataset_iris',
                use_standard_sql=True)
            # |    beam.Map(print)
        )

        source_data | 'WriteToText' >> beam.io.WriteToText(destination_file,file_name)
        


project_id='bigqueryproject-386417'
dataset_id='bigqueryproject-386417.BQ_Data'
table_id='ml_dataset_iris'
dataset_ref = bigquery.DatasetReference(project=project_id, dataset_id=dataset_id)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    source_table = 'bigqueryproject-386417.BQ_Data.ml_dataset_iris'
    destination_table = dataset_ref.table(table_id)
    file_name=f"ml_dataset_iris{datetime.datetime.now()}.csv"
    # destination_table = bigquery.TableReference(dataset_ref=dataset_id, table_id=table_id)
    # destination_table = 'bigqueryproject-386417.BQ_Data.ml_dataset_iris'
    project_id = 'bigqueryproject-386417'
    temp_location = 'gs://gcp_training1/temp'  # Set your desired GCS bucket
    input_file=f"gs://gcp_training1/files/{file_name}"
    destination_file=f"gs://gcp_training1/archived/"
    run_dataflow_job(project_id, temp_location,destination_file,file_name)
