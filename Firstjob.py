import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceAccountKey.json'

def run(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(
        flags=None,
        runner='DataflowRunner',
        project='bigqueryproject-386417',
        region='us-central1',
        job_name='data-flow-job1',
        temp_location='gs://gcp_training1/tmp/',
        staging_location= 'gs://gcp_training1/'
    )
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read data from BigQuery
        read_data = (pipeline
                     | beam.io.gcp.bigquery.ReadFromBigQuery(table='bigquery-public-data.covid19_aha.staffing'))

        # Write the transformed data to a file
        read_data | beam.io.gcp.bigquery.WriteToBigQuery(table='bigqueryproject-386417.BQ_Dataset.sample1',
                                                         schema="SCHEMA_AUTODETECT",
                                                         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    
