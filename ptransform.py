import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import logging
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceAccountKey.json'


def sum_total_council_district(element): 
    s = 0
    for row in element:
        s += row['total_council_district']
        return s

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
                     | beam.io.gcp.bigquery.ReadFromBigQuery(table='bigqueryproject-386417.BQ_Dataset.bikesharing'))


    with beam.Pipeline(options=pipeline_options) as p:
        source_data = p | 'ReadFromBigQuery' >> ReadFromBigQuery(table = 'bigqueryproject-386417.BQ_Dataset.bikesharing')
        aggregate_data = source_data | beam.GroupBy(lambda x: x["data_file_month"])
        final_data = aggregate_data | beam.Map(lambda x: {'Month': x[0], "Total_council_district": x[1]})
        final_data | 'WriteToBigQuery' >> WriteToBigQuery(table='bigqueryproject-386417.BQ_Dataset.samplenew',
                                                       schema= 'Month:INTEGER,Total_number_of_docks:INTEGER',
                                                       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    
