import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from datetime import datetime
import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
# from apache_beam.options.pipeline_options import ReadFromBigQuery
# from apache_beam.options.pipeline_options import WriteToBigQuery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceAccountKey.json'

class MakeAuditElement(beam.DoFn):
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def process(self, element):
        row = {'Total_Rows': element['f0_'], 'Timestamp': self.timestamp}
        yield row

# Set up the pipeline options
def run(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(
        flags=None,
        runner='DataflowRunner',
        project='bigqueryproject-386417',
        region='us-central1',
        job_name='data-flow-job2',
        temp_location='gs://gcp_training1/tmp/',
        staging_location='gs://gcp_training1/'
    )
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


    with beam.Pipeline(options=pipeline_options) as pipeline:
        my_query = "select * except(Row_Number) from (select *, row_number() over() as Row_Number from `BQ_Data.ml_dataset_iris`) where Row_Number > (select Total_Rows from `BQ_Data.AUDIT` Order By Timestamp DESC Limit 1);"
        data = pipeline | 'Get Latest Extract' >> ReadFromBigQuery(query=my_query, use_standard_sql=True)

        insert_time = datetime.now().isoformat()

        # Write data to GCS
        output_path = 'gs://gcp_training1/files/revenue' + str(insert_time)
        data | beam.Map(print)
        data | 'Write to GCS' >> beam.io.WriteToText(output_path, file_name_suffix='.csv')

        query = 'SELECT count(*) FROM BQ_Data.ml_dataset_iris'
        count = pipeline | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query)

        audit = count | beam.ParDo(MakeAuditElement(insert_time))

        audit | 'WriteToBigQuery' >> WriteToBigQuery(table='bigqueryproject-386417.BQ_Data.AUDIT',
                                                schema='Total_Rows:INTEGER,Timestamp:TIMESTAMP',
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


