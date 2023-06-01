import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime
import datetime
from apache_beam.pvalue import TaggedOutput
from google.cloud import bigquery
import logging
import json
import os
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceAccountKey.json'


# Define a custom DoFn to handle success and failure data
class DoFnMethods(beam.DoFn):
    def __init__(self,file_name):
        self.file_name = file_name
        self.success = 0
        self.error = 0
        self.total = 0
        self.window = beam.transforms.window.GlobalWindow()

    def process(self, element, window=beam.DoFn.WindowParam):
        row  = element.split(',')
        row = {'pclass': row[0], 'Sex': row[1], 'TotalPassengers': row[2]}
        self.total += 1
        yield row
        # if row['CITY'].upper() in ['PUNE','BANGALORE']:
        #     self.success += 1
        # else:
        #     self.error += 1

    def finish_bundle(self):
        current_timestamp = datetime.datetime.now()
        yield beam.pvalue.TaggedOutput('auditlog',beam.utils.windowed_value.WindowedValue(
            value = {'file_name': self.file_name, 'success':self.total, 'error':self.error,'total':self.total, 'timestamp':current_timestamp},
            timestamp=0,
            windows=[self.window],
        ))

table_schema = 'pclass:INTEGER, Sex:STRING, TotalPassengers:INTEGER'

GCS_INPUT_FILE = "gs://gcp_training1/titanic_transfered.csv"


BIGQUERY_PROJECT = "bigqueryproject-386417"
BIGQUERY_DATASET = "BQ_Dataset "
BIGQUERY_TABLE = "Titanic_load"
BIGQUERY_AUDIT_TABLE = "audit_table"
GCS_ARCHIVE_FOLDER = "gs://gcp_training1/archives/"

# Define the input data source
input_file ='gs://gcp_training1/titanic_transfered.csv'
input_file_path = 'gs://gcp_training1/'
input_file_name = 'titanic_transfered.csv'



audit_table_schema = 'file_name:STRING,success:INTEGER,error:INTEGER,total:INTEGER,timestamp:TIMESTAMP'

def run(GCS_INPUT_FILE,argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(
        flags=None,
        runner='DataflowRunner',
        project='bigqueryproject-386417',
        region='us-central1',
        job_name='data-flow-job',
        temp_location='gs://gcp_training1/tmp/',
        staging_location='gs://gcp_training1/'
    )
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as pipeline:
        print('creating data')
        
        # Read data from the CSV file
        data = (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(GCS_INPUT_FILE, skip_header_lines=1))

        # Process the data and handle success and failure rows
        print('processing data')
        output,auditlog = data | 'DoFn methods' >> beam.ParDo(DoFnMethods(input_file_name)).with_outputs('auditlog', main='element')
        
        # writing ouput to bigquery table
        # print('Writing data to Bigquery table')
        output | 'writing to bigquery' >> beam.io.WriteToBigQuery(
            table='bigqueryproject-386417:BQ_Dataset.Titanic_load',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        # writing auditlog to bigquery table
        # print('Writing data to Bigquery audit table')
        auditlog | 'writing to audit table' >> beam.io.WriteToBigQuery(
            table='bigqueryproject-386417:BQ_Dataset.audit_log',
            schema=audit_table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        # moving file to archive folder with tamestamp in name
        # archive_folder = os.path.dirname(GCS_ARCHIVE_FOLDER)
        # print(archive_folder)
        # archive_file_path = os.path.join(archive_folder, f"{input_file_name.rstrip('.csv')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        
        # #only for windows
        # archive_file_path = archive_file_path.replace("\\","/")
        # print(archive_file_path)
        # fs = GCSFileSystem(pipeline_options=pipeline_options)
        # fs.rename([input_file], [archive_file_path])
        print("File renamed successfully.")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(GCS_INPUT_FILE)