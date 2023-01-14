import json
import logging
import os
import traceback
from datetime import datetime
from google.api_core import retry
from google.cloud import bigquery
from google.cloud import storage


# Get ENV variables 
BQ_PROJECT_ID = os.getenv('bq_project')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
SOURCE_LANDING_BUCKET = os.getenv('source_bucket')
DESTINATION_BUCKET = os.getenv('destination_bucket')

# Cloud storage client 
CS = storage.Client()
# BigQuery Client
BQ = bigquery.Client()


def bq_load(data, context):
    '''This function is executed whenever a file is added to Cloud Storage Landing bucket'''
    
    file_name = data['name']
    table_name = file_name.split(".")[0] 
    file_extension = str(file_name.split(".")[1])
    
    
    # Check for file extension 
    if(file_extension.lower() == "avro"):
        message = 'Perform bq load with file movement, file : \'%s\'' % (file_name)
        logging.info(message)
        _load_table_from_avro(table_name,file_name)
    elif(file_extension.lower() == "png"):
        message = 'Perform file movemnt only , file  : \'%s\'' % (file_name)
        logging.info(message)
        source_bucket_name = SOURCE_LANDING_BUCKET
        destination_bucket_name = DESTINATION_BUCKET
        _move_file(file_name,source_bucket_name,destination_bucket_name)
    elif(file_extension.lower() == "parquet"):
        message = 'Perform file movemnt only , file  : \'%s\'' % (file_name)
        logging.info(message)
        _load_table_from_parquet(table_name,file_name)
    elif(file_extension.lower() == "csv"):
        message = 'Perform file movemnt only , file  : \'%s\'' % (file_name)
        logging.info(message)
        _load_table_from_csv(table_name,file_name)	
    elif(file_extension.lower() == "json"):
        message = 'Perform file movemnt only , file  : \'%s\'' % (file_name)
        logging.info(message)
        source_bucket_name = SOURCE_LANDING_BUCKET
        _load_table_from_json(table_name,file_name,source_bucket_name)	
        
    else: 
        message = 'Not supported file format, file : \'%s\'' % (file_name)
        logging.info(message)
        
def _move_file(file_name,source_bucket_name,destination_bucket_name):
    '''This function perform file movement '''
    
    source_bucket = CS.get_bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)
    
    destination_bucket = CS.get_bucket(destination_bucket_name)

    source_bucket.copy_blob(source_blob, destination_bucket, file_name)
    source_blob.delete()

    logging.info('File \'%s\' moved from \'%s\' to \'%s\'',
                 file_name,
                 source_bucket_name,
                 destination_bucket_name
                 )
def _if_tbl_exists(table_ref):
    ''' This function check if bigquery table is present or not '''
    from google.cloud.exceptions import NotFound
    try:
        BQ.get_table(table_ref)
        return True
    except NotFound:
        return False


def _load_table_from_parquet(table_name,file_name):
    '''This function will perform loading bq table and file movement'''
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "%s.%s.%s" % (BQ_PROJECT_ID,BQ_DATASET_ID,table_name)
    message = 'Table_id  : \'%s\'' % (table_id)
    logging.info(message)

    if(_if_tbl_exists(table_id)):
        destination_table = BQ.get_table(table_id)
        num_rows_added = destination_table.num_rows
    else:
        num_rows_added = 0

    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
    source_format=bigquery.SourceFormat.PARQUET,
    )
    uri = 'gs://%s/%s' % (SOURCE_LANDING_BUCKET,file_name)
    try:
        
        load_job = BQ.load_table_from_uri(
        uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.
        destination_table = BQ.get_table(table_id)
        logging.info('Table \'%s\' loaded with \'%s\' rows ',
                    table_id,
                    destination_table.num_rows-num_rows_added
                    )
        source_bucket_name = SOURCE_LANDING_BUCKET
        destination_bucket_name = DESTINATION_BUCKET
        _move_file(file_name,source_bucket_name,destination_bucket_name)     
    except Exception:
        error_message = 'Invalid file format for file name : \'%s\'' % (file_name)
        logging.error(error_message)

    print("Job finished.")
      

def _load_table_from_avro(table_name,file_name):
    '''This function will perform loading bq table and file movement'''
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "%s.%s.%s" % (BQ_PROJECT_ID,BQ_DATASET_ID,table_name)
    message = 'Table_id  : \'%s\'' % (table_id)
    logging.info(message)

    if(_if_tbl_exists(table_id)):
        destination_table = BQ.get_table(table_id)
        num_rows_added = destination_table.num_rows
    else:
        num_rows_added = 0

    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
    source_format=bigquery.SourceFormat.AVRO,
    )
    uri = 'gs://%s/%s' % (SOURCE_LANDING_BUCKET,file_name)
    try:
        
        load_job = BQ.load_table_from_uri(
        uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.
        destination_table = BQ.get_table(table_id)
        logging.info('Table \'%s\' loaded with \'%s\' rows ',
                    table_id,
                    destination_table.num_rows-num_rows_added
                    )
        source_bucket_name = SOURCE_LANDING_BUCKET
        destination_bucket_name = DESTINATION_BUCKET
        _move_file(file_name,source_bucket_name,destination_bucket_name)     
    except Exception:
        error_message = 'Invalid file format for file name : \'%s\'' % (file_name)
        logging.error(error_message)

    print("Job finished.")


def _load_table_from_csv(table_name,file_name):
    '''This function will perform loading bq table and file movement'''
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "%s.%s.%s" % (BQ_PROJECT_ID,BQ_DATASET_ID,table_name)
    message = 'Table_id  : \'%s\'' % (table_id)
    logging.info(message)

    if(_if_tbl_exists(table_id)):
        destination_table = BQ.get_table(table_id)
        num_rows_added = destination_table.num_rows
    else:
        num_rows_added = 0

    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("post_abbr", "STRING"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
    )
    uri = 'gs://%s/%s' % (SOURCE_LANDING_BUCKET,file_name)
    try:
            
        load_job = BQ.load_table_from_uri(
        uri, table_id, job_config=job_config
        )  # Make an API request.
    
        load_job.result()  # Waits for the job to complete.
        destination_table = BQ.get_table(table_id)
        logging.info('Table \'%s\' loaded with \'%s\' rows ',
                    table_id,
                    destination_table.num_rows-num_rows_added
                    )
        source_bucket_name = SOURCE_LANDING_BUCKET
        destination_bucket_name = DESTINATION_BUCKET
        _move_file(file_name,source_bucket_name,destination_bucket_name)     
    except Exception:
        error_message = 'Invalid file format for file name : \'%s\'' % (file_name)
        logging.error(error_message)

    print("Job finished.")
       

def _load_table_from_json(table_name,file_name,source_bucket_name):
    table_id = "%s.%s.%s" % (BQ_PROJECT_ID,BQ_DATASET_ID,table_name)
    message = 'Table_id  : \'%s\'' % (table_id)
    logging.info(message)

    if(_if_tbl_exists(table_id)):
        destination_table = BQ.get_table(table_id)
        num_rows_added = destination_table.num_rows
    else:
        num_rows_added = 0

    job_config = bigquery.LoadJobConfig(
	schema=[
		bigquery.SchemaField("name", "STRING"),
		bigquery.SchemaField("post_abbr", "STRING"),
	],
	source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
	)
    uri = 'gs://%s/%s' % (SOURCE_LANDING_BUCKET,file_name)
    try:
             
        load_job = BQ.load_table_from_uri(
        uri, table_id, job_config=job_config
        )  # Make an API request.	
    
        load_job.result()  # Waits for the job to complete.
        destination_table = BQ.get_table(table_id)
        logging.info('Table \'%s\' loaded with \'%s\' rows ',
                    table_id,
                    destination_table.num_rows-num_rows_added
                    )
        source_bucket_name = SOURCE_LANDING_BUCKET
        destination_bucket_name = DESTINATION_BUCKET
        _move_file(file_name,source_bucket_name,destination_bucket_name)         
    except Exception:
        error_message = 'Invalid file format for file name : \'%s\'' % (file_name)
        logging.error(error_message)

    print("Job finished.")
     
def _handle_error():
    message = 'Error streaming file. Cause: %s' % (traceback.format_exc())
    print(message)
     
def _handle_error():
    message = 'Error streaming file. Cause: %s' % (traceback.format_exc())
    print(message)


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened''' 

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
