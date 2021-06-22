import json
import boto3
import os
import logging
import os.path
import urllib.parse
from datetime import datetime
import dateutil.tz
import uuid


def statr_etl_job_run(execution_id,p_stp_fn_time,sfn_arn,sfn_name,table_name,sfn_input):
    """function to start the new pipeline
        
    """
    try:
        print("statr_etl_job_run")
        logger.info("[INFO] statr_etl_job_run() called")
        
        # current_time = datetime.datetime.utcnow()
        # utc_time_iso = get_timestamp_iso(current_time)
        # local_date_iso = get_local_date()

        
        item = {}
        item["execution_id"] = execution_id
        item["sfn_execution_name"] = sfn_name
        item["sfn_arn"] = sfn_arn
        item["sfn_input"] = sfn_input
        item["job_latest_status"] = "started"
        item["job_start_date"] = p_stp_fn_time
        item["joblast_updated_timestamp"] = p_stp_fn_time
        client = boto3.resource('dynamodb')
        table = client.Table(table_name)
        table.put_item(Item=item)
    except Exception as e:
        logger.info("[ERROR] Dynamodb process failed:{}".format(e))
        raise e
    logger.info("[INFO] statr_etl_job_run() execution completed")
    print("insert table completed")


#Function for logger
def load_log_config():
    # Basic config. Replace with your own logging config if required
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root

  
#Logger initiation
logger = load_log_config()

def lambda_handler(event, context):
    
    print(event)
    lambda_message = event['Records'][0]
    bucket = lambda_message['s3']['bucket']['name']
    key = lambda_message['s3']['object']['key']
    
    p_full_path = key
    p_source_system_name = key.split('/')[0]
    p_table_name = key.split('/')[1]
    p_file_dir = os.path.dirname(p_full_path)
    p_file_dir_upd = p_file_dir.replace("%3D","=")
    p_base_file_name = os.path.basename(p_full_path)

    logger.info('bucket: '+bucket)
    logger.info('key: '+key)
    logger.info('source system name: '+p_source_system_name)
    logger.info('table name: '+p_table_name)
    logger.info('File Path: '+p_file_dir)
    logger.info('file base name: '+p_base_file_name)
    
    if p_base_file_name != "":
        #Capturing the current time in CST
        central = dateutil.tz.gettz('US/Central')
        now = datetime.now(tz=central)
        p_ingest_time = now.strftime("%m/%d/%Y %H:%M:%S")
        logger.info(p_ingest_time)
        #Time stamp for the stepfunction name
        p_stp_fn_time = now.strftime("%Y%m%d%H%M%S%f")
        
        logger.info("sfn name:"+p_base_file_name +'-'+p_stp_fn_time)
        
        
            
        
        sfn_name= p_base_file_name +'-'+p_stp_fn_time
        print("befor step function")
        sfn_arn = os.environ['sfn_arn']
        sfn_client=boto3.client("stepfunctions")
        
        
        execution_id = str(uuid.uuid4())
        sfn_input='{\"source_bucketname\":\"' + bucket  + \
            '\",\"base_file_name\":\"' + p_base_file_name + \
            '\",\"execution_id\":\"' + execution_id + \
            '\",\"source_key\":\"' + p_file_dir_upd + \
            '\",\"source_system_name\":\"' + p_source_system_name + \
            '\",\"table_name\":\"' + p_table_name + \
            '\" }'
            
        logger.info(sfn_input)
        
        sfn_response=sfn_client.start_execution(stateMachineArn=sfn_arn,
            name = sfn_name,
            input=sfn_input)
        print(sfn_response)
        
        statr_etl_job_run(execution_id,p_stp_fn_time,sfn_arn,sfn_name,os.environ['dynamo_tablename'],sfn_input)
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
