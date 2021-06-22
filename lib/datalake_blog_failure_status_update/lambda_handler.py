import json
import boto3
import os
import logging
import os.path
import urllib.parse
from datetime import datetime
import dateutil.tz

#Function for logger
def load_log_config():
    # Basic config. Replace with your own logging config if required
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    return root

  
#Logger initiation
logger = load_log_config()

def lambda_handler(event, context):
    # TODO implement
    print(event)
    print(event['Input'])
    
    print(event['Input']['execution_id'])
    print(event['Input']['taskresult']['Cause'])
    
    execution_id=event['Input']['execution_id']
    
    
    
    central = dateutil.tz.gettz('US/Central')
    now = datetime.now(tz=central)
    p_ingest_time = now.strftime("%m/%d/%Y %H:%M:%S")
    logger.info(p_ingest_time)
    
    status="FAILED"
    error_msg=event['Input']['taskresult']['Cause']
    #Time stamp for the stepfunction name
    p_stp_fn_time = now.strftime("%Y%m%d%H%M%S%f")
    # update table
    
    client = boto3.resource('dynamodb')
    table = client.Table(os.environ['dynamo_tablename'])
    table.update_item(
        Key={
            'execution_id': execution_id
        },
        UpdateExpression="set joblast_updated_timestamp=:lut,job_latest_status=:sts,error_message=:emsg",
        ExpressionAttributeValues={
            ':sts': status,
            ':lut': p_stp_fn_time,
            ':emsg':error_msg
        },
        ReturnValues="UPDATED_NEW"
        )
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
