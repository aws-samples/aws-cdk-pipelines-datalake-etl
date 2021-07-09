# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import botocore
import os
import logging
import os.path
from datetime import datetime
import dateutil.tz
import uuid


def start_etl_job_run(execution_id, p_stp_fn_time, sfn_arn, sfn_name, table_name, sfn_input):
    """
    Function to insert entry in dynamodb table for audit trail
    @param execution_id:
    @param p_stp_fn_time:
    @param sfn_arn:
    @param sfn_name:
    @param table_name:
    @param sfn_input:
    """
    try:
        print('start_etl_job_run')
        logger.info('[INFO] start_etl_job_run() called')
        item = {}
        item['execution_id'] = execution_id
        item['sfn_execution_name'] = sfn_name
        item['sfn_arn'] = sfn_arn
        item['sfn_input'] = sfn_input
        item['job_latest_status'] = 'STARTED'
        item['job_start_date'] = p_stp_fn_time
        item['joblast_updated_timestamp'] = p_stp_fn_time
        dynamo_client = boto3.resource('dynamodb')
        table = dynamo_client.Table(table_name)
        table.put_item(Item=item)
    except botocore.exceptions.ClientError as error:
        logger.info('[ERROR] Dynamodb process failed:{}'.format(error))
        raise error
    except Exception as e:
        logger.info('[ERROR] Dynamodb process failed:{}'.format(e))
        raise e
    logger.info('[INFO] start_etl_job_run() execution completed')
    print('insert table completed')


# Function for logger
def load_log_config():
    # Basic config. Replace with your own logging config if required
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    return root


# Logger initiation
logger = load_log_config()


def lambda_handler(event, context):
    print(event)
    lambda_message = event['Records'][0]
    source_bucket_name = lambda_message['s3']['bucket']['name']
    key = lambda_message['s3']['object']['key']

    p_full_path = key
    # first object/directory name after buckname will be used as source system name example:
    # s3://<buckename>/<source_system_name>/<table_name>
    p_source_system_name = key.split('/')[0]
    # second object/directory name after buckname will be used as tablename name example:
    # s3://<buckename>/<source_system_name>/<table_name>
    p_table_name = key.split('/')[1]
    p_file_dir = os.path.dirname(p_full_path)
    p_file_dir_upd = p_file_dir.replace('%3D', '=')
    p_base_file_name = os.path.basename(p_full_path)
    sfn_arn = os.environ['SFN_STATE_MACHINE_ARN']
    target_bucket_name = os.environ['target_bucket_name']
    raw_to_conformed_etl_job_name = 'raw_to_conformed_etl_job'

    logger.info('bucket: ' + source_bucket_name)
    logger.info('key: ' + key)
    logger.info('source system name: ' + p_source_system_name)
    logger.info('table name: ' + p_table_name)
    logger.info('File Path: ' + p_file_dir)
    logger.info('p_file_dir_upd: ' + p_file_dir_upd)
    logger.info('file base name: ' + p_base_file_name)
    logger.info('state machine arn: ' + sfn_arn)
    logger.info('target bucket name: ' + target_bucket_name)

    if p_base_file_name != '':
        # Capturing the current time in CST
        central = dateutil.tz.gettz('US/Central')
        now = datetime.now(tz=central)
        p_ingest_time = now.strftime('%m/%d/%Y %H:%M:%S')
        logger.info(p_ingest_time)
        # Time stamp for the stepfunction name
        p_stp_fn_time = now.strftime('%Y%m%d%H%M%S%f')

        p_year = now.strftime('%Y')
        p_month = now.strftime('%m')
        p_day = now.strftime('%d')

        logger.info('year: ' + p_year)
        logger.info('p_month: ' + p_month)
        logger.info('p_day: ' + p_day)
        logger.info('sfn name: ' + p_base_file_name + '-' + p_stp_fn_time)
        sfn_name = p_base_file_name + '-' + p_stp_fn_time
        print('before step function')
        execution_id = str(uuid.uuid4())
        sfn_input = json.dumps(
            {
                'JOB_NAME': raw_to_conformed_etl_job_name,
                'target_databasename': p_source_system_name,
                'target_bucketname': target_bucket_name,
                'source_bucketname': source_bucket_name,
                'source_key': p_file_dir_upd, 
                'base_file_name': p_base_file_name,
                'p_year': p_year,
                'p_month': p_month,
                'p_day': p_day,
                'table_name': p_table_name,
                'execution_id': execution_id,
            }
        )
        logger.info(sfn_input)
        try:
            sfn_client = boto3.client('stepfunctions')
            sfn_response = sfn_client.start_execution(
                stateMachineArn=sfn_arn,
                name=sfn_name,
                input=sfn_input
            )
            print(sfn_response)
        except botocore.exceptions.ClientError as error:
            logger.info('[ERROR] Step function client process failed:{}'.format(error))
            raise error
        except Exception as e:
            logger.info('[ERROR] Step function call failed:{}'.format(e))
            raise e

        start_etl_job_run(execution_id, p_stp_fn_time, sfn_arn, sfn_name, os.environ['DYNAMODB_TABLE_NAME'], sfn_input)

    return {
        'statusCode': 200,
        'body': json.dumps('Step function triggered successfully!')
    }
