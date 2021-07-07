# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import json
import boto3
import botocore
import traceback
from ..dynamodb_stack import get_transformation_rules_table_name


def main():
    """
    Main method
    :return: 
    """
    target_environment = sys.argv[1]
    role_output_name = sys.argv[2]

    try:
        cloudformation_client = boto3.client('cloudformation')
        response = cloudformation_client.list_exports()
    except botocore.exceptions.ClientError as error:
        print(f'Error retrieving exports: {error}')
        traceback.print_exc()
        raise
    except Exception as error:
        print(f'Error retrieving exports: {error}')
        traceback.print_exc()
        raise

    role_arn = ''
    exports = response.get('Exports', [])
    for export in exports:
        if export.get('Name', None) == role_output_name:
            role_arn = export.get('Value', None)

    try:
        sts_connection = boto3.client('sts')
        target_account = sts_connection.assume_role(
            RoleArn=role_arn,
            RoleSessionName='cross_account_access'
        )
    except botocore.exceptions.ClientError as error:
        print(f'Error assuming role: {error}')
        traceback.print_exc()
        raise
    except Exception as error:
        print(f'Error assuming role: {error}')
        traceback.print_exc()
        raise

    access_key = target_account['Credentials']['AccessKeyId']
    secret_key = target_account['Credentials']['SecretAccessKey']
    session_token = target_account['Credentials']['SessionToken']

    try:
        # create service client using the assumed role credentials, e.g. Amazon DynamoDB
        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
        )
        table = dynamodb.Table(
            get_transformation_rules_table_name(target_environment))
    except botocore.exceptions.ClientError as error:
        print(f'Error connecting to dynamodb: {error}')
        traceback.print_exc()
        raise
    except Exception as error:
        print(f'Error connecting to dynamodb: {error}')
        traceback.print_exc()
        raise

    with open(
        './lib/etl_job_config/etl_job_config_conformed_stage.json') as json_file:
        records = json.load(json_file)

    for record in records:
        existing_record = table.get_item(
            Key={'load_name': record['load_name']}
        )
        if existing_record:
            print('Updating: ', record['load_name'])
            table.update_item(
                Key={'load_name': record['load_name']},
                UpdateExpression="set transfer_logic=:transfer_logic",
                ExpressionAttributeValues={
                    ':transfer_logic': record['transfer_logic']
                },
                ReturnValues='UPDATED_NEW'
            )
        else:
            print('Creating: ', record['load_name'])
            table.put_item(
                Item=record,
            )


if __name__ == '__main__':
    main()
