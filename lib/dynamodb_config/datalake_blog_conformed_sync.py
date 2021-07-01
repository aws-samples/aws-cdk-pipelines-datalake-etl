# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
import json
import boto3
from ..dynamodb_stack import get_transformation_rules_table_name


def main():
    target_environment = sys.argv[1]
    role_arn = sys.argv[2]

    sts_connection = boto3.client('sts')
    target_account = sts_connection.assume_role(
        RoleArn=role_arn,
        RoleSessionName='cross_account_access'
    )

    access_key = target_account['Credentials']['AccessKeyId']
    secret_key = target_account['Credentials']['SecretAccessKey']
    session_token = target_account['Credentials']['SessionToken']

    # create service client using the assumed role credentials, e.g. Amazon DynamoDB
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )
    table = dynamodb.Table(get_transformation_rules_table_name(target_environment))
    with open('./lib/dynamodb_config/datalake_blog_conformed_logic.json') as json_file:
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
