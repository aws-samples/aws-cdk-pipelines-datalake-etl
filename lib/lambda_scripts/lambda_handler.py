# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import boto3


def lambda_handler(event, context):
    """
    AWS Lambda's handler function. It reads the value of environment variable 'sfn_state_machine_arn ' and calls
    State machine.
    :param event:
    :param context:
    :return:
    """
    sfn_arn = os.environ['sfn_state_machine_arn']
    sfn_client = boto3.client("stepfunctions")
    response = sfn_client.start_execution(stateMachineArn=sfn_arn)
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully exected Step Functions State Machine!')
    }
