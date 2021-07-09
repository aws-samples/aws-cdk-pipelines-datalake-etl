# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk.core as cdk
import aws_cdk.aws_dynamodb as dynamodb

from .configuration import (
    PROD, TEST, get_logical_id_prefix, get_resource_name_prefix,
)


class DynamoDbStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, target_environment: str, **kwargs) -> None:
        """
        CloudFormation stack to create DynamoDB Tables.

        @param scope cdk.Construct: Parent of this stack, usually an App or a Stage, but could be any construct.
        @param construct_id str: The construct ID of this stack. If stackName is not explicitly defined,
        this id (and any parent IDs) will be used to determine the physical ID of the stack.
        @param target_environment str: The target environment for stacks in the deploy stage
        @param kwargs:
        """
        super().__init__(scope, construct_id, **kwargs)

        logical_id_prefix = get_logical_id_prefix()
        resource_name_prefix = get_resource_name_prefix().replace('-', '_')

        self.removal_policy = cdk.RemovalPolicy.DESTROY
        if (target_environment == PROD or target_environment == TEST):
            self.removal_policy = cdk.RemovalPolicy.RETAIN

        self.job_audit_table = dynamodb.Table(
            self,
            f'{target_environment}{logical_id_prefix}EtlAuditTable',
            table_name=f'{target_environment.lower()}-{resource_name_prefix}-etl-job-audit',
            partition_key=dynamodb.Attribute(name='execution_id', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            encryption=dynamodb.TableEncryption.DEFAULT,
            point_in_time_recovery=False,
            read_capacity=5,
            removal_policy=self.removal_policy,
            write_capacity=5,
        )
