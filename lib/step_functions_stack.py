# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import aws_cdk.core as cdk
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_lambda_event_sources as lambda_event_sources
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_sns as sns
import aws_cdk.aws_stepfunctions as stepfunctions
import aws_cdk.aws_stepfunctions_tasks as stepfunctions_tasks

from .configuration import (
    AVAILABILITY_ZONES, ROUTE_TABLES, S3_RAW_BUCKET, SUBNET_IDS,
    SHARED_SECURITY_GROUP_ID, VPC_ID, get_logical_id_prefix,
    get_mappings, get_resource_name_prefix, get_ssm_parameter
)


class StepFunctionsStack(cdk.Stack):
    def __init__(
        self, app: cdk.App, id: str, target_environment: str,
        raw_to_conformed_job: glue.CfnJob, conformed_to_purpose_built_job: glue.CfnJob,
        **kwargs
    ) -> None:
        super().__init__(app, id, **kwargs)

        self.mappings = get_mappings()[target_environment]
        logical_id_prefix = get_logical_id_prefix()
        resource_name_prefix = get_resource_name_prefix()

        vpc_id = get_ssm_parameter(self.mappings[VPC_ID])
        shared_security_group_parameter = get_ssm_parameter(self.mappings[SHARED_SECURITY_GROUP_ID])
        availability_zones_parameter = get_ssm_parameter(self.mappings[AVAILABILITY_ZONES])
        availability_zones = availability_zones_parameter.split(',')
        subnet_ids_parameter = get_ssm_parameter(self.mappings[SUBNET_IDS])
        subnet_ids = subnet_ids_parameter.split(',')
        route_tables_parameter = get_ssm_parameter(self.mappings[ROUTE_TABLES])
        route_tables = route_tables_parameter.split(',')
        # Manually construct the VPC because it lives in the target account, not the Deployment Util account where the synth is ran
        vpc = ec2.Vpc.from_vpc_attributes(self, f'ImportedVpc',
            vpc_id=vpc_id,
            availability_zones=availability_zones,
            private_subnet_ids=subnet_ids,
            private_subnet_route_table_ids=route_tables,
        )
        shared_security_group = ec2.SecurityGroup.from_security_group_id(self, f'ImportedSecurityGroup',
            shared_security_group_parameter
        )
        raw_bucket_name = get_ssm_parameter(self.mappings[S3_RAW_BUCKET])
        raw_bucket = s3.Bucket.from_bucket_name(self, id='ImportedRawBucket', bucket_name=raw_bucket_name)

        notification_topic = sns.Topic(self, f'{target_environment}{logical_id_prefix}EtlFailedTopic')

        fail_state = stepfunctions.Fail(self, f'{target_environment}{logical_id_prefix}EtlFailedState',
            cause='Invalid response.',
            error='Error'
        )
        success_state = stepfunctions.Succeed(self, f'{target_environment}{logical_id_prefix}EtlSucceededState')

        error_task = stepfunctions_tasks.SnsPublish(self, f'{target_environment}{logical_id_prefix}EtlErrorPublishTask',
            topic=notification_topic,
            subject='Job Completed',
            message=stepfunctions.TaskInput.from_data_at('$')
        )
        error_task.next(fail_state)

        success_task = stepfunctions_tasks.SnsPublish(self, f'{target_environment}{logical_id_prefix}EtlErrorPublishTask',
            topic=notification_topic,
            subject='Job Failed',
            message=stepfunctions.TaskInput.from_data_at('$')
        )
        success_task.next(success_state)

        glue_raw_task = stepfunctions_tasks.GlueStartJobRun(self, f'{target_environment}{logical_id_prefix}GlueRawJobTask',
            glue_job_name=raw_to_conformed_job.name,
            comment='Stage to Raw data load',
        )
        glue_raw_task.add_catch(error_task, result_path='$.taskresult')

        glue_conformed_task = stepfunctions_tasks.GlueStartJobRun(self, f'{target_environment}{logical_id_prefix}GlueConformedJobTask',
            glue_job_name=conformed_to_purpose_built_job.name,
            comment='Raw to Conformed data load',
        )
        glue_conformed_task.add_catch(error_task, result_path='$.taskresult')

        machine_definition = glue_raw_task.next(
            glue_conformed_task.next(success_task)
        )

        machine = stepfunctions.StateMachine(self, f'{target_environment}{logical_id_prefix}EtlStateMachine',
            state_machine_name=f'{target_environment.lower()}-{resource_name_prefix}-etl',
            definition=machine_definition,
        )

        trigger_function = _lambda.Function(self, id, f'{target_environment}{logical_id_prefix}EtlTrigger',
            function_name=f'{target_environment.lower()}-{resource_name_prefix}-etl-trigger',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='lambda_handler.lambda_handler',
            code=_lambda.Code.from_asset(f'{os.path.dirname(__file__)}/trigger_lambda_scripts'),
            environment={
                'SFN_STATE_MACHINE_ARN': machine.state_machine_arn,
            },
            security_groups=[shared_security_group],
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE),
        )
        trigger_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'dynamodb:PutItem',
                    'dynamodb:DescribeTable',
                    'dynamodb:GetItem',
                    'dynamodb:Scan',
                    'dynamodb:Query',
                    'dynamodb:UpdateItem',
                    'dynamodb:DescribeTimeToLive',
                    'dynamodb:GetRecords',
                ],
                resources=['*'],
            )
        )
        trigger_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=['dynamodb:ListTables'],
                resources=['*'],
            )
        )
        trigger_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'states:StartExecution',
                ],
                resources=[machine.state_machine_arn],
            )
        )
        trigger_function.add_event_source(lambda_event_sources.S3EventSource(
            bucket=raw_bucket,
            events=[
                s3.EventType.OBJECT_CREATED,
            ]
        ))
