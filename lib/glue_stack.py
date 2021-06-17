# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk.core as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
import aws_cdk.aws_s3_deployment as s3_deployment

from .configuration import (
    ACCOUNT_ID, GLUE_CONNECTION_AVAILABILITY_ZONE, GLUE_CONNECTION_SECRET, GLUE_CONNECTION_SUBNET, REGION, 
    S3_ACCESS_LOG_BUCKET, S3_KMS_KEY, S3_CONFORMED_BUCKET, S3_RAW_BUCKET, SHARED_SECURITY_GROUP_ID, get_mappings,
    get_logical_id_prefix, get_resource_name_prefix, get_ssm_parameter
)


class GlueStack(cdk.Stack):
    def __init__(self, app: cdk.App, id: str,
        target_environment: str, transformation_rules_table: dynamodb.Table, **kwargs
    ) -> None:
        super().__init__(app, id, **kwargs)

        self.mappings = get_mappings()[target_environment]
        logical_id_prefix = get_logical_id_prefix()
        resource_name_prefix = get_resource_name_prefix()

        account_id = get_ssm_parameter(self.mappings[ACCOUNT_ID])
        region = get_ssm_parameter(self.mappings[REGION])
        existing_access_logs_bucket_name = get_ssm_parameter(self.mappings[S3_ACCESS_LOG_BUCKET])
        access_logs_bucket = s3.Bucket.from_bucket_attributes(self, 'ImportedBucket',
            bucket_name=existing_access_logs_bucket_name
        )
        s3_kms_key_parameter = get_ssm_parameter(self.mappings[S3_KMS_KEY])
        s3_kms_key = kms.Key.from_key_arn(self, 'ImportedKmsKey', s3_kms_key_parameter)
        shared_security_group_parameter = get_ssm_parameter(self.mappings[SHARED_SECURITY_GROUP_ID])
        glue_connection_subnet = get_ssm_parameter(self.mappings[GLUE_CONNECTION_SUBNET])
        glue_connection_availability_zone = get_ssm_parameter(self.mappings[GLUE_CONNECTION_AVAILABILITY_ZONE])

        raw_bucket_name = get_ssm_parameter(self.mappings[S3_RAW_BUCKET])
        raw_bucket = s3.Bucket.from_bucket_name(self, id='ImportedRawBucket', bucket_name=raw_bucket_name)
        conformed_bucket_name = get_ssm_parameter(self.mappings[S3_CONFORMED_BUCKET])
        conformed_bucket = s3.Bucket.from_bucket_name(self, id='ImportedRawBucket', bucket_name=conformed_bucket_name)

        # Resources shared across the workflow
        shared_security_group = ec2.SecurityGroup.from_security_group_id(self, f'ImportedSecurityGroup',
            shared_security_group_parameter
        )
        subnet = ec2.Subnet.from_subnet_attributes(self, 'ImportedSubnet',
            subnet_id=glue_connection_subnet,
            availability_zone=glue_connection_availability_zone
        )
        glue_scripts_bucket = self.glue_scripts_bucket(s3_kms_key, access_logs_bucket, account_id)
        glue_scripts_temp_bucket = self.glue_scripts_temporary_bucket(s3_kms_key, access_logs_bucket, account_id)
        workflow_role = self.get_role(account_id, region, s3_kms_key)

        job_connection = glue.Connection(self, f'DataLakeRawToConformedWorkflowConnection',
            type=glue.ConnectionType.NETWORK,
            connection_name=f'{target_environment.lower()}-{resource_name_prefix}-raw-to-conformed-connection',
            security_groups=[shared_security_group],
            subnet=subnet
        )

        self.raw_to_conformed_job = glue.CfnJob(self, f'{target_environment}{logical_id_prefix}RawToConformedJob',
            name=f'{target_environment.lower()}-{resource_name_prefix}-raw-to-conformed-job',
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{glue_scripts_bucket.bucket_name}/etl/datalake_conformed_load.py'
            ),
            connections=glue.CfnJob.glue.ConnectionsListProperty(
                connections=[job_connection.connection_name],
            ),
            default_arguments={
                '--target_database_name': 'datablog_arg',
                '--target_bucket': conformed_bucket.bucket_name,
                '--target_table_name': 'datablog_nyc_raw',
                '--transformation_rules_table': transformation_rules_table.table_name,
                '--extra-py-files': f's3://{glue_scripts_bucket.bucket_name}/etl/utils.py',
                '--TempDir': f's3://{glue_scripts_temp_bucket.bucket_name}/etl/raw-to-conformed'
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            glue_version='2.0',
            max_retries=0,
            number_of_workers=1,
            role=workflow_role.role_arn,
            worker_type='G.1X',
        )

        self.conformed_to_purpose_built_job = glue.CfnJob(self, f'{target_environment}{logical_id_prefix}ConformedToPurposeBuiltJob',
            name=f'{target_environment.lower()}-{resource_name_prefix}-conformed-to-purpose-built-job',
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{glue_scripts_bucket.bucket_name}/etl/datalake_conformed_load.py'
            ),
            connections=glue.CfnJob.glue.ConnectionsListProperty(
                connections=[job_connection.connection_name],
            ),
            default_arguments={
                '--target_database_name': 'datablog_arg',
                '--target_bucket': conformed_bucket.bucket_name,
                '--target_table_name': 'datablog_nyc_raw',
                '--transformation_rules_table': transformation_rules_table.table_name,
                '--extra-py-files': f's3://{glue_scripts_bucket.bucket_name}/etl/utils.py',
                '--TempDir': f's3://{glue_scripts_temp_bucket.bucket_name}/etl/conformed-to-purpose-built'
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            ),
            glue_version='2.0',
            max_retries=0,
            number_of_workers=1,
            role=workflow_role.role_arn,
            worker_type='G.1X',
        )

    def glue_scripts_bucket(self, s3_kms_key, access_logs_bucket, account_id) -> s3.Bucket:
        bucket = s3.Bucket(self, 'DataLakeRawGlueScriptsBucket',
            bucket_name=f'datalake-{account_id}-to-raw-glue-scripts',
            access_control=s3.BucketAccessControl.PRIVATE,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            bucket_key_enabled=s3_kms_key != None,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=s3_kms_key,
            public_read_access=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix=f'datalake-{account_id}-glue-scripts',
        )
        # Dynamically upload resources to the script target 
        s3_deployment.BucketDeployment(self, 'DeployGlueJobScript',
            # This path is relative to the root of the project
            sources=[s3_deployment.Source.asset('./const_datalake_rdbms_data_ingestion_to_raw_etl/glue_scripts')],
            destination_bucket=bucket,
            destination_key_prefix='etl',
        )

        return bucket

    def glue_scripts_temporary_bucket(self, s3_kms_key, access_logs_bucket, account_id) -> s3.Bucket:
        bucket = s3.Bucket(self, 'DataLakeRawGlueScriptsTemporaryBucket',
            bucket_name=f'datalake-{account_id}-to-raw-glue-temporary-scripts',
            access_control=s3.BucketAccessControl.PRIVATE,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            bucket_key_enabled=s3_kms_key != None,
            encryption=s3.BucketEncryption.KMS if s3_kms_key else s3.BucketEncryption.S3_MANAGED,
            encryption_key=s3_kms_key if s3_kms_key else None,
            public_read_access=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix=f'datalake-{account_id}-to-raw-glue-temporary-scripts',
        )

        return bucket

    def get_role(self, account_id, region, s3_kms_key) -> iam.Role:
        return iam.Role(self, 'DataLakeRawCrawleriam.Role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            inline_policies=[
                iam.PolicyDocument(statements=[
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
                        resources=[
                            f'arn:aws:dynamodb:{region}:{account_id}:table/datalake*'
                        ]
                    )
                ]),
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'dynamodb:ListTables',
                        ],
                        resources=[
                            '*'
                        ]
                    )
                ]),
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            's3:ListBucketVersions',
                            's3:ListBucket',
                            's3:GetBucketNotification',
                            's3:GetBucketLocation',
                        ],
                        resources=[
                            'arn:aws:s3:::*'
                        ]
                    )
                ]),
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            's3:ReplicationObject',
                            's3:PutObject',
                            's3:GetObject',
                            's3:DeleteObject',
                        ],
                        resources=[
                            'arn:aws:s3:::*/*'
                        ]
                    )
                ]),
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            's3:ListAllMyBuckets',
                        ],
                        resources=[
                            '*'
                        ]
                    )
                ]),
                # NOTE: This is required due to bucket level encryption on S3 Buckets
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'kms:*',
                        ],
                        resources=[
                            s3_kms_key.key_arn,
                        ]
                    )
                ]),
            ],
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceiam.Role'),
            ]
        )
