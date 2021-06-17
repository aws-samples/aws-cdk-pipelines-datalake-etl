# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk.core as cdk
import aws_cdk.pipelines as pipelines
import aws_cdk.aws_codepipeline as codepipeline
import aws_cdk.aws_codepipeline_actions as codepipeline_actions
import aws_cdk.pipelines as pipelines
import aws_cdk.aws_iam as iam

from .configuration import (
    CROSS_ACCOUNT_DYNAMO_ROLE, DEPLOYMENT, GITHUB_TOKEN,
    get_logical_id_prefix, get_mappings, get_repository_name,
    get_repository_owner, get_resource_name_prefix, get_ssm_parameter,
)
from .pipeline_deploy_stage import PipelineDeployStage


class PipelineStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, id: str, target_environment: str, target_branch: str, target_aws_env: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.create_environment_pipeline(
            target_environment,
            target_branch,
            target_aws_env
        )
        
    def create_environment_pipeline(self, target_environment, target_branch, target_aws_env):
        mappings = get_mappings()
        source_artifact = codepipeline.Artifact()
        cloud_assembly_artifact = codepipeline.Artifact()

        logical_id_prefix = get_logical_id_prefix()
        resource_name_prefix = get_resource_name_prefix()

        pipeline = pipelines.CdkPipeline(self, f'{target_environment}{logical_id_prefix}DataLakeEtlPipeline',
            pipeline_name=f'{target_environment.lower()}-{resource_name_prefix}-datalake-etl-pipeline',
            cloud_assembly_artifact=cloud_assembly_artifact,
            source_action=codepipeline_actions.GitHubSourceAction(
                action_name='GitHub',
                branch=target_branch,
                output=source_artifact,
                oauth_token=cdk.SecretValue.secrets_manager(mappings[DEPLOYMENT][GITHUB_TOKEN]),
                trigger=codepipeline_actions.GitHubTrigger.POLL,
                owner=get_repository_owner(),
                repo=get_repository_name(),
            ),
            synth_action=pipelines.SimpleSynthAction.standard_npm_synth(
                source_artifact=source_artifact,
                cloud_assembly_artifact=cloud_assembly_artifact,
                install_command='npm install -g aws-cdk && pip3 install -r requirements.txt',
                # TODO: Automate unit testing
                # build_command='pytest unit_tests',
                role_policy_statements=[
                    iam.PolicyStatement(
                        sid='EtlParameterStorePolicy',
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'ssm:*',
                        ],
                        resources=[
                            f'arn:aws:ssm:{self.region}:{self.account}:parameter/DataLake/*',
                        ],
                    ),
                    iam.PolicyStatement(
                        sid='EtlSecretsManagerPolicy',
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'secretsmanager:*',
                        ],
                        resources=[
                            f'arn:aws:secretsmanager:{self.region}:{self.account}:secret:/DataLake/*',
                        ],
                    ),
                    iam.PolicyStatement(
                        sid='EtlKmsPolicy',
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'kms:*',
                        ],
                        resources=[
                            '*',
                        ],
                    ),
                    iam.PolicyStatement(
                        sid='EtlVpcPolicy',
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'vpc:*',
                        ],
                        resources=[
                            '*',
                        ],
                    ),
                    iam.PolicyStatement(
                        sid='EtlEc2Policy',
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'ec2:*',
                        ],
                        resources=[
                            '*',
                        ],
                    ),
                ],
                synth_command='cdk synth --verbose',
            ),
            cross_account_keys=True,
        )

        deploy_stage = PipelineDeployStage(self, target_environment,
            target_environment=target_environment,
            env=target_aws_env,
        )
        app_stage = pipeline.add_application_stage(deploy_stage)

        cross_account_role = get_ssm_parameter(mappings[target_environment][CROSS_ACCOUNT_DYNAMO_ROLE])
        # Dynamically manage extract list
        app_stage.add_actions(pipelines.ShellScriptAction(
            action_name='UploadTransformationRules',
            additional_artifacts=[source_artifact],
            run_order=app_stage.next_sequential_run_order(),
            commands=[
                'pip3 install boto3',
                f'python3 ./lib/dynamodb_config/datalake_blog_conformed_sync.py {target_environment} {cross_account_role}',
            ],
            role_policy_statements=[
                iam.PolicyStatement(
                    sid='AssumeRolePolicy',
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'sts:AssumeRole',
                    ],
                    resources=[
                        cross_account_role,
                    ],
                )
            ]
        ))
