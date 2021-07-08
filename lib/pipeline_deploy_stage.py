# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_cdk import core

from .step_functions_stack import StepFunctionsStack
from .glue_stack import GlueStack
from .dynamodb_stack import DynamoDbStack
from .tagging import tag
from .configuration import (
    get_logical_id_prefix,
)


class PipelineDeployStage(core.Stage):
    def __init__(self, scope: core.Construct, construct_id: str, target_environment: str, **kwargs):
        """
        Adds deploy stage to CodePipeline

        @param scope cdk.Construct: Parent of this stack, usually an App or a Stage, but could be any construct.
        @param construct_id str:
            The construct ID of this stack. If stackName is not explicitly defined,
            this id (and any parent IDs) will be used to determine the physical ID of the stack.
        @param target_environment str: The target environment for stacks in the deploy stage
        @param kwargs:
        """
        super().__init__(scope, construct_id, **kwargs)
        logical_id_prefix = get_logical_id_prefix()

        dynamodb_stack = DynamoDbStack(
            self,
            f'{target_environment}{logical_id_prefix}EtlDynamoDb',
            target_environment=target_environment,
            **kwargs,
        )

        glue_stack = GlueStack(
            self,
            f'{target_environment}{logical_id_prefix}EtlGlue',
            target_environment=target_environment,
            **kwargs,
        )

        step_function_stack = StepFunctionsStack(
            self,
            f'{target_environment}{logical_id_prefix}EtlStepFunctions',
            target_environment=target_environment,
            raw_to_conformed_job=glue_stack.raw_to_conformed_job,
            conformed_to_purpose_built_job=glue_stack.conformed_to_purpose_built_job,
            job_audit_table=dynamodb_stack.job_audit_table,
            **kwargs,
        )

        tag(step_function_stack, target_environment)
        tag(dynamodb_stack, target_environment)
        tag(glue_stack, target_environment)
