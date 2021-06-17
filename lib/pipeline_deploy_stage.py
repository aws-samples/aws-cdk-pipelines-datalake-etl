# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    def __init__(self, scope: core.Construct, id: str, target_environment: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        logical_id_prefix = get_logical_id_prefix()

        self.dynamodb_stack = DynamoDbStack(self, f'{target_environment}{logical_id_prefix}EtlDynamoDb',
            target_environment=target_environment,
            **kwargs,
        )

        self.glue_stack = GlueStack(self, f'{target_environment}{logical_id_prefix}EtlGlue',
            target_environment=target_environment,
            transformation_rules_table=self.dynamodb_stack.transformation_rules_table,
            **kwargs,
        )

        self.step_function_stack = StepFunctionsStack(self, f'{target_environment}{logical_id_prefix}EtlStepFunctions',
            target_environment=target_environment,
            raw_to_conformed_job=self.glue_stack.raw_to_conformed_job,
            conformed_to_purpose_built_job=self.glue_stack.conformed_to_purpose_built_job,
            **kwargs,
        )

        tag(self.step_function_stack, target_environment)
        tag(self.dynamodb_stack, target_environment)
        tag(self.glue_stack, target_environment)
