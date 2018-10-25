# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.contrib.operators.sagemaker_base_operator import SageMakerBaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerEndpointOperator(SageMakerBaseOperator):

    """
    Create a SageMaker endpoint
    This operator returns The ARN of the endpoint created in Amazon SageMaker

    :param config:
        The configuration necessary to create an endpoint
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    :param wait_for_completion: if the operator should block
        until training job finishes
    :type wait_for_completion: bool
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the training job
    :type check_interval: int
    :param max_ingestion_time: if wait is set to be true, the operator will fail
        if the training job hasn't finish within the max_ingestion_time in seconds
        (Caution: be careful to set this parameters because training can take very long)
        Setting it to None implies no timeout.
    :type max_ingestion_time: int
    :param operation: Whether to create an endpoint or update an endpoint. Must be
        one of 'create' and 'update'.
    :type operation: str
    """

    @apply_defaults
    def __init__(self,
                 config,
                 aws_conn_id='aws_default',
                 wait_for_completion=True,
                 check_interval=30,
                 max_ingestion_time=None,
                 operation='create',
                 *args, **kwargs):
        super(SageMakerEndpointOperator, self).__init__(config=config,
                                                        aws_conn_id=aws_conn_id,
                                                        *args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.operation = operation.lower()
        self.create_integer_fields()

    def create_integer_fields(self):
        if 'EndpointConfig' in self.config:
            self.integer_fields = [
                ['EndpointConfig', 'ProductionVariants', 'InitialInstanceCount']
            ]

    def expand_role(self):
        if 'Model' not in self.config:
            return
        config = self.config['Model']
        if 'ExecutionRoleArn' in config:
            config['ExecutionRoleArn'] = \
                self.hook.expand_role(config['ExecutionRoleArn'])

    def execute(self, context):
        self.preprocess_config()

        model_info = self.config['Model']\
            if 'Model' in self.config else None
        endpoint_config_info = self.config['EndpointConfig']\
            if 'EndpointConfig' in self.config else None
        endpoint_info = self.config['Endpoint']\
            if 'Endpoint' in self.config else self.config

        if model_info:
            self.log.info(
                'Creating SageMaker model %s.'
                % model_info['ModelName']
            )
            self.hook.create_model(model_info)

        if endpoint_config_info:
            self.log.info(
                'Creating endpoint config %s.'
                % endpoint_config_info['EndpointConfigName']
            )
            self.hook.create_endpoint_config(endpoint_config_info)

        if self.operation == 'create':
            sagemaker_operation = self.hook.create_endpoint
            log_str = 'Creating'
        elif self.operation == 'update':
            sagemaker_operation = self.hook.update_endpoint
            log_str = 'Updating'
        else:
            raise AirflowException(
                'Invalid value! '
                'Argument operation has to be one of "create" and "update"')

        self.log.info(
            '{} SageMaker endpoint {}.'.format(log_str, endpoint_info['EndpointName'])
        )

        response = sagemaker_operation(
            endpoint_info,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(
                'Sagemaker endpoint creation failed: %s' % response)
        else:
            return {
                'EndpointConfig': self.hook.describe_endpoint_config(
                    endpoint_info['EndpointConfigName']
                ),
                'Endpoint': self.hook.describe_endpoint(
                    endpoint_info['EndpointName']
                )
            }
