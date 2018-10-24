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

import json

from airflow.contrib.hooks.sagemaker_hook import SageMakerHook, parse_dict_integers
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerEndpointOperator(BaseOperator):

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

    template_fields = ['config']
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 config,
                 aws_conn_id='sagemaker_default',
                 wait_for_completion=True,
                 check_interval=30,
                 max_ingestion_time=None,
                 operation='create',
                 *args, **kwargs):
        super(SageMakerEndpointOperator, self).__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.operation = operation.lower()

    def parse_config_integers(self):
        # Parse the integer fields of endpoint config to integers
        if 'EndpointConfig' in self.config:
            for variant in self.config['EndpointConfig']['ProductionVariants']:
                parse_dict_integers(variant, ['InitialInstanceCount'])

    def execute(self, context):
        sagemaker = SageMakerHook(aws_conn_id=self.aws_conn_id)

        self.log.info(
            'Evaluating the config and doing required s3_operations'
        )

        self.config = sagemaker.configure_s3_resources(self.config)
        self.parse_config_integers()

        self.log.info(
            'After evaluation the config is:\n {}'.format(
                json.dumps(self.config, sort_keys=True, indent=4, separators=(',', ': '))
            )
        )

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
            sagemaker.create_model(model_info)

        if endpoint_config_info:
            self.log.info(
                'Creating endpoint config %s.'
                % endpoint_config_info['EndpointConfigName']
            )
            sagemaker.create_endpoint_config(endpoint_config_info)

        if self.operation == 'create':
            sagemaker_operation = sagemaker.create_endpoint
            log_str = 'Creating'
        elif self.operation == 'update':
            sagemaker_operation = sagemaker.update_endpoint
            log_str = 'Updating'
        else:
            raise AirflowException(
                'Invalid value. '
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
                'EndpointConfig': sagemaker.describe_endpoint_config(
                    endpoint_info['EndpointConfigName']
                ),
                'Endpoint': sagemaker.describe_endpoint(
                    endpoint_info['EndpointName']
                )
            }
