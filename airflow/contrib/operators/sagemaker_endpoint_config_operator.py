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


class SageMakerEndpointConfigOperator(BaseOperator):

    """
    Create a SageMaker endpoint config
    This operator returns The ARN of the endpoint config created in Amazon SageMaker

    :param config: The configuration necessary to create an endpoint config
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    """

    template_fields = ['config']
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 config,
                 aws_conn_id='sagemaker_default',
                 *args, **kwargs):
        super(SageMakerEndpointConfigOperator, self).__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config

    def parse_config_integers(self):
        # Parse the integer fields of endpoint config to integers
        for variant in self.config['ProductionVariants']:
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

        self.log.info(
            'Creating SageMaker Endpoint Config %s.'
            % self.config['EndpointConfigName']
        )
        response = sagemaker.create_endpoint_config(self.config)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(
                'Sagemaker endpoint config creation failed: %s' % response)
        else:
            return {
                'EndpointConfig': sagemaker.describe_endpoint_config(
                    self.config['EndpointConfigName']
                )
            }
