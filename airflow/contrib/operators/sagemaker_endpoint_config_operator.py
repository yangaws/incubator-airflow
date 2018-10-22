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

from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerEndpointConfigOperator(BaseOperator):

    """
       Create a SageMaker endpoint config
       This operator returns The ARN of the endpoint config created in Amazon SageMaker
       :param config:
       The configuration necessary to create an endpoint config
       :type config: dict
       :param region_name: The AWS region_name
       :type region_name: str
       :param aws_conn_id: The AWS connection ID to use.
       :type aws_conn_id: str
       **Example**:
           The following operator would create a endpoint config when executed
            sagemaker_endpoint_config =
               SageMakerEndpointConfigOperator(
                   task_id='sagemaker_endpoint_config',
                   config=request,
                   region_name='us-west-2'
                   aws_conn_id='aws_customers_conn'
               )
    """

    template_fields = ['config', 'region_name']
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 config,
                 region_name=None,
                 aws_conn_id='sagemaker_default',
                 *args, **kwargs):
        super(SageMakerEndpointConfigOperator, self).__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config
        self.region_name = region_name

    def evaluate(self):
        for variant in self.config['ProductionVariants']:
            variant['InitialInstanceCount'] = \
                int(variant['InitialInstanceCount'])

    def execute(self, context):
        sagemaker = SageMakerHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name
        )

        self.log.info(
            'Evaluating the config and doing required s3_operations'
        )

        self.config = sagemaker.configure_s3_resources(self.config)
        self.evaluate()

        self.log.info(
            'After evaluation the config is:\n {}'.format(self.config)
        )

        self.log.info(
            'Creating SageMaker Endpoint Config %s.'
            % self.config['EndpointConfigName']
        )
        response = sagemaker.create_endpoint_config(self.config)
        if not response['ResponseMetadata']['HTTPStatusCode'] \
           == 200:
            raise AirflowException(
                'Sagemaker endpoint config creation failed: %s' % response)
        else:
            return {
                'EndpointConfig': sagemaker.describe_endpoint_config(
                    self.config['EndpointConfigName']
                )
            }
