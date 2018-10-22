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


class SageMakerEndpointOperator(BaseOperator):

    """
       Create a SageMaker endpoint
       This operator returns The ARN of the endpoint created in Amazon SageMaker
       :param config:
       The configuration necessary to create an endpoint
       :type config: dict
       :param region_name: The AWS region_name
       :type region_name: str
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
       **Example**:
           The following operator would create an endpoint when executed
            sagemaker_endpoint =
               SageMakerEndpointOperator(
                   task_id='sagemaker_endpoint',
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
                 wait_for_completion=True,
                 check_interval=30,
                 max_ingestion_time=None,
                 operation='create',
                 *args, **kwargs):
        super(SageMakerEndpointOperator, self).__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config
        self.region_name = region_name
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.operation = operation.lower()

    def execute(self, context):
        sagemaker = SageMakerHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name
        )

        self.log.info(
            'Evaluating the config and doing required s3_operations'
        )

        self.config = sagemaker.configure_s3_resources(self.config)

        self.log.info(
            'After evaluation the config is:\n {}'.format(self.config)
        )

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
            '{} SageMaker endpoint {}.'.format(log_str, self.config['EndpointName'])
        )

        response = sagemaker_operation(
            self.config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time
        )
        if not response['ResponseMetadata']['HTTPStatusCode'] \
           == 200:
            raise AirflowException(
                'Sagemaker endpoint creation failed: %s' % response)
        else:
            return {
                'Endpoint': sagemaker.describe_endpoint(
                    self.config['EndpointName']
                )
            }
