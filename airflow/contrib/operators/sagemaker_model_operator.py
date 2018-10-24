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

from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerModelOperator(BaseOperator):

    """
    Create a SageMaker model
    This operator returns The ARN of the model created in Amazon SageMaker

    :param config: The configuration necessary to create a model
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
        super(SageMakerModelOperator, self).__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config

    def execute(self, context):
        sagemaker = SageMakerHook(aws_conn_id=self.aws_conn_id)

        self.log.info(
            'Evaluating the config and doing required s3_operation'
        )

        self.config = sagemaker.configure_s3_resources(self.config)
        self.config['ExecutionRoleArn'] = \
            sagemaker.expand_role(self.config['ExecutionRoleArn'])

        self.log.info(
            'After evaluation the config is:\n {}'.format(
                json.dumps(self.config, sort_keys=True, indent=4, separators=(',', ': '))
            )
        )

        self.log.info(
            'Creating SageMaker Model %s.'
            % self.config['ModelName']
        )
        response = sagemaker.create_model(self.config)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(
                'Sagemaker model creation failed: %s' % response)
        else:
            return {
                'Model': sagemaker.describe_model(
                    self.config['ModelName']
                )
            }
