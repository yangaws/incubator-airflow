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


class SageMakerTuningOperator(BaseOperator):

    """
    Initiate a SageMaker hyper-parameter tuning job
    This operator returns The ARN of the tuning job created in Amazon SageMaker

    :param config: The configuration necessary to start a tuning job (templated)
    :type config: dict
    :param aws_conn_id: The AWS connection ID to use.
    :type aws_conn_id: str
    :param wait_for_completion: if the operator should block until tuning job finishes
    :type wait_for_completion: bool
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the tuning job
    :type check_interval: int
    :param max_ingestion_time: if wait is set to be true, the operator will fail
        if the tuning job hasn't finish within the max_ingestion_time in seconds
        (Caution: be careful to set this parameters because tuning can take very long)
    :type max_ingestion_time: int
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
                 *args, **kwargs):
        super(SageMakerTuningOperator, self)\
            .__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

    def parse_config_integers(self):
        # Parse the integer fields of tuning config to integers
        parse_dict_integers(self.config, ['HyperParameterTuningJobConfig', 'ResourceLimits',
                                          'MaxNumberOfTrainingJobs'])
        parse_dict_integers(self.config, ['HyperParameterTuningJobConfig', 'ResourceLimits',
                                          'MaxParallelTrainingJobs'])
        parse_dict_integers(self.config, ['TrainingJobDefinition', 'ResourceConfig',
                                          'InstanceCount'])
        parse_dict_integers(self.config, ['TrainingJobDefinition', 'ResourceConfig',
                                          'VolumeSizeInGB'])
        parse_dict_integers(self.config, ['TrainingJobDefinition', 'StoppingCondition',
                                          'MaxRuntimeInSeconds'], False)

    def execute(self, context):
        sagemaker = SageMakerHook(aws_conn_id=self.aws_conn_id)
        self.log.info(
            'Evaluating the config and doing required s3_operations'
        )

        self.config = sagemaker.configure_s3_resources(self.config)
        self.parse_config_integers()
        self.config['TrainingJobDefinition']['RoleArn'] = \
            sagemaker.expand_role(self.config['TrainingJobDefinition']['RoleArn'])

        self.log.info(
            'After evaluation the config is:\n {}'.format(
                json.dumps(self.config, sort_keys=True, indent=4, separators=(',', ': '))
            )
        )

        self.log.info(
            'Creating SageMaker Hyper Parameter Tunning Job %s'
            % self.config['HyperParameterTuningJobName']
        )

        response = sagemaker.create_tuning_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(
                'Sagemaker Tuning Job creation failed: %s' % response)
        else:
            return {
                'Tuning': sagemaker.describe_tuning_job(
                    self.config['HyperParameterTuningJobName']
                )
            }
