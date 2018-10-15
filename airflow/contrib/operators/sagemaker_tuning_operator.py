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


class SageMakerTuningOperator(BaseOperator):

    """
       Initiate a SageMaker hyper-parameter tuning job

       This operator returns The ARN of the tuning job created in Amazon SageMaker

       :param config:
       The configuration necessary to start a tuning job (templated)
       :type config: dict
       :param region_name: The AWS region_name
       :type region_name: str
       :param aws_conn_id: The AWS connection ID to use.
       :type aws_conn_id: str
       :param wait_for_completion: if the operator should block
       until tuning job finishes
       :type wait_for_completion: bool
       :param check_interval: if wait is set to be true, this is the time interval
       in seconds which the operator will check the status of the tuning job
       :type check_interval: int
       :param max_ingestion_time: if wait is set to be true, the operator will fail
       if the tuning job hasn't finish within the max_ingestion_time in seconds
       (Caution: be careful to set this parameters because tuning can take very long)
       :type max_ingestion_time: int

       **Example**:
           The following operator would start a tuning job when executed

            sagemaker_tuning =
               SageMakerTuningOperator(
                   task_id='sagemaker_tuning',
                   config=config,
                   region='us-west-2',
                   check_interval=20,
                   max_ingestion_time=3600,
                   aws_conn_id='aws_customers_conn',
               )
       """

    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 config,
                 region_name=None,
                 aws_conn_id='sagemaker_default',
                 wait_for_completion=True,
                 check_interval=30,
                 max_ingestion_time=None,
                 *args, **kwargs):
        super(SageMakerTuningOperator, self)\
            .__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

    def execute(self, context):
        sagemaker = SageMakerHook(aws_conn_id=self.aws_conn_id,
                                  region_name=self.region_name)

        self.log.info(
            "Evaluating the config and doing required s3_operations"
        )

        self.config = sagemaker.evaluate_and_configure_s3(self.config)

        self.log.info(
            "After evaluation the config is:\n {}".format(self.config)
        )

        self.log.info(
            "Creating SageMaker Hyper Parameter Tunning Job %s"
            % self.config['HyperParameterTuningJobName']
        )

        response = sagemaker.create_tuning_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time
        )
        if not response['ResponseMetadata']['HTTPStatusCode'] \
           == 200:
            raise AirflowException(
                "Sagemaker Tuning Job creation failed: %s" % response)
        else:
            return {
                'config': self.config,
                'information': sagemaker.describe_tuning_job(
                    self.config['HyperParameterTuningJobName']
                )}
