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


class SageMakerTransformOperator(BaseOperator):
    """
       Initiate a SageMaker transform

       This operator returns The ARN of the model created in Amazon SageMaker

       :param config:
       The configuration necessary to start a transform job (templated)
       :type config: dict
       :param model_config:
       The configuration necessary to create a SageMaker model, the default is none
       which means the SageMaker model used for the SageMaker transform job already exists.
       If given, it will be used to create a SageMaker model before creating
       the SageMaker transform job
       :type model_config: dict
       :param region_name: The AWS region_name
       :type region_name: string
       :param aws_conn_id: The AWS connection ID to use.
       :type aws_conn_id: string
       :param wait_for_completion: if the program should keep running until job finishes
       :type wait_for_completion: bool
       :param check_interval: if wait is set to be true, this is the time interval
       in seconds which the operator will check the status of the transform job
       :type check_interval: int
       :param max_ingestion_time: if wait is set to be true, the operator will fail
       if the transform job hasn't finish within the max_ingestion_time in seconds
       (Caution: be careful to set this parameters because transform can take very long)
       :type max_ingestion_time: int

       **Example**:
           The following operator would start a transform job when executed

            sagemaker_transform =
               SageMakerTransformOperator(
                   task_id='sagemaker_transform',
                   config=transform_config,
                   model_config=model_config,
                   region_name='us-west-2'
                   aws_conn_id='aws_customers_conn'
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
        super(SageMakerTransformOperator, self).__init__(*args, **kwargs)

        self.aws_conn_id = aws_conn_id
        self.config = config
        self.region_name = region_name
        self.wait_for_completion = wait_for_completion
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time

    def execute(self, context):
        sagemaker = SageMakerHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name
        )

        self.log.info(
            "Evaluating the config"
        )

        self.config = sagemaker.evaluate_and_configure_s3(self.config)

        self.log.info(
            "After evaluation the config is:\n {}".format(self.config)
        )

        model_config = self.config["Model"]\
            if "Model" in self.config else None
        transform_config = self.config["Transform"]\
            if "Transform" in self.config else self.config

        if model_config:
            self.log.info(
                "Creating SageMaker Model %s for transform job"
                % model_config['ModelName']
            )
            sagemaker.create_model(model_config)

        self.log.info(
            "Creating SageMaker transform Job %s."
            % transform_config['TransformJobName']
        )
        response = sagemaker.create_transform_job(
            transform_config,
            wait_for_completion=self.wait_for_completion,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time)
        if not response['ResponseMetadata']['HTTPStatusCode'] \
           == 200:
            raise AirflowException(
                'Sagemaker transform Job creation failed: %s' % response)
        else:
            return {
                'config': self.config,
                'model_information': sagemaker.describe_model(
                    transform_config['ModelName']
                ),
                'transform_job_information': sagemaker.describe_transform_job(
                    transform_config['TransformJobName']
                )}
