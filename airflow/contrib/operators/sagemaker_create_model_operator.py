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


class SageMakerCreateModelOperator(BaseOperator):

    """
       Create a SageMaker model

       This operator returns The ARN of the model created in Amazon SageMaker

       :param model_request:
       The configuration necessary to create a model (templated)
       :type model_request: dict
       :param region_name: The AWS region_name
       :type region_name: str
       :param sagemaker_conn_id: The SageMaker connection ID to use.
       :type sagemaker_conn_id: str
       :param use_db_config: Whether or not to use db config
       associated with sagemaker_conn_id.
       If set to true, will automatically update the model request
       with what's in db, so the db config doesn't need to
       included everything, but what's there does replace the ones
       in the model_request, so be careful
       :type use_db_config: bool
       :param aws_conn_id: The AWS connection ID to use.
       :type aws_conn_id: str

       **Example**:
           The following operator would create a model when executed

            sagemaker_model =
               SageMakerCreateModelOperator(
                   task_id='sagemaker_model',
                   model_request=request,
                   region_name='us-west-2'
                   sagemaker_conn_id='sagemaker_customers_conn',
                   use_db_config=False,
                   aws_conn_id='aws_customers_conn'
               )
    """

    template_fields = ['model_request']
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 model_request=None,
                 region_name=None,
                 sagemaker_conn_id=None,
                 use_db_config=False,
                 *args, **kwargs):
        super(SageMakerCreateModelOperator, self).__init__(*args, **kwargs)

        self.sagemaker_conn_id = sagemaker_conn_id
        self.model_request = model_request
        self.use_db_config = use_db_config
        self.region_name = region_name

    def execute(self, context):
        sagemaker = SageMakerHook(
            sagemaker_conn_id=self.sagemaker_conn_id,
            use_db_config=self.use_db_config,
            region_name=self.region_name
        )

        self.log.info(
            "Creating SageMaker Model %s."
            % self.model_request['ModelName']
        )
        response = sagemaker.create_model(self.model_request)
        if not response['ResponseMetadata']['HTTPStatusCode'] \
           == 200:
            raise AirflowException(
                'Sagemaker model creation failed: %s' % response)
        else:
            return response
