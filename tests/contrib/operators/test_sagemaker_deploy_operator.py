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

import unittest
try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from airflow import configuration
from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.contrib.operators.sagemaker_deploy_operator \
    import SageMakerDeployOperator
from airflow.exceptions import AirflowException

role = 'test-role'
bucket = 'test-bucket'
image = 'test-image'
output_url = 's3://{}/test/output'.format(bucket)
model_name = 'test-model-name'
config_name = 'test-endpoint-config-name'
endpoint_name = 'test-endpoint-name'

create_model_params = {
    'ModelName': model_name,
    'PrimaryContainer': {
        'Image': image,
        'ModelDataUrl': output_url,
    },
    'ExecutionRoleArn': role
}

create_endpoint_config_params = {
    'EndpointConfigName': config_name,
    'ProductionVariants': [
        {
            'VariantName': 'AllTraffic',
            'ModelName': model_name,
            'InitialInstanceCount': '1',
            'InstanceType': 'ml.c4.xlarge'
        }
    ]
}

create_endpoint_params = {
    'EndpointName': endpoint_name,
    'EndpointConfigName': config_name
}

config = {
    'Model': create_model_params,
    'EndpointConfig': create_endpoint_config_params,
    'Endpoint': create_endpoint_params
}


class TestSageMakerDeployOperator(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.sagemaker = SageMakerDeployOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=config,
            region_name='us-west-2',
            wait_for_completion=False,
            check_interval=5,
            operation='create'
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'describe_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'describe_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    @mock.patch.object(SageMakerHook, 'describe_endpoint')
    @mock.patch.object(SageMakerHook, '__init__')
    def test_hook_init(self, hook_init, mock_describe_endpoint, mock_endpoint,
                       mock_describe_endpoint_config, mock_endpoint_config,
                       mock_describe_model, mock_model, mock_client):
        mock_model.return_value = {'ModelArn': 'testarn',
                                   'ResponseMetadata':
                                   {'HTTPStatusCode': 200}}
        mock_endpoint_config.return_value = {'EndpointConfigArn': 'testarn',
                                             'ResponseMetadata':
                                             {'HTTPStatusCode': 200}}
        mock_endpoint.return_value = {'EndpointArn': 'testarn',
                                      'ResponseMetadata':
                                      {'HTTPStatusCode': 200}}
        mock_describe_model.return_value = {
            'ModelName': model_name
        }
        mock_describe_endpoint_config.return_value = {
            'EndpointConfigName': config_name
        }
        mock_describe_endpoint.return_value = {
            'EndpointName': endpoint_name
        }
        hook_init.return_value = None
        self.sagemaker.execute(None)
        hook_init.assert_called_once_with(
            aws_conn_id='sagemaker_test_id',
            region_name='us-west-2'
        )

    def test_evaluate(self):
        self.sagemaker.evaluate()
        for variant in self.sagemaker.config['EndpointConfig']['ProductionVariants']:
            self.assertEqual(variant['InitialInstanceCount'],
                             int(variant['InitialInstanceCount']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    def test_execute(self, mock_endpoint, mock_endpoint_config,
                     mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'testarn',
                                      'ResponseMetadata':
                                      {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(create_model_params)
        mock_endpoint_config.assert_called_once_with(create_endpoint_config_params)
        mock_endpoint.assert_called_once_with(create_endpoint_params,
                                              wait_for_completion=False,
                                              check_interval=5,
                                              max_ingestion_time=None
                                              )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    def test_execute_with_failure(self, mock_endpoint, mock_endpoint_config,
                                  mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'testarn',
                                      'ResponseMetadata':
                                      {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


if __name__ == '__main__':
    unittest.main()
