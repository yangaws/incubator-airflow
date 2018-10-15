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
import tarfile
import tempfile
import time
import os

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook


class SageMakerHook(AwsHook):
    """
    Interact with Amazon SageMaker.

    :param region_name: the AWS region name (example: us-east-1)
    :type region_name: str
    """
    non_terminal_states = {'InProgress', 'Stopping'}
    endpoint_non_terminal_states = {'Creating', 'Updating', 'SystemUpdating',
                                    'RollingBack', 'Deleting'}
    failed_states = {'Failed'}

    def __init__(self,
                 region_name=None,
                 *args, **kwargs):
        super(SageMakerHook, self).__init__(*args, **kwargs)
        self.region_name = region_name
        self.conn = self.get_conn()
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

    def tar_and_s3_upload(self, path, key, bucket):
        """
        Tar the local file or directory and upload to s3

        :param path: local file or directory
        :type path: str
        :param key: s3 key
        :type key: str
        :param bucket: s3 bucket
        :type bucket: str
        :return: None
        """
        with tempfile.TemporaryFile() as temp_file:
            if os.path.isdir(path):
                files = [os.path.join(path, name) for name in os.listdir(path)]
            else:
                files = [path]
            with tarfile.open(mode='w:gz', fileobj=temp_file) as tar_file:
                for f in files:
                    tar_file.add(f, arcname=os.path.basename(f))
            temp_file.seek(0)
            self.s3_hook.load_file_obj(temp_file, key, bucket, True)

    def evaluate(self, config):
        """
        Evaluate the config of SageMaker operation

        :param config: config of SageMaker operation
        :type config: dict
        :return: dict
        """
        if isinstance(config, list):
            return [self.evaluate(element) for element in config]
        if isinstance(config, dict):
            return {key: self.evaluate(config[key]) for key in config}
        if callable(config):
            return self.evaluate(config())
        else:
            return config

    def evaluate_and_configure_s3(self, config):
        """
        Evaluate the config of SageMaker operation and
        execute related s3 operations

        :param config: config of SageMaker operation
        :type config: dict
        :return: dict
        """
        s3_operations = config.pop('S3Operations', None)
        config = self.evaluate(config)
        if s3_operations is not None:
            create_bucket_ops = s3_operations['S3CreateBucket']
            upload_ops = s3_operations['S3Upload']
            if create_bucket_ops:
                for op in create_bucket_ops:
                    self.s3_hook.create_bucket(bucket_name=op['Bucket'],
                                               region_name=self.region_name)
            if upload_ops:
                for op in upload_ops:
                    if op['Tar']:
                        self.tar_and_s3_upload(op['Path'], op['Key'],
                                               op['Bucket'])
                    else:
                        self.s3_hook.load_file(op['Path'], op['Key'],
                                               op['Bucket'])

        return config

    def check_s3_url(self, s3url):
        """
        Check if a s3 url exists
        :param s3url: S3 url
        :type s3url:str
        :return: bool
        """
        bucket, key = S3Hook.parse_s3_url(s3url)
        if not self.s3_hook.check_for_bucket(bucket_name=bucket):
            raise AirflowException(
                "The input S3 Bucket {} does not exist ".format(bucket))
        if key and not self.s3_hook.check_for_key(key=key, bucket_name=bucket)\
           and not self.s3_hook.check_for_prefix(
                prefix=key, bucket_name=bucket, delimiter='/'):
            # check if s3 key exists in the case user provides a single file
            # or if s3 prefix exists in the case user provides multiple files in
            # a prefix
            raise AirflowException("The input S3 Key "
                                   "or Prefix {} does not exist in the Bucket {}"
                                   .format(s3url, bucket))
        return True

    def check_training_config(self, training_config):
        """
        Check if a training config is valid
        :param training_config: training_config
        :type training_config: dict
        :return: None
        """
        for channel in training_config['InputDataConfig']:
            self.check_s3_url(channel['DataSource']
                                     ['S3DataSource']['S3Uri'])

    def check_tuning_config(self, tuning_config):
        """
        Check if a tuning config is valid
        :param tuning_config: tuning_config
        :type tuning_config: dict
        :return: None
        """
        for channel in tuning_config['TrainingJobDefinition']['InputDataConfig']:
            self.check_s3_url(channel['DataSource']
                                     ['S3DataSource']['S3Uri'])

    def check_status(self, non_terminal_states,
                     failed_state, key,
                     describe_function, check_interval,
                     max_ingestion_time, *args):
        """
        Check status of a SageMaker job
        :param non_terminal_states: the set of non_terminal states
        :type non_terminal_states: set
        :param failed_state: the set of failed states
        :type failed_state: set
        :param key: the key of the response dict
        that points to the state
        :type key: str
        :param describe_function: the function used to retrieve the status
        :type describe_function: python callable
        :param args: the arguments for the function
        :param check_interval: the time interval in seconds which the operator
        will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
        SageMaker jobs that run longer than this will fail. Setting this to
        None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: None
        """
        sec = 0
        running = True

        while running:

            sec = sec + check_interval

            if max_ingestion_time and sec > max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException("SageMaker job took more than "
                                       "%s seconds", max_ingestion_time)

            time.sleep(check_interval)
            try:
                response = describe_function(*args)
                status = response[key]
                self.log.info("Job still running for %s seconds... "
                              "current status is %s" % (sec, status))
            except KeyError:
                raise AirflowException("Could not get status of the SageMaker job")
            except ClientError:
                raise AirflowException("AWS request failed, check logs for more info")

            if status in non_terminal_states:
                running = True
            elif status in failed_state:
                raise AirflowException("SageMaker job failed because %s"
                                       % response['FailureReason'])
            else:
                running = False

        self.log.info('SageMaker Job Compeleted')

    def get_conn(self):
        """
        Establish an AWS connection
        :return: a boto3 SageMaker client
        """
        return self.get_client_type('sagemaker', region_name=self.region_name)

    def create_training_job(self, config, wait_for_completion=True,
                            check_interval=30, max_ingestion_time=None):
        """
        Create a training job

        :param config: the config for training
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
        will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
        SageMaker jobs that run longer than this will fail. Setting this to
        None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to training job creation
        """

        self.check_training_config(config)

        response = self.conn.create_training_job(
            **config)
        if wait_for_completion:
            self.check_status(SageMakerHook.non_terminal_states,
                              SageMakerHook.failed_states,
                              'TrainingJobStatus',
                              self.describe_training_job,
                              check_interval, max_ingestion_time,
                              config['TrainingJobName']
                              )
        return response

    def create_tuning_job(self, config, wait_for_completion=True,
                          check_interval=30, max_ingestion_time=None):
        """
        Create a tuning job

        :param config: the config for tuning
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :param wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
        will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
        SageMaker jobs that run longer than this will fail. Setting this to
        None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to tuning job creation
        """

        self.check_tuning_config(config)

        response = self.conn.create_hyper_parameter_tuning_job(
            **config)
        if wait_for_completion:
            self.check_status(SageMakerHook.non_terminal_states,
                              SageMakerHook.failed_states,
                              'HyperParameterTuningJobStatus',
                              self.describe_tuning_job,
                              check_interval, max_ingestion_time,
                              config['HyperParameterTuningJobName']
                              )
        return response

    def create_transform_job(self, config, wait_for_completion=True,
                             check_interval=30, max_ingestion_time=None):
        """
        Create a transform job

        :param config: the config for transform job
        :type config: dict
        :param wait_for_completion:
        if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
        will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
        SageMaker jobs that run longer than this will fail. Setting this to
        None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to transform job creation

        """

        self.check_s3_url(config
                          ['TransformInput']['DataSource']
                          ['S3DataSource']['S3Uri'])

        response = self.conn.create_transform_job(
            **config)
        if wait_for_completion:
            self.check_status(SageMakerHook.non_terminal_states,
                              SageMakerHook.failed_states,
                              'TransformJobStatus',
                              self.describe_transform_job,
                              check_interval, max_ingestion_time,
                              config['TransformJobName']
                              )
        return response

    def create_model(self, config):
        """
        Create a model job

        :param config: the config for model
        :type config: dict
        :return: A response to model creation
        """

        return self.conn.create_model(
            **config)

    def create_endpoint_config(self, config):
        """
        Create an endpoint config

        :param config: the config for endpoint-config
        :type config: dict
        :return: A response to endpoint config creation
        """

        return self.conn.create_endpoint_config(
            **config)

    def create_endpoint(self, config, wait_for_completion=True,
                        check_interval=30, max_ingestion_time=None):
        """
        Create an endpoint

        :param config: the config for endpoint
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
        will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
        SageMaker jobs that run longer than this will fail. Setting this to
        None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to endpoint creation
        """

        response = self.conn.create_endpoint(
            **config)
        if wait_for_completion:
            self.check_status(SageMakerHook.endpoint_non_terminal_states,
                              SageMakerHook.failed_states,
                              'EndpointStatus',
                              self.describe_endpoint,
                              check_interval, max_ingestion_time,
                              config['EndpointName']
                              )
        return response

    def update_endpoint(self, config, wait_for_completion=True,
                        check_interval=30, max_ingestion_time=None):
        """
        Update an endpoint

        :param config: the config for endpoint
        :type config: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :param check_interval: the time interval in seconds which the operator
        will check the status of any SageMaker job
        :type check_interval: int
        :param max_ingestion_time: the maximum ingestion time in seconds. Any
        SageMaker jobs that run longer than this will fail. Setting this to
        None implies no timeout for any SageMaker job.
        :type max_ingestion_time: int
        :return: A response to endpoint update
        """

        response = self.conn.update_endpoint(
            **config)
        if wait_for_completion:
            self.check_status(SageMakerHook.non_terminal_states,
                              SageMakerHook.failed_states,
                              'EndpointStatus',
                              self.describe_endpoint,
                              check_interval, max_ingestion_time,
                              config['EndpointName']
                              )
        return response

    def describe_training_job(self, name):
        """
        :param name: the name of the training job
        :type name: str
        Return the training job info associated with the current job_name
        :return: A dict contains all the training job info
        """

        return self.conn\
                   .describe_training_job(TrainingJobName=name)

    def describe_tuning_job(self, name):
        """
        :param name: the name of the tuning job
        :type name: string
        Return the tuning job info associated with the current job_name
        :return: A dict contains all the tuning job info
        """

        return self.conn\
            .describe_hyper_parameter_tuning_job(
                HyperParameterTuningJobName=name)

    def describe_model(self, name):
        """
        :param name: the name of the SageMaker model
        :type name: string
        Return the SageMaker model info associated with the model name
        :return: A dict contains all the model info
        """

        return self.conn\
            .describe_model(
                ModelName=name)

    def describe_transform_job(self, name):
        """
        :param name: the name of the transform job
        :type name: string
        Return the transform job info associated with the current job_name
        :return: A dict contains all the transform job info
        """

        return self.conn\
            .describe_transform_job(
                TransformJobName=name)

    def describe_endpoint_config(self, name):
        """
        :param name: the name of the endpoint config
        :type name: string
        :return: A dict contains all the endpoint config info
        """

        return self.conn\
            .describe_endpoint_config(
                EndpointConfigName=name)

    def describe_endpoint(self, name):
        """
        :param name: the name of the endpoint
        :type name: string
        :return: A dict contains all the endpoint info
        """

        return self.conn\
            .describe_endpoint(
                EndpointName=name)
