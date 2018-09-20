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
import copy
import time
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook


class SageMakerHook(AwsHook):
    """
    Interact with Amazon SageMaker.
    sagemaker_conn_id is required for using
    the config stored in db for training/tuning
    """
    non_terminal_states = {'InProgress', 'Stopping', 'Stopped'}
    failed_states = {'Failed'}

    def __init__(self,
                 sagemaker_conn_id=None,
                 use_db_config=False,
                 region_name=None,
                 check_interval=5,
                 max_ingestion_time=None,
                 *args, **kwargs):
        super(SageMakerHook, self).__init__(*args, **kwargs)
        self.sagemaker_conn_id = sagemaker_conn_id
        self.use_db_config = use_db_config
        self.region_name = region_name
        self.check_interval = check_interval
        self.max_ingestion_time = max_ingestion_time
        self.conn = self.get_conn()
        self.runtime_conn = self.get_runtime_conn()

    def check_for_url(self, s3url):
        """
        check if the s3url exists
        :param s3url: S3 url
        :type s3url:str
        :return: bool
        """
        bucket, key = S3Hook.parse_s3_url(s3url)
        s3hook = S3Hook(aws_conn_id=self.aws_conn_id)
        if not s3hook.check_for_bucket(bucket_name=bucket):
            raise AirflowException(
                "The input S3 Bucket {} does not exist ".format(bucket))
        if key and not s3hook.check_for_key(key=key, bucket_name=bucket)\
           and not s3hook.check_for_prefix(
                prefix=key, bucket_name=bucket, delimiter='/'):
            # check if s3 key exists in the case user provides a single file
            # or if s3 prefix exists in the case user provides a prefix for files
            raise AirflowException("The input S3 Key "
                                   "or Prefix {} does not exist in the Bucket {}"
                                   .format(s3url, bucket))
        return True

    def check_valid_training_input(self, training_config):
        """
        Run checks before a training starts
        :param training_config: training_config
        :type training_config: dict
        :return: None
        """
        for channel in training_config['InputDataConfig']:
            self.check_for_url(channel['DataSource']
                               ['S3DataSource']['S3Uri'])

    def check_valid_tuning_input(self, tuning_config):
        """
        Run checks before a tuning job starts
        :param tuning_config: tuning_config
        :type tuning_config: dict
        :return: None
        """
        for channel in tuning_config['TrainingJobDefinition']['InputDataConfig']:
            self.check_for_url(channel['DataSource']
                               ['S3DataSource']['S3Uri'])

    def check_status(self, non_terminal_states,
                     failed_state, key,
                     describe_function, *args):
        """
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
        :return: None
        """
        sec = 0
        running = True

        while running:

            sec = sec + self.check_interval

            if self.max_ingestion_time and sec > self.max_ingestion_time:
                # ensure that the job gets killed if the max ingestion time is exceeded
                raise AirflowException("SageMaker job took more than "
                                       "%s seconds", self.max_ingestion_time)

            time.sleep(self.check_interval)
            try:
                response = describe_function(*args)
                status = response[key]
                self.log.info("Job still running for %s seconds... "
                              "current status is %s" % (sec, status))
            except KeyError:
                raise AirflowException("Could not get status of the SageMaker job")
            except ClientError:
                raise AirflowException("AWS request failed, check log for more info")

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

    def get_runtime_conn(self):
        """
        Establish an AWS connection
        :return: a boto3 SageMaker runtime client
        """
        return self.get_client_type('sagemaker-runtime', region_name=self.region_name)

    def list_training_jobs(self, **kwargs):
        """
        List the training jobs associated with the given input
        :param https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_training_jobs
        :return: A dict of training job summaries
        """  # noqa
        return self.conn.list_training_jobs(**kwargs)

    def list_tuning_jobs(self, **kwargs):
        """
        List the tuning jobs associated with the given input
        :param https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_hyper_parameter_tuning_jobs
        :return: A dict of hyperparamter tuning job summaries
        """  # noqa
        return self.conn.list_hyper_parameter_tuning_job(**kwargs)

    def list_transform_jobs(self, **kwargs):
        """
        :param https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_transform_jobs
        :return: A dict of transform job summaries
        """  # noqa
        return self.conn.list_transform_jobs(**kwargs)

    def list_models(self, **kwargs):
        """
        :param https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_models
        :return: A dict of model summaries
        """  # noqa
        return self.conn.list_models(**kwargs)

    def list_endpoint_configs(self, **kwargs):
        """
        :param https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_endpoint_configs
        :return: A dict of endpoint config summaries
        """  # noqa
        return self.conn.list_endpoint_configs(**kwargs)

    def list_endpoints(self, **kwargs):
        """
        :param https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.list_endpoints
        :return: A dict of endpoints summaries
        """  # noqa
        return self.conn.list_endpoints(**kwargs)

    def create_training_job(self, training_job_request, wait_for_completion=True):
        """
        Create a training job
        :param training_job_request: the request for creating training job
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_training_job
        :type training_job_request: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :type wait_for_completion: bool
        :return: A dict that contains information of training job created
        """
        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException("SageMaker connection id must be present to read \
                                        SageMaker training jobs configuration.")
            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = copy.deepcopy(sagemaker_conn.extra_dejson)
            training_job_request.update(config)

        self.check_valid_training_input(training_job_request)

        response = self.conn.create_training_job(
            **training_job_request)
        if wait_for_completion:
            self.check_status(SageMakerHook.non_terminal_states,
                              SageMakerHook.failed_states,
                              'TrainingJobStatus',
                              self.describe_training_job,
                              training_job_request['TrainingJobName'])
        return response

    def create_tuning_job(self, tuning_job_request, wait_for_completion=True):
        """
        Create a tuning job
        :param tuning_job_request: the request for creating tuning job
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_hyper_parameter_tuning_job
        :type tuning_job_request: dict
        :param wait_for_completion: if the program should keep running until job finishes
        :param wait_for_completion: bool
        :return: A dict that contains information of hyperparameter tuning job created
        """  # noqa
        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException(
                    "SageMaker connection id must be present to \
                    read SageMaker tunning job configuration.")

            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = sagemaker_conn.extra_dejson.copy()
            tuning_job_request.update(config)

        self.check_valid_tuning_input(tuning_job_request)

        response = self.conn.create_hyper_parameter_tuning_job(
            **tuning_job_request)
        if wait_for_completion:
            self.check_status(SageMakerHook.non_terminal_states,
                              SageMakerHook.failed_states,
                              'HyperParameterTuningJobStatus',
                              self.describe_tuning_job,
                              tuning_job_request['HyperParameterTuningJobName'])
        return response

    def create_transform_job(self, transform_job_request, wait_for_completion=True):
        """
        Create a transform job
        :param transform_job_request: the request for creating transform job
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_transform_job
        :type transform_job_request: dict
        :param wait_for_completion:
        if the program should keep running until job finishes
        :type wait_for_completion: bool
        :return: A dict that contains information of transform job created
        """  # noqa
        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException(
                    "SageMaker connection id must be present to \
                    read SageMaker transform job configuration.")

            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = sagemaker_conn.extra_dejson.copy()
            transform_job_request.update(config)

        self.check_for_url(transform_job_request
                           ['TransformInput']['DataSource']
                           ['S3DataSource']['S3Uri'])

        response = self.conn.create_transform_job(
            **transform_job_request)
        if wait_for_completion:
            self.check_status(SageMakerHook.non_terminal_states,
                              SageMakerHook.failed_states,
                              'TransformJobStatus',
                              self.describe_transform_job,
                              transform_job_request['TransformJobName'])
        return response

    def create_model(self, model_request):
        """
        Create a model job
        :param model_request: the request for creating model
        See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_model
        :type model_request: dict
        :return: A dict that contains information of model created
        """  # noqa
        if self.use_db_config:
            if not self.sagemaker_conn_id:
                raise AirflowException(
                    "SageMaker connection id must be present to \
                    read SageMaker model configuration.")

            sagemaker_conn = self.get_connection(self.sagemaker_conn_id)

            config = sagemaker_conn.extra_dejson.copy()
            model_request.update(config)

        self.check_for_url(model_request
                           ['PrimaryContainer']['ModelDataUrl'])
        return self.conn.create_model(
            **model_request)

    def describe_training_job(self, training_job_name):
        """
        :param training_job_name: the name of the training job
        :type training_job_name: str
        Return the training job info associated with the current job_name
        :return: A dict contains all the training job info
        """
        return self.conn\
                   .describe_training_job(TrainingJobName=training_job_name)

    def describe_tuning_job(self, tuning_job_name):
        """
        :param tuning_job_name: the name of the tuning job
        :type tuning_job_name: string
        Return the tuning job info associated with the current job_name
        :return: A dict contains all the tuning job info
        """
        return self.conn\
            .describe_hyper_parameter_tuning_job(
                HyperParameterTuningJobName=tuning_job_name)

    def describe_transform_job(self, transform_job_name):
        """
        :param transform_job_name: the name of the transform job
        :type transform_job_name: string
        Return the transform job info associated with the current job_name
        :return: A dict contains all the transform job info
        """
        return self.conn\
            .describe_transform_job(
                TransformJobName=transform_job_name)
