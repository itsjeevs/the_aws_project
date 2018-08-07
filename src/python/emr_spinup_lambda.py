#!/usr/bin/env python
import boto3
from datetime import datetime

__author__ = 'itsjeevs'


def lambda_handler(event, context):

    instance_type_master = event.get('MasterInstanceType', 'm3.xlarge')
    instance_type_core = event.get('CoreInstanceType', 'm3.xlarge')
    instance_type_slave = event.get('SlaveInstanceType', 'm3.xlarge')
    instance_count_core = event.get('CoreInstanceCount', 5)
    instance_count_slave = event.get('SlaveInstanceCount', 20)
    cluster_name = event.get('ClusterName', 'MyFirstETL')
    key_name = event.get('KeyName', "<TO_BE_FILLED>")
    launched_by = event.get('LauncehdBy', "<TO_BE_FILLED>")


    connection = boto3.client(
                'emr',
                region_name="us-west-2"
            )
    BUCKET_NAME = "<TO_BE_FILLED>"
    CODE_HOME = "<TO_BE_FILLED>"

    EmrManagedMasterSecurityGroup = "<TO_BE_FILLED>"
    EmrManagedSlaveSecurityGroup = "<TO_BE_FILLED>"
  
    CLUSTER_TERMINATION = "<TO_BE_FILLED>"

    cluster_id = connection.run_job_flow(
                Name=cluster_name,
                ReleaseLabel = 'emr-5.15.0',
                Instances = {
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    'Ec2KeyName': key_name,
                    'EmrManagedMasterSecurityGroup': EmrManagedMasterSecurityGroup,
                    'EmrManagedSlaveSecurityGroup': EmrManagedSlaveSecurityGroup,
                    'InstanceGroups':[
                        {
                            'Name': 'Master',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': instance_type_master,
                            'InstanceCount': 1,
                        },
                        {
                            'Name': 'Core',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            'InstanceType': instance_type_core,
                            'InstanceCount': instance_count_core,
                        },
                        {
                            'Name': 'Task',
                            'Market': 'SPOT',
                            'InstanceRole': 'TASK',
                            'InstanceType': instance_type_slave,
                            'InstanceCount': instance_count_slave,
                        }],
                    },
                Steps = [
                    {
                        'Name': 'Setup Hadoop Debugging',
                        'ActionOnFailure': CLUSTER_TERMINATION,
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['state-pusher-script']
                        }
                    },{
                    'Name': 'Copy code to /home/hadoop/code/',
                    'ActionOnFailure': CLUSTER_TERMINATION,
                    'HadoopJarStep': {
                        'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': [
                            '<YOUR S3 CODE SOURCE>/code_copy.sh'
                        ]}
                	},{
                    'Name': 'Spark ETL',
                    'ActionOnFailure': CLUSTER_TERMINATION,
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            "spark-submit",
                            "--deploy-mode",
                            "client",
                            "/home/hadoop/spark_job.py",
                            ]}
                	},{
                    'Name': 'Upload to Dynamodb',
                    'ActionOnFailure': CLUSTER_TERMINATION,
                    'HadoopJarStep': {
                        'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': [
                            '/home/hadoop/dynamodb_update.py'
                        ]}
                	}
                ],
                Configurations = [
                    {
                        "Classification": "spark",
                        "Properties": {
                            "maximizeResourceAllocation": "true"
                        },
                        "Configurations": []
                    }],
                Applications = [
                    {
                        "Name": "Hadoop",
                    },
                    {
                        "Name": "Spark",
                    },
                    {
                        "Name": "Zeppelin",
                    },
                ],
                VisibleToAllUsers = True,
                JobFlowRole = 'EMR_EC2_DefaultRole',
                ServiceRole = 'EMR_DefaultRole',
                EbsRootVolumeSize=50,
            )
lambda_handler({}, {})