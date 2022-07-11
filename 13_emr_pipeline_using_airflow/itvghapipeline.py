import os
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

ai_cluster = {
    "Name": "AI Cluster",
    "LogUri": "s3n://aws-logs-269066542444-us-east-1/elasticmapreduce/",
    "ReleaseLabel": "emr-6.6.0",
    "Instances": {
        "KeepJobFlowAliveWhenNoSteps": True,
        "InstanceGroups": [
            {
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "SizeInGB": 32,
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 2
                        }
                    ]
                },
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "Name": "Master - 1"
            },
            {
                "InstanceCount": 2,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "SizeInGB": 32,
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 2
                        }
                    ]
                },
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "Name": "Core - 2"
            }
        ]
    },
    "Applications": [
        {
            "Name": "Hadoop"
        },
        {
            "Name": "Spark"
        }
    ],
    "Configurations": [
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            }
        }
    ],
    "ServiceRole": "EMR_DefaultRole",
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "AutoScalingRole": "EMR_AutoScaling_DefaultRole",
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
    "EbsRootVolumeSize": 10,
    "AutoTerminationPolicy": {
        "IdleTimeout": 3600
    }
}

emr_steps = [
        {
            'Name': 'GHActivity Processor',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                         '--deploy-mode',
                         'cluster',
                         '--conf',
                         'spark.yarn.appMasterEnv.ENVIRON=PROD',
                         '--conf',
                         'spark.yarn.appMasterEnv.SRC_DIR=s3://aigithub/landing/ghactivity',
                         '--conf',
                         'spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json',
                         '--conf',
                         'spark.yarn.appMasterEnv.TGT_DIR=s3://aigithub/emrraw/ghactivity',
                         '--conf',
                         'spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet',
                         '--conf',
                         'spark.yarn.appMasterEnv.SRC_FILE_PATTERN=2021-01-13',
                         '--py-files',
                         's3://aigithub/app/itv-ghactivity.zip',
                         's3://aigithub/app/app.py']
            }
        }
    ]
with DAG(
    dag_id='ai_cluster',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['aighcluster'],
    catchup=False,
) as dag:
    job_flow_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=ai_cluster
    )

    job_sensor = EmrJobFlowSensor(
        task_id='check_job_flow',
        job_flow_id=job_flow_creator.output,
    )

    spark_step_add = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=job_flow_creator.output,
        steps=emr_steps
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=job_flow_creator.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id=job_flow_creator.output,
    )

    terminate_sensor = EmrJobFlowSensor(
        task_id='check_job_flow_terminate',
        job_flow_id=job_flow_creator.output,
    )

    chain(job_flow_creator,
        job_sensor,
        spark_step_add,
        step_checker,
        cluster_remover,
        terminate_sensor
    )
