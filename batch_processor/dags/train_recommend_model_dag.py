from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

TARGET_DATE = "2025-08-27"
target_datetime = datetime.fromisoformat(f"{TARGET_DATE}T00:00:00+00:00")
start_time = target_datetime.strftime('%Y-%m-%dT%H:%M:%S+00:00')
end_time = (target_datetime + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S+00:00')

with DAG(
        dag_id="train_recommendation_model_dag",
        start_date=pendulum.datetime(2025, 8, 25, tz="Asia/Seoul"),
        schedule=None,
        catchup=False,
        tags=["recommendation", "spark", "batch"],
) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_als_model_trainer",
        application="/opt/airflow/spark_jobs/batch/jobs/model_trainer.py",
        verbose=True,
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/woodstox-core-6.5.1.jar,"
             "/opt/airflow/jars/stax2-api-4.2.1.jar",
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.network.timeout": "300s",
            "spark.executor.heartbeatInterval": "60s",
        },
        application_args=[
            '--start', start_time,
            '--end', end_time
        ],
    )
