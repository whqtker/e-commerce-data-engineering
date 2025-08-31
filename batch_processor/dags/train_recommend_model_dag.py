from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
        dag_id="train_recommendation_model_dag",
        start_date=pendulum.datetime(2025, 8, 25, tz="Asia/Seoul"),
        schedule=None,
        catchup=False,
        tags=["recommendation", "spark", "batch"],
) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_als_model_trainer",
        conn_id="spark_default",
        application="/opt/bitnami/spark/jobs/batch/model_trainer.py",
        verbose=True,
    )
