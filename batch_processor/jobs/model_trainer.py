import logging
import boto3
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lit, to_timestamp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=False, help="ISO-8601 start timestamp, e.g. 2025-09-06T00:00:00+00:00")
    parser.add_argument("--end", required=False, help="ISO-8601 end timestamp, e.g. 2025-09-06T01:00:00+00:00")
    return parser.parse_args()


def get_s3_bucket_name(bucket_prefix="e-commerce-data-lake"):
    try:
        s3_datalake_path = os.getenv('S3_DATALAKE_PATH')
        if s3_datalake_path and s3_datalake_path.startswith('s3a://'):
            bucket_name = s3_datalake_path.split('/')[2]
            logger.info(f"환경 변수에서 S3 버킷 이름 로드: {bucket_name}")
            return bucket_name

        s3 = boto3.client('s3')
        response = s3.list_buckets()

        for bucket in response['Buckets']:
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']

        raise ValueError(f"버킷 '{bucket_prefix}*'를 찾을 수 없습니다.")
    except Exception as e:
        logger.error(f"S3 버킷 조회 실패: {e}")
        return f"{bucket_prefix}-default"

def train_model():
    """ALS 추천 모델을 학습 및 저장"""

    args = parse_args()
    start_ts = args.start
    end_ts = args.end

    bucket_name = get_s3_bucket_name()
    logger.info(f"사용할 S3 버킷: {bucket_name}")

    spark = SparkSession.builder \
        .appName("ALS Model Trainer") \
        .config("spark.master", "spark://spark-master:7077") \
        .getOrCreate()

    logger.info("Spark Session 생성 완료")

    try:
        s3_data_path = f"s3a://{bucket_name}/events"

        logger.info(f"S3 데이터 경로: {s3_data_path}")

        raw_data = spark.read.parquet(s3_data_path)

        if start_ts and end_ts:
            fmt_fn = "yyyy-MM-dd'T'HH:mm:ssXXX"
            start_col = to_timestamp(lit(start_ts), fmt_fn)
            end_col = to_timestamp(lit(end_ts),   fmt_fn)
            raw_data = raw_data.filter(
                (col("timestamp") >= start_col) & (col("timestamp") < end_col)
            )

        raw_data.createOrReplaceTempView("user_behavior_events")
        logger.info("S3 데이터를 'user_behavior_events' 임시 뷰로 등록 완료")

        sql_file_path = "/opt/airflow/spark_jobs/batch/sql/user_behavior.sql"
        with open(sql_file_path, 'r') as f:
            query = f.read()

        interaction_df = spark.sql(query)
        logger.info("데이터 로드 및 상호작용 점수 계산 완료")
        interaction_df.show(5)

        # 문자열 ID를 숫자 ID로 변환 ---
        user_indexer = StringIndexer(inputCol="user_id", outputCol="user_idx", handleInvalid="skip")
        product_indexer = StringIndexer(inputCol="product_id", outputCol="product_idx", handleInvalid="skip")

        pipeline = Pipeline(stages=[user_indexer, product_indexer])
        indexer_model = pipeline.fit(interaction_df)
        indexed_df = indexer_model.transform(interaction_df)

        logger.info("StringIndexer 변환 완료")

        als = ALS(
            userCol="user_idx",
            itemCol="product_idx",
            ratingCol="interaction_strength",
            coldStartStrategy="drop", # 학습 데이터에 없는 아이템/사용자에 대한 예측을 NaN으로 처리
            rank=10,
            maxIter=10,
            regParam=0.1,
            nonnegative=True
        )

        model = als.fit(indexed_df)
        logger.info("ALS 모델 학습 완료")

        # S3에 모델 저장
        model_s3_path = f"s3a://{bucket_name}/models/als-recommendation-model"
        indexer_s3_path = f"s3a://{bucket_name}/models/string-indexer-model"

        model.write().overwrite().save(model_s3_path)
        indexer_model.write().overwrite().save(indexer_s3_path)
        logger.info(f"모델 S3 저장 완료: {model_s3_path}")

    except Exception as e:
        logger.error(f"모델 학습 또는 저장 실패: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark Session 종료")


if __name__ == "__main__":
    train_model()
