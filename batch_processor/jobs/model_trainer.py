import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_s3_bucket_name(bucket_prefix="e-commerce-data-lake"):
    try:
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

    bucket_name = get_s3_bucket_name()
    logger.info(f"사용할 S3 버킷: {bucket_name}")

    spark = SparkSession.builder \
        .appName("ALS Model Trainer") \
        .config("spark.driver.host", "localhost") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.6,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367") \
        .getOrCreate()

    logger.info("Spark Session 생성 완료")

    try:
        s3_data_path = f"s3a://{bucket_name}/events"

        logger.info(f"S3 데이터 경로: {s3_data_path}")

        raw_data = spark.read \
            .option("basePath", s3_data_path) \
            .parquet(f"{s3_data_path}/*")

        logger.info(f"S3에서 Parquet 데이터 로드 완료")
        logger.info(f"로드된 데이터 건수: {raw_data.count()}")

        logger.info("데이터 스키마:")
        raw_data.printSchema()

    except Exception as e:
        logger.error(f"S3 Parquet 데이터 로드 실패: {e}")

    with open('batch_processor/sql/user_behavior.sql', 'r') as f:
        query = f.read()
    interaction_df = spark.sql(query)

    logger.info("데이터 로드 및 상호작용 점수 계산 완료")

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
        ratingCol="rating",
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

    try:
        model.write().overwrite().save(model_s3_path)
        indexer_model.write().overwrite().save(indexer_s3_path)

        logger.info(f"ALS 모델 S3 저장 완료: {model_s3_path}")
        logger.info(f"StringIndexer 모델 S3 저장 완료: {indexer_s3_path}")

    except Exception as e:
        logger.error(f"S3 모델 저장 실패: {e}")
        local_model_path = "./models/als_model"
        local_indexer_path = "./models/indexer_model"

        model.write().overwrite().save(local_model_path)
        indexer_model.write().overwrite().save(local_indexer_path)

        logger.info(f"로컬 백업 저장 완료: {local_model_path}")

    spark.stop()


if __name__ == "__main__":
    train_model()
