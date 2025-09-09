import os
import logging
from typing import List

import boto3
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.functions import col, lit, explode

logger = logging.getLogger(__name__)


class RecommendationEngine:
    def __init__(self):
        self.spark = self._get_spark_session()
        self.bucket_name = self._get_s3_bucket_name()
        self.model_path = f"s3a://{self.bucket_name}/models/als-recommendation-model"
        self.indexer_path = f"s3a://{self.bucket_name}/models/string-indexer-model"
        self.model = self._load_model()
        self.indexer_model = self._load_indexer()

    def _get_spark_session(self) -> SparkSession:
        # API 서버 환경에서는 로컬 Spark 세션을 사용합니다.
        # 실제 프로덕션에서는 더 견고한 설정이 필요할 수 있습니다.
        return SparkSession.builder \
            .appName("Recommendation-API-Spark") \
            .master("local[*]") \
            .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

    def _get_s3_bucket_name(self) -> str:
        s3_datalake_path = os.getenv('S3_DATALAKE_PATH')
        if s3_datalake_path and s3_datalake_path.startswith('s3a://'):
            return s3_datalake_path.split('/')[2]
        raise ValueError("S3_DATALAKE_PATH 환경 변수가 올바르게 설정되지 않았습니다.")

    def _load_model(self) -> ALSModel:
        logger.info(f"ALS 모델 로딩: {self.model_path}")
        return ALSModel.load(self.model_path)

    def _load_indexer(self) -> PipelineModel:
        logger.info(f"Indexer 모델 로딩: {self.indexer_path}")
        return PipelineModel.load(self.indexer_path)

    def get_recommendations(self, user_id: str, num_items: int = 10) -> List[str]:
        try:
            # 1. 사용자 ID를 숫자 인덱스로 변환
            user_df = self.spark.createDataFrame([(user_id,)], ["user_id"])
            # 전체 파이프라인 대신 첫 번째 스테이지(user_indexer)만 적용
            indexed_user_df = self.indexer_model.stages[0].transform(user_df)

            if indexed_user_df.count() == 0:
                logger.warning(f"'{user_id}'는 학습 데이터에 없는 새로운 사용자입니다.")
                return []  # 또는 인기 상품 등 기본 추천 제공

            user_idx = indexed_user_df.select("user_idx").first()[0]
            user_subset = self.spark.createDataFrame([(user_idx,)], ["user_idx"])

            # 2. 해당 사용자에 대한 추천 생성
            recommendations_df = self.model.recommendForUserSubset(user_subset, num_items)

            # 3. 추천된 상품 인덱스를 다시 문자열 ID로 변환
            product_indices = recommendations_df.select(explode("recommendations").alias("rec")) \
                .select("rec.product_idx")

            # StringIndexerModel은 역변환을 직접 지원하지 않으므로, 원본 매핑 정보를 사용해야 합니다.
            # 여기서는 간단하게 원본 product_id와 product_idx를 조인하여 변환합니다.
            product_labels = self.indexer_model.stages[1].labels
            product_mapping_df = self.spark.createDataFrame(
                [(i, label) for i, label in enumerate(product_labels)],
                ["product_idx", "product_id"]
            )

            recommended_products_df = product_indices.join(product_mapping_df, "product_idx")

            product_ids = [row.product_id for row in recommended_products_df.collect()]

            return product_ids

        except Exception as e:
            logger.error(f"추천 생성 중 오류 발생: {e}", exc_info=True)
            return []


# 싱글톤 인스턴스
recommendation_engine = RecommendationEngine()
