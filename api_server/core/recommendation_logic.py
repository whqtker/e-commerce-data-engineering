from __future__ import annotations

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
        # 시작 시점에 모델을 로드하지 않고 None으로 초기화 (Lazy Loading)
        self.model: ALSModel | None = None
        self.indexer_model: PipelineModel | None = None
        self._models_loaded = False

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

    def _ensure_models_loaded(self):
        """모델이 로드되었는지 확인하고, 로드되지 않았다면 로드를 시도합니다."""
        if not self._models_loaded:
            logger.info("모델 Lazy Loading 시작...")
            try:
                self.model = self._load_model()
                self.indexer_model = self._load_indexer()
                self._models_loaded = True
                logger.info("모델 Lazy Loading 성공.")
            except Exception as e:
                # Py4JJavaError는 Spark가 경로를 찾지 못할 때 발생합니다.
                if 'Input path does not exist' in str(e):
                    logger.warning(f"모델 경로를 찾을 수 없습니다. 모델이 아직 학습되지 않았을 수 있습니다. 경로: {self.model_path}")
                else:
                    logger.error(f"모델 로딩 중 예측하지 못한 오류 발생: {e}", exc_info=True)
                # 로드 실패 시, 다시 시도하지 않도록 _models_loaded를 True로 설정합니다.
                # 또는 특정 시간 후 재시도 로직을 추가할 수도 있습니다.
                self._models_loaded = True


    def _load_model(self) -> ALSModel:
        logger.info(f"ALS 모델 로딩: {self.model_path}")
        return ALSModel.load(self.model_path)

    def _load_indexer(self) -> PipelineModel:
        logger.info(f"Indexer 모델 로딩: {self.indexer_path}")
        return PipelineModel.load(self.indexer_path)

    def get_recommendations(self, user_id: str, num_items: int = 10) -> List[str]:
        # 추천 요청이 들어왔을 때 모델 로드를 시도합니다.
        self._ensure_models_loaded()

        # 모델 로드에 실패했다면 빈 리스트를 반환합니다.
        if not self.model or not self.indexer_model:
            logger.warning("모델이 로드되지 않아 추천을 생성할 수 없습니다.")
            return []

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


recommendation_engine = RecommendationEngine()
