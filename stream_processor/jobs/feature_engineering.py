import logging
from typing import Dict, Any
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


class FeatureEngineering:
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_user_behavior(self, df: DataFrame) -> DataFrame:
        try:
            # 기본 피처 생성
            enhanced_df = self._add_basic_features(df)
            
            # 시간 기반 피처 생성
            enhanced_df = self._add_temporal_features(enhanced_df)
            
            # 집계 피처 생성
            enhanced_df = self._add_aggregated_features(enhanced_df)
            
            # 사용자 세그먼트 생성
            enhanced_df = self._add_user_segments(enhanced_df)
            
            self.logger.info("피처 엔지니어링 완료")
            return enhanced_df
            
        except Exception as e:
            self.logger.error(f"피처 엔지니어링 실패: {e}")
            raise
    
    def _add_basic_features(self, df: DataFrame) -> DataFrame:
        return df.withColumn("price_range", 
                           when(col("product_price") < 50, "low")
                           .when(col("product_price") < 200, "medium")
                           .otherwise("high")) \
                .withColumn("is_mobile", 
                          col("user_agent").rlike("(?i)mobile|android|iphone")) \
                .withColumn("referrer_type",
                          when(col("referrer").isNull(), "direct")
                          .when(col("referrer").contains("google"), "search")
                          .when(col("referrer").contains("facebook"), "social")
                          .otherwise("other"))
    
    def _add_temporal_features(self, df: DataFrame) -> DataFrame:
        return df.withColumn("hour_of_day", hour("timestamp")) \
                .withColumn("day_of_week", dayofweek("timestamp")) \
                .withColumn("is_weekend", 
                          when(dayofweek("timestamp").isin([1, 7]), True)
                          .otherwise(False)) \
                .withColumn("time_segment",
                          when(hour("timestamp").between(6, 11), "morning")
                          .when(hour("timestamp").between(12, 17), "afternoon")
                          .when(hour("timestamp").between(18, 22), "evening")
                          .otherwise("night"))
    
    def _add_aggregated_features(self, df: DataFrame) -> DataFrame:
        # 세션 기반 윈도우: 특정 사용자가 한 번의 방문 동안 일어난 행동 분석
        user_session_window = Window.partitionBy("user_id", "session_id").orderBy("timestamp")

        df_with_unix_ts = df.withColumn("timestamp_unix", unix_timestamp("timestamp"))
        
        # 시간 기반 윈도우: 특정 시용자가 일정 시간 동안 일어난 동작 분석
        time_window = Window.partitionBy("user_id") \
                           .orderBy("timestamp_unix") \
                           .rangeBetween(-600, 0)  # 10분 (600초)
        
        return df_with_unix_ts.withColumn("action_sequence",
                           row_number().over(user_session_window)) \
                .withColumn("session_action_count", 
                           count("*").over(user_session_window)) \
                .withColumn("recent_view_count",
                           sum(when(col("action_type") == "view", 1)
                               .otherwise(0)).over(time_window)) \
                .withColumn("recent_cart_count",
                           sum(when(col("action_type") == "add_to_cart", 1)
                               .otherwise(0)).over(time_window)) \
                .withColumn("avg_price_viewed",
                           avg(when(col("action_type") == "view", col("product_price")))
                           .over(time_window)) \
                .drop("timestamp_unix")
    
    def _add_user_segments(self, df: DataFrame) -> DataFrame:
        return df.withColumn("user_behavior_type",
                           when((col("recent_cart_count") > 0) & 
                                (col("action_sequence") <= 3), "quick_buyer")
                           .when(col("recent_view_count") > 5, "browser")
                           .when((col("action_type") == "purchase") &
                                 (col("session_action_count") <= 2), "direct_buyer")
                           .otherwise("casual_visitor")) \
                .withColumn("engagement_level",
                          when(col("session_action_count") >= 10, "high")
                          .when(col("session_action_count") >= 5, "medium")
                          .otherwise("low"))
    
    def calculate_user_features(self, df: DataFrame) -> DataFrame:
        return df.groupBy("user_id") \
                .agg(
                    count("*").alias("total_events"),
                    count_distinct("session_id").alias("session_count"),
                    count_distinct("product_id").alias("unique_products_viewed"),
                    count_distinct("product_category").alias("categories_explored"),
                    avg("product_price").alias("avg_price_interest"),
                    sum(when(col("action_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                    sum(when(col("action_type") == "add_to_cart", 1).otherwise(0)).alias("cart_additions"),
                    max("timestamp").alias("last_activity")
                ) \
                .withColumn("conversion_rate",
                          col("purchase_count") / col("total_events")) \
                .withColumn("cart_conversion_rate",
                          col("purchase_count") / 
                          when(col("cart_additions") > 0, col("cart_additions")).otherwise(1))
    
    def generate_product_features(self, df: DataFrame) -> DataFrame:
        return df.groupBy("product_id", "product_category") \
                .agg(
                    count("*").alias("total_interactions"),
                    count_distinct("user_id").alias("unique_users"),
                    sum(when(col("action_type") == "view", 1).otherwise(0)).alias("view_count"),
                    sum(when(col("action_type") == "add_to_cart", 1).otherwise(0)).alias("cart_count"),
                    sum(when(col("action_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                    first("product_price").alias("price")
                ) \
                .withColumn("popularity_score",
                          col("unique_users") / col("total_interactions")) \
                .withColumn("conversion_rate",
                          col("purchase_count") / col("view_count"))
