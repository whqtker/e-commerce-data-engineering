import os
import sys
import logging
from typing import Optional, Dict, Any
from datetime import datetime
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import redis
from pyspark.sql.streaming import StreamingQuery

from .jobs.feature_engineering import FeatureEngineering


def _store_user_behavior(pipeline, row):
    user_id = row['user_id']
    timestamp = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

    # 최근 행동 이력 (List)
    behavior_key = f"user:{user_id}:recent_behaviors"
    behavior_data = json.dumps({
        'product_id': row['product_id'],
        'action_type': row['action_type'],
        'category': row['product_category'],
        'price': float(row['product_price']),
        'timestamp': timestamp
    })

    pipeline.lpush(behavior_key, behavior_data)
    pipeline.ltrim(behavior_key, 0, 99)  # 최근 100개만 유지
    pipeline.expire(behavior_key, 86400 * 7)  # 7일 TTL


def _store_realtime_features(pipeline, row):
    """실시간 피처 저장"""
    user_id = row['user_id']

    # 사용자별 카테고리 관심도 (Hash)
    category_key = f"user:{user_id}:category_interests"
    pipeline.hincrby(category_key, row['product_category'], 1)
    pipeline.expire(category_key, 86400 * 30)  # 30일 TTL

    # 가격대별 선호도 (Sorted Set)
    price_range = _get_price_range(row['product_price'])
    price_key = f"user:{user_id}:price_preferences"
    pipeline.zincrby(price_key, 1, price_range)
    pipeline.expire(price_key, 86400 * 30)

    # 행동 타입별 카운트 (Hash)
    action_key = f"user:{user_id}:action_counts"
    pipeline.hincrby(action_key, row['action_type'], 1)
    pipeline.expire(action_key, 86400 * 30)


def _store_session_data(pipeline, row):
    session_id = row['session_id']

    # 세션 정보 (Hash)
    session_key = f"session:{session_id}"
    session_data = {
        'user_id': row['user_id'],
        'start_time': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
        'last_activity': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
        'action_count': 1
    }

    for field, value in session_data.items():
        pipeline.hset(session_key, field, value)

    pipeline.expire(session_key, 3600)  # 1시간 TTL


def _get_price_range(price: float) -> str:
    """가격을 범위로 분류"""
    if price < 50:
        return "0-50"
    elif price < 100:
        return "50-100"
    elif price < 200:
        return "100-200"
    elif price < 500:
        return "200-500"
    else:
        return "500+"


# Kafka 스트림 처리 및 Redis 저장
class StreamProcessor:
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config)
        self.spark = self._create_spark_session()
        self.redis_client = self._create_redis_client()
        self.feature_engineer = FeatureEngineering()

        # 스트림 상태 관리
        self.active_streams: Dict[str, StreamingQuery] = {}
        
    def _load_config(self, config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        default_config = {
            # Kafka 설정
            'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'kafka_topics': os.getenv('KAFKA_TOPICS', 'user-behavior-events'),
            'kafka_starting_offsets': os.getenv('KAFKA_STARTING_OFFSETS', 'latest'),
            
            # Redis 설정
            'redis_host': os.getenv('REDIS_HOST', 'localhost'),
            'redis_port': int(os.getenv('REDIS_PORT', '6379')),
            'redis_db': int(os.getenv('REDIS_DB', '0')),
            'redis_password': os.getenv('REDIS_PASSWORD'),
            
            # Spark 설정
            'app_name': 'E-commerce-Stream-Processor',
            'checkpoint_location': os.getenv('CHECKPOINT_LOCATION', './checkpoints'),
            'output_mode': 'update',
            'trigger_interval': '10 seconds',
            
            # 처리 설정
            'batch_size': int(os.getenv('BATCH_SIZE', '100')),
            'watermark_delay': '1 minute',
        }
        
        if config:
            default_config.update(config)
        
        return default_config
    
    # forceDeleteTempCheckpointLocation: 스트리밍 쿼리 정상 종료 시 체크포인트 파일 삭제하도록
    def _create_spark_session(self) -> SparkSession:
        import sys
        python_path = sys.executable
        os.environ['PYSPARK_PYTHON'] = python_path
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

        spark = SparkSession.builder \
            .appName(self.config['app_name']) \
            .master("local[12]") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.port", "0") \
            .config("spark.sql.streaming.checkpointLocation", self.config['checkpoint_location']) \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                   "com.redislabs:spark-redis_2.12:3.1.0") \
            .config("spark.redis.host", self.config['redis_host']) \
            .config("spark.redis.port", self.config['redis_port']) \
            .config("spark.redis.db", self.config['redis_db']) \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        # 로그 레벨 설정
        spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info("Spark Session 생성 완료")
        return spark
    
    def _create_redis_client(self) -> redis.Redis:
        redis_config = {
            'host': self.config['redis_host'],
            'port': self.config['redis_port'],
            'db': self.config['redis_db'],
            'decode_responses': True
        }
        
        if self.config['redis_password']:
            redis_config['password'] = self.config['redis_password']
        
        client = redis.Redis(**redis_config)
        
        # 연결 테스트
        try:
            client.ping()
            self.logger.info("Redis 연결 성공")
        except Exception as e:
            self.logger.error(f"Redis 연결 실패: {e}")
            raise
        
        return client
    
    def _define_schema(self) -> StructType:
        return StructType([
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("action_type", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("session_id", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_price", FloatType(), True),
            StructField("user_agent", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("referrer", StringType(), True)
        ])
    
    # Kafka에서 스트리밍 데이터 읽기
    # failOnDataLoss: 데이터를 읽는 도중 유실된 상황을 감지했을 때 작업 실패 여부
    def create_kafka_stream(self) -> DataFrame:
        try:
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config['kafka_bootstrap_servers']) \
                .option("subscribe", self.config['kafka_topics']) \
                .option("startingOffsets", self.config['kafka_starting_offsets']) \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Kafka value를 JSON으로 파싱
            schema = self._define_schema()
            
            parsed_df = kafka_df.select(
                col("key").cast("string").alias("kafka_key"),
                from_json(col("value").cast("string"), schema).alias("data"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp")
            ).select(
                "kafka_key",
                "data.*",
                "topic",
                "partition", 
                "offset",
                "kafka_timestamp"
            )
            
            # 타임스탬프 변환 및 워터마크 설정
            processed_df = parsed_df \
                .withColumn("timestamp", 
                           when(col("timestamp").isNull(), 
                                current_timestamp()).otherwise(col("timestamp"))) \
                .withWatermark("timestamp", self.config['watermark_delay'])
            
            self.logger.info("Kafka 스트림 생성 완료")
            return processed_df
            
        except Exception as e:
            self.logger.error(f"Kafka 스트림 생성 실패: {e}")
            raise
    
    def process_and_store_to_redis(self, df: DataFrame) -> StreamingQuery:
        
        def process_batch(batch_df: DataFrame, batch_id: int):
            try:
                if batch_df.count() == 0:
                    return
                
                self.logger.info(f"배치 {batch_id} 처리 시작 - 레코드 수: {batch_df.count()}")
                
                # 피처 엔지니어링
                processed_df = self.feature_engineer.process_user_behavior(batch_df)
                
                # Redis에 저장
                self._save_to_redis(processed_df, batch_id)
                
                self.logger.info(f"배치 {batch_id} 처리 완료")
                
            except Exception as e:
                self.logger.error(f"배치 {batch_id} 처리 중 오류: {e}")
                raise
        
        # 스트림 쿼리 시작
        query = df.writeStream \
            .outputMode(self.config['output_mode']) \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", f"{self.config['checkpoint_location']}/main") \
            .trigger(processingTime=self.config['trigger_interval']) \
            .start()
        
        return query
    
    def _save_to_redis(self, df: DataFrame, batch_id: int):
        try:
            rows = df.collect()

            if not rows:
                self.logger.info(f"배치 {batch_id}: 저장할 데이터 없음")
                return

            # Redis 연결
            redis_client = redis.Redis(
                host=self.config['redis_host'],
                port=self.config['redis_port'],
                db=self.config['redis_db'],
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=30
            )

            pipeline = redis_client.pipeline()
            count = 0

            for row in rows:
                try:
                    # 사용자 행동 데이터 저장
                    _store_user_behavior(pipeline, row)

                    # 실시간 피처 저장
                    _store_realtime_features(pipeline, row)

                    # 세션 데이터 저장
                    _store_session_data(pipeline, row)

                    count += 1

                    # 배치 실행 (100개씩)
                    if count % 100 == 0:
                        pipeline.execute()
                        pipeline = redis_client.pipeline()

                except Exception as e:
                    self.logger.error(f"Redis 저장 중 오류: {e}")

            # 남은 데이터 실행
            if count % 100 != 0:
                pipeline.execute()

            self.logger.info(f"배치 {batch_id}: {count}개 레코드 Redis 저장 완료")

        except Exception as e:
            self.logger.error(f"배치 {batch_id} Redis 저장 실패: {e}")
            raise

    
    def start_streaming(self, duration_seconds: Optional[int] = None) -> StreamingQuery:
        try:
            self.logger.info("스트리밍 처리 시작")
            
            # Kafka 스트림 생성
            kafka_stream = self.create_kafka_stream()
            
            # 처리 및 Redis 저장 시작
            query = self.process_and_store_to_redis(kafka_stream)
            
            # 활성 스트림 추가
            self.active_streams['main'] = query
            
            if duration_seconds:
                self.logger.info(f"{duration_seconds}초간 실행 후 종료")
                query.awaitTermination(duration_seconds)
            else:
                self.logger.info("무한 스트리밍 모드로 실행")
                query.awaitTermination()
            
            return query
            
        except Exception as e:
            self.logger.error(f"스트리밍 시작 실패: {e}")
            raise
    
    def stop_all_streams(self):
        for name, query in self.active_streams.items():
            try:
                self.logger.info(f"스트림 '{name}' 정지 중...")
                query.stop()
            except Exception as e:
                self.logger.error(f"스트림 '{name}' 정지 중 오류: {e}")
        
        self.active_streams.clear()
    
    def get_stream_status(self) -> Dict[str, Any]:
        status = {}
        for name, query in self.active_streams.items():
            status[name] = {
                'id': query.id,
                'name': query.name,
                'is_active': query.isActive,
                'last_progress': query.lastProgress
            }
        return status
    
    def cleanup(self):
        self.stop_all_streams()
        if hasattr(self, 'spark'):
            self.spark.stop()
        if hasattr(self, 'redis_client'):
            self.redis_client.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka → Spark → Redis 스트림 처리')
    parser.add_argument('--duration', type=int, help='실행 시간 (초)')
    parser.add_argument('--config', type=str, help='설정 파일 경로')
    
    args = parser.parse_args()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = None
    try:
        processor = StreamProcessor()
        processor.start_streaming(duration_seconds=args.duration)
    except KeyboardInterrupt:
        logging.info("사용자에 의해 중단됨")
    except Exception as e:
        logging.error(f"실행 중 오류: {e}")
        sys.exit(1)
    finally:
        if processor:
            processor.cleanup()


if __name__ == "__main__":
    main()
