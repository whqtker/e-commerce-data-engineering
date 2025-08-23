import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging


@dataclass # 데이터를 저장하는 목적의 클래스를 만들 때 반복적으로 작성해야 하는 기본적인 메서드를 자동으로 생성
class KafkaConfig:

    # Kafka 브로커 설정
    bootstrap_servers: List[str] = field(
        default_factory=lambda: os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ).split(",")
    )
    
    # 토픽 설정
    topic_name: str = os.getenv("KAFKA_TOPIC", "user-behavior-events")
    
    # Producer 설정
    producer_config: Dict = field(default_factory=lambda: {
        'acks': 'all',  # 모든 replica에서 확인받을 때까지 대기
        'retries': 5,   # 재시도 횟수
        'batch_size': int(os.getenv("KAFKA_BATCH_SIZE", "16384")),
        'linger_ms': int(os.getenv("KAFKA_LINGER_MS", "10")),  # 배치 대기 시간
        'buffer_memory': int(os.getenv("KAFKA_BUFFER_MEMORY", "33554432")),
        'compression_type': os.getenv("KAFKA_COMPRESSION", "snappy"), # 메시지를 보내기 전 어떤 알고리즘으로 압축할지
        'max_in_flight_requests_per_connection': 5, # 프로듀서가 브로커와의 단일 연결에서 응답을 받지 않고 동시에 보낼 수 있는 최대 요청의 수
        'enable_idempotence': True,  # 중복 메시지 방지
    })
    
    # 프로덕션 용 보안 설정
    # SASL: 인터넷 프로토콜에서 인증 및 보안을 위한 프레임워크
    security_protocol: Optional[str] = os.getenv("KAFKA_SECURITY_PROTOCOL")
    sasl_mechanism: Optional[str] = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username: Optional[str] = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password: Optional[str] = os.getenv("KAFKA_SASL_PASSWORD")
    
    def get_full_config(self) -> Dict:
        config = {
            'bootstrap_servers': self.bootstrap_servers,
            **self.producer_config
        }
        
        # 보안 설정이 되었다면 추가
        if self.security_protocol:
            config.update({
                'security_protocol': self.security_protocol,
                'sasl_mechanism': self.sasl_mechanism,
                'sasl_plain_username': self.sasl_username,
                'sasl_plain_password': self.sasl_password,
            })
        
        return config


@dataclass
class DataConfig:
    
    user_pool_size: int = int(os.getenv("USER_POOL_SIZE", "10000"))
    product_pool_size: int = int(os.getenv("PRODUCT_POOL_SIZE", "5000"))
    
    # 행동 타입별 가중치
    action_weights: Dict[str, float] = field(default_factory=lambda: {
        'view': 0.60, # 상품 조회
        'click': 0.25, # 상품 클릭
        'add_to_cart': 0.12, # 장바구니 추가
        'purchase': 0.03 # 실제 구매
    })
    
    # 데이터 생성 주기 설정
    events_per_second: int = int(os.getenv("EVENTS_PER_SECOND", "100"))
    batch_size: int = int(os.getenv("DATA_BATCH_SIZE", "10"))
    
    # 시간대별 트래픽 패턴 가중치
    hourly_traffic_multiplier: Dict[int, float] = field(default_factory=lambda: {
        0: 0.3, 1: 0.2, 2: 0.15, 3: 0.1, 4: 0.1, 5: 0.15,
        6: 0.3, 7: 0.5, 8: 0.7, 9: 0.8, 10: 0.9, 11: 1.0,
        12: 1.1, 13: 1.0, 14: 0.9, 15: 0.8, 16: 0.9, 17: 1.2,
        18: 1.4, 19: 1.3, 20: 1.5, 21: 1.4, 22: 1.0, 23: 0.6
    })
    
    # 상품 카테고리별 설정
    product_categories: List[str] = field(default_factory=lambda: [
        'Electronics', 'Fashion', 'Home & Garden', 'Books', 'Sports',
        'Beauty', 'Toys', 'Food & Beverage', 'Automotive', 'Health'
    ])
    
    # 로깅 설정
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    def setup_logging(self):
        logging.basicConfig(
            level=getattr(logging, self.log_level.upper()),
            format=self.log_format,
            handlers=[
                logging.StreamHandler(), # 로그를 콘솔로 보냄
                            logging.FileHandler('data_producer.log', encoding='utf-8') # 로그를 명시된 파일로 저장
            ]
        )


# 설정 인스턴스 생성
kafka_config = KafkaConfig()
data_config = DataConfig()

# 로깅 설정 초기화
data_config.setup_logging()
