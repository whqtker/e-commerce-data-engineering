import json
import time
import random
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Generator
from dataclasses import dataclass
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

from .config import kafka_config, data_config


@dataclass
class UserBehaviorEvent:
    """
    사용자의 단일 행동을 표현한다.
    """
    user_id: str
    product_id: str
    action_type: str
    timestamp: str
    session_id: str
    product_category: str
    product_price: float
    user_agent: str # 사용자 브라우저 또는 기기 정보
    ip_address: str
    referrer: Optional[str] = None # 사용자가 유입된 경로

    # 객체를 딕셔너리로 변환
    def to_dict(self) -> Dict:
        return {
            'user_id': self.user_id,
            'product_id': self.product_id,
            'action_type': self.action_type,
            'timestamp': self.timestamp,
            'session_id': self.session_id,
            'product_category': self.product_category,
            'product_price': self.product_price,
            'user_agent': self.user_agent,
            'ip_address': self.ip_address,
            'referrer': self.referrer
        }


class UserBehaviorProducer:
    """
    UserBehaviorEvent를 생성하고 Kafka로 전송하는 프로듀서를 정의한다.
    """
    
    def __init__(self):
        self.faker = Faker(['en_US', 'ko_KR'])
        self.logger = logging.getLogger(__name__)
        self.producer: Optional[KafkaProducer] = None
        self.is_running = False
        self.stats = {
            'total_sent': 0,
            'errors': 0,
            'start_time': None
        }
        
        # 사용자 및 상품 풀 생성
        self.user_pool = self._generate_user_pool()
        self.product_pool = self._generate_product_pool()
        
        # 세션을 관리하는 딕셔너리
        # key: user_id, value: session_id
        self.user_sessions: Dict[str, str] = {}
        
        # Graceful shutdown을 위한 시그널 핸들러
        # 명시된 시그널이 발생하면 명시된 핸들러가 수행된다.
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    # 사용자 풀 생성
    def _generate_user_pool(self) -> List[str]:
        self.logger.info(f"사용자 풀 생성 중... (크기: {data_config.user_pool_size})")
        return [f"user_{i:06d}" for i in range(1, data_config.user_pool_size + 1)]
    
    # 상품 정보 풀 생성
    def _generate_product_pool(self) -> List[Dict]:
        self.logger.info(f"상품 풀 생성 중... (크기: {data_config.product_pool_size})")
        products = []
        for i in range(1, data_config.product_pool_size + 1):
            product = {
                'id': f"product_{i:06d}",
                'category': random.choice(data_config.product_categories),
                'price': round(random.uniform(10.0, 1000.0), 2) # uniform(a, b): a<=x<=b를 만족하는 실수 x를 리턴
            }
            products.append(product)
        return products
    
    # 사용자 ID가 존재하는 세션 ID 리턴
    def _get_session_id(self, user_id: str) -> str:
        # 사용자가 세션에 없다면 새로 생성
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = self.faker.uuid4()
        
        # 세션 만료 상황을 시뮬레이션한다. 5% 확률로 세션을 새로 생성한다.
        if random.random() < 0.05:
            self.user_sessions[user_id] = self.faker.uuid4()
        
        return self.user_sessions[user_id]
    
    # 현재 시간대별 트래픽 가중치 적용
    def _apply_hourly_multiplier(self) -> float:
        # 현재 시간을 확인하여 해당하는 가중치를 리턴한다.
        current_hour = datetime.now().hour
        return data_config.hourly_traffic_multiplier.get(current_hour, 1.0) # 만약 해당 시간이 없는 경우 1.0(가중치 없음) 리턴
    
    # 단일 UserBehaviorEvent 생성
    def _generate_event(self) -> UserBehaviorEvent:
        user_id = random.choice(self.user_pool)
        product = random.choice(self.product_pool)
        
        # 가중치를 적용하여 사용자의 행동 결정
        action_type = random.choices(
            list(data_config.action_weights.keys()),
            weights=list(data_config.action_weights.values())
        )[0]
        
        # 현실적인 사용자 행동 시뮬레이션
        # (예: 같은 사용자가 연속으로 같은 상품에 대해 view -> click -> add_to_cart 패턴)
        if action_type in ['click', 'add_to_cart', 'purchase']:
            # 이전 행동이 있을 가능성이 높은 상품 선택 (실제로는 더 복잡한 로직 필요)
            pass
        
        event = UserBehaviorEvent(
            user_id=user_id,
            product_id=product['id'],
            action_type=action_type,
            timestamp=datetime.now(timezone.utc).isoformat(),
            session_id=self._get_session_id(user_id),
            product_category=product['category'],
            product_price=product['price'],
            user_agent=self.faker.user_agent(),
            ip_address=self.faker.ipv4(),
            referrer=random.choice([
                None,
                'https://google.com',
                'https://facebook.com',
                'https://instagram.com',
                'direct'
            ])
        )
        
        return event
    
    def _setup_producer(self) -> KafkaProducer:
        try:
            # 데이터 직렬화 설정 및 외부 설정 주입
            # 카프카는 바이트 형태의 데이터만 주고받을 수 있음
            producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'), # 딕셔너리를 JSON으로 변환 후 인코딩
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                **kafka_config.get_full_config()
            )
            
            self.logger.info("Kafka Producer 생성 성공")
            return producer
            
        except Exception as e:
            self.logger.error(f"Kafka Producer 생성 실패: {e}")
            raise

    def _delivery_callback(self, record_metadata):
        """
        메시지 전송 결과에 대한 콜백함수이다.
        kafka-python의 send 메서드는 기본적으로 비동기로 작동한다. 따라서 send 메서드의 리턴 값인 Future 객체에 해당 메서드를
        콜백함수로 등록하면 메시지가 성공적으로 도착하면 콜백함수가 자동으로 수행된다.
        이후 future.add_callback 을 통해 콜백함수를 등록한다.
        """
        if record_metadata:
            self.stats['total_sent'] += 1
            if self.stats['total_sent'] % 100 == 0:
                self._log_stats()
    
    def _log_stats(self):
        if self.stats['start_time']:
            elapsed = time.time() - self.stats['start_time']
            rate = self.stats['total_sent'] / elapsed if elapsed > 0 else 0
            self.logger.info(
                f"통계: 전송={self.stats['total_sent']}, "
                f"오류={self.stats['errors']}, "
                f"속도={rate:.2f} events/sec"
            )
    
    # Graceful shutdown에 대한 시그널 핸들러
    def _signal_handler(self, signum, frame):
        self.logger.info(f"종료 시그널 수신: {signum}")
        self.stop()
        sys.exit(0)
    
    # 배치 단위로 이벤트 생성
    # Generator: 데이터를 미리 만들지 않고 필요할 때마다 하나씩 만들 수 있는 객체
    # yield: return과 유사하나 반환 후 함수를 일시중지함. 다시 호출되면 계속 실행
    def generate_events_batch(self, batch_size: int) -> Generator[UserBehaviorEvent, None, None]:
        for _ in range(batch_size):
            yield self._generate_event()
    
    def start_streaming(self, duration_seconds: Optional[int] = None):
        if self.is_running:
            self.logger.warning("이미 실행 중입니다.")
            return
        
        try:
            self.producer = self._setup_producer()
            self.is_running = True
            self.stats['start_time'] = time.time()
            
            self.logger.info(f"데이터 스트리밍 시작 (목표: {data_config.events_per_second} events/sec)")
            
            end_time = time.time() + duration_seconds if duration_seconds else None

            # 4개의 워커 쓰레드로 이벤트를 병렬 생성 및 전송
            with ThreadPoolExecutor(max_workers=4) as executor:
                while self.is_running:
                    if end_time and time.time() >= end_time:
                        break
                    
                    # 시간대별 가중치 적용
                    hourly_multiplier = self._apply_hourly_multiplier()
                    current_batch_size = int(data_config.batch_size * hourly_multiplier)
                    
                    # 결정된 배치 단위로 이벤트 생성 및 전송
                    events_batch = list(self.generate_events_batch(current_batch_size))
                    
                    # 병렬 전송
                    futures = []
                    for event in events_batch:
                        future = executor.submit(self._send_event, event)
                        futures.append(future)
                    
                    # 결과 대기
                    for future in as_completed(futures, timeout=5):
                        try:
                            future.result()
                        except Exception as e:
                            self.logger.error(f"이벤트 전송 중 오류: {e}")
                    
                    # 속도 조절
                    time.sleep(1.0 / data_config.events_per_second * current_batch_size)
        
        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 중단됨")
        except Exception as e:
            self.logger.error(f"스트리밍 중 오류 발생: {e}")
            raise
        finally:
            self.stop()
    
    def _send_event(self, event: UserBehaviorEvent):
        try:
            # 파티셔닝을 위해 사용자 ID 기반으로 키 설정
            key = event.user_id
            
            future = self.producer.send(
                kafka_config.topic_name,
                key=key,
                value=event.to_dict(),
                headers=[
                    ('event_type', event.action_type.encode('utf-8')),
                    ('category', event.product_category.encode('utf-8'))
                ]
            )
        
            # 성공 콜백 등록
            future.add_callback(self._delivery_callback)
            
            # 에러 콜백 등록
            future.add_errback(self._error_callback)
        
        except Exception as e:
            self.stats['errors'] += 1
            self.logger.error(f"이벤트 전송 실패: {e}")

    def _error_callback(self, exception):
        self.stats['errors'] += 1
        self.logger.error(f'메시지 전송 실패: {exception}')
    
    def stop(self):
        if not self.is_running:
            return
        
        self.logger.info("프로듀서 정지 중...")
        self.is_running = False
        
        if self.producer:
            try:
                # 모든 메시지가 전송될 때까지 대기
                self.producer.flush(timeout=10)
                self.producer.close()
                self.logger.info("Kafka Producer가 정상적으로 종료됨")
            except Exception as e:
                self.logger.error(f"Producer 종료 중 오류: {e}")
        
        # 최종 통계 출력
        self._log_stats()
    
    def send_sample_data(self, count: int = 100):
        self.logger.info(f"{count}개의 샘플 데이터 전송 시작")
        
        try:
            self.producer = self._setup_producer()
            
            for i in range(count):
                event = self._generate_event()
                self._send_event(event)
                
                if (i + 1) % 10 == 0:
                    self.logger.info(f"{i + 1}/{count} 이벤트 전송됨")
            
            # 모든 메시지 전송 완료될 때까지 대기
            self.producer.flush(timeout=30)
            self.logger.info(f"샘플 데이터 전송 완료: {count}개")
            
        except Exception as e:
            self.logger.error(f"샘플 데이터 전송 중 오류: {e}")
            raise
        finally:
            if self.producer:
                self.producer.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='실시간 사용자 행동 데이터 생성기')
    parser.add_argument('--mode', choices=['stream', 'sample'], default='stream',
                      help='실행 모드: stream (연속 스트리밍) 또는 sample (샘플 데이터)')
    parser.add_argument('--duration', type=int, help='스트리밍 지속 시간 (초)')
    parser.add_argument('--count', type=int, default=100, help='샘플 데이터 개수')
    
    args = parser.parse_args()
    
    producer = UserBehaviorProducer()
    
    try:
        if args.mode == 'stream':
            producer.start_streaming(duration_seconds=args.duration)
        elif args.mode == 'sample':
            producer.send_sample_data(count=args.count)
    except Exception as e:
        logging.error(f"실행 중 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
