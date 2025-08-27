## 디렉터리 구조

```
e-commerce-de/
├── .github/
│   └── workflows/
│       └── ci.yml
├── .gitignore
├── README.md
|
├── data_producer/ # 실시간 로그 생성기
│   ├── __init__.py
│   ├── producer.py # Kafka로 데이터를 전송하는 스크립트 w/ Faker
│   └── config.py # Kafka 설정
│
├── stream_processor/ # Spark Streaming
│   ├── __init__.py
│   ├── process_stream.py # Kafka 데이터를 읽어 처리, Redis/S3에 저장하는 Spark 작업
│   └── jobs/
│       └── feature_engineering.py # 스트림 데이터 피처 엔지니어링 로직
│
├── batch_processor/ # 배치 처리 및 모델 학습 w/ Airflow/Spark
│   ├── dags/ # Airflow DAG 파일
│   │   └── train_recommendation_model_dag.py
│   ├── jobs/ # Airflow가 실행할 Spark 배치 작업 스크립트
│   │   └── model_trainer.py
│   └── sql/ # 배치 작업에 사용될 SQL 쿼리
│       └── batch.sql
│
├── api_server/ # FastAPI 서버
│   ├── __init__.py
│   ├── main.py # API 엔드포인트 정의
│   ├── models.py # Pydantic 모델
│   ├── Dockerfile
│   └── core/
│       └── recommendation_logic.py # 학습된 모델과 Redis를 사용한 추천 로직
│
├── infra/
│   ├── docker-compose.yml
│   └── terraform/ # AWS S3, EKS, ...
│       └── main.tf
│
└── notebooks/# 데이터 탐색 및 분석용
    └── 01_explore_user_logs.ipynb
```

## version

python : 3.9.x
hadoop : 3.3.6
spark : 3.5.6

## how to start

### 개발 환경 세팅

hadoop 3.3.6 설치
hadoop.ddl, winutils.exe

환경 변수 설정

py -3.9 -m venv .venv

source .venv/Scripts/activate

pip install poetry

poetry install

### produce

python -m data_producer.producer --mode sample --count 10
python -m data_producer.producer --mode stream --duration 60

### consume

python -m stream_processor.process_stream --duration 60

### Airflow DAG 트리거
Airflow UI 접속: http://localhost:8081

Admin → Connections 메뉴

"+" 버튼으로 새 Connection 생성:

Connection Id: spark_default

Connection Type: Spark

Host: spark-master

Port: 7077

Extra: {"queue": "default"}
docker exec airflow-scheduler airflow dags trigger train_recommendation_model_dag

---

last updated: 25.08.25

## ETL

### Extract

`data_producer/producer.py`에서 `Faker` 라이브러리를 통해 샘플 데이터를 produce한다.

- 미리 정해진 수의 사용자와 상품 정보를 만들고 이 안에서 무작위로 선택하여 이벤트를 생성한다.
- 실제 쇼핑몰처럼 구매보다 단순 조회가 훨씬 많이 일어나도록 설정했다.
- 일정 확률로 세션을 새로 생성하여 세션 만료 상황을 시뮬레이션한다.
- 새벽 시간에는 데이터 발생량을 줄이고 저녁 피크 타임에 늘리는 등 실제 트래픽 패턴을 모방한다.
- 이후 생성된 데이터는 JSON 형식으로 직렬화되어 카프카로 전송된다.

### Transform

`stream_processor/process_stream.py` 에서 데이터를 consume한다.

- 스파크가 카프카의 `user-behavior-events` 토픽을 구독하도록 한다.
- 카프카로부터 받은 데이터는 바이너리 형태이므로 정의된 스키마를 통해 JSON을 파싱하고 각 필드를 컬럼으로 분리한다.
- 워터마크를 사용하여 특정 시간만큼 지연된 데이터까지 정상적으로 처리할 수 있도록 한다.
- 스파크 스트리밍은 정해진 간격마다 들어온 데이터를 마이크로 배치로 묶어 처리한다.

---

`stream_processor/jobs/feature_engineering.py` 은 주어진 원본 데이터를 의미 있는 데이터로 가공한다.

- 원본 컬럼을 가공하여 피처를 생성한다. 예를 들어 숫자형인 가격을 `low`, `medium`, `high` 와 같은 범주형 데이터로 변환한다. 시간 컬럼을 `hour_of_day`, `day_of_week`, `is_weekend` 로 분해한다.
- 윈도우 함수를 사용하여 고차원적인 피처를 생성한다. 예를 들어 특정 사용자가 한 번 방문 시 시간 순으로 행동의 순서, 세션 내 총 행동 수 등을 계산한다.
- 생성된 피처들을 조합하여 사용자를 분류한다. 예를 들어 행동 패턴에 따라 `browser`, `quick_buyer` 등으로 사용자를 분류한다.

### Load

`stream_processor/process_stream.py` 에서 피처들을 레디스에 저장한다. 데이터를 드라이버로 모으지 않고 각 워커 노드에서 직접 레디스로 저장한다.

- 워커 노드에 분산되어 있던 마이크로 배치 데이터들을 드라이버 메모리로 가져온 후 레디스 파이프라인을 통해 레디스로 보낼 명령어들을 담는다. 레디스 파이프라인을 통해 여러 명령어들을 한 번의 네트워크 통신으로 처리할 수 있으며, 네트워크 오버헤드를 줄일 수 있다.
- `List`, `Hash`, `Sorted Set`과 같은 다양한 자료구조를 통해 세션 요약 정보를 저장한다.
- `TTL`을 설정하여 만료된 데이터는 자동으로 삭제하도록 한다.
