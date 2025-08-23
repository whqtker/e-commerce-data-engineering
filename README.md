## 디렉터리 구조

```
e-commerce-de/
├── .github/
│   └── workflows/
│       └── ci.yml
├── .gitignore
├── README.md
├── requirements.txt
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
