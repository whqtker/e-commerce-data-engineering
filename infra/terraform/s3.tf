# Spark 애플리케이션을 위한 IAM 사용자
resource "aws_iam_user" "spark_app_user" {
  name = "spark-s3-data-lake-user"
  tags = {
    Project = "DE-Portfolio-Project"
  }
}

# S3 버킷 이름에 사용할 랜덤 문자열
resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

resource "aws_iam_policy" "s3_data_lake_access_policy" {
  name        = "S3DataLakeAccessPolicy"
  description = "Allows Spark job to write to the data lake S3 bucket"

  # IAM 정책
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject", # 객체 생성/덮어쓰기
          "s3:GetObject", # 객체 읽기
          "s3:ListBucket", # 버킷 내 객체 목록 조회
          "s3:DeleteObject" # 임시 파일 삭제 등
        ],
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}

# IAM 사용자와 정책 부착
resource "aws_iam_policy_attachment" "spark_user_policy_attachment" {
  name       = "spark-user-s3-policy-attachment"
  users      = [aws_iam_user.spark_app_user.name]
  policy_arn = aws_iam_policy.s3_data_lake_access_policy.arn
}

resource "aws_iam_access_key" "spark_app_user_key" {
  user = aws_iam_user.spark_app_user.name
}

output "spark_app_user_access_key_id" {
  value     = aws_iam_access_key.spark_app_user_key.id
  sensitive = true
}

output "spark_app_user_secret_access_key" {
  value     = aws_iam_access_key.spark_app_user_key.secret
  sensitive = true
}

# S3
resource "aws_s3_bucket" "data_lake" {
  bucket = "e-commerce-data-lake-${random_string.bucket_suffix.result}"

  # 버킷 보호 기능
  force_destroy = false

  tags = {
    Name        = "ECommerceDataLake"
    Project     = "DE-Portfolio-Project"
    ManagedBy   = "Terraform"
  }
}

# S3 버킷 버전 관리 설정
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 버킷 수명 주기 정책 설정
resource "aws_s3_bucket_lifecycle_configuration" "data_lake_lifecycle" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id      = "log-transition-rule"
    status  = "Enabled"

    # 30일이 지난 데이터는 Standard-IA 클래스로 이동
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # 90일이 지난 데이터는 Glacier Instant Retrieval 클래스로 이동
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }

    # 오래된 버전의 객체는 180일 후에 영구 삭제
    noncurrent_version_expiration {
      noncurrent_days = 180
    }
  }
}

output "data_lake_bucket_name" {
  value       = aws_s3_bucket.data_lake.bucket
  description = "The name of the created S3 data lake bucket."
}

resource "local_file" "env_file" {
  filename = "${path.module}/../../.env"

  content = <<-EOT
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPICS=user-behavior-events

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
# REDIS_PASSWORD=

# Spark Configuration
CHECKPOINT_LOCATION=./checkpoints

# Data Producer Configuration
EVENTS_PER_SECOND=100
DATA_BATCH_SIZE=10

# --- Terraform apply에 의해 자동으로 생성된 값들 ---
AWS_ACCESS_KEY_ID=${aws_iam_access_key.spark_app_user_key.id}
AWS_SECRET_ACCESS_KEY=${aws_iam_access_key.spark_app_user_key.secret}
S3_DATALAKE_PATH=s3a://${aws_s3_bucket.data_lake.bucket}/events
EOT
}
