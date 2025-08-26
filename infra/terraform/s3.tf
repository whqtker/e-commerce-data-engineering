# S3 버킷 이름에 사용할 랜덤 문자열
resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
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
