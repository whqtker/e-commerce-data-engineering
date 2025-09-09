FROM python:3.11-slim

# Java 설치 (사용 가능한 버전으로 변경)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Java 환경 변수 설정 (버전에 맞게 경로 수정)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

RUN mkdir -p /opt/spark/jars && \
    wget -q -O /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -O /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]