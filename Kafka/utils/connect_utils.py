import json
import os

from Kafka.variables.aws_variables import aws_variables


def create_s3_sink_json():
    # 현재 파일(= connect_utils.py) 위치
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # connectors 폴더 경로
    connectors_dir = os.path.join(base_dir, "..", "connectors")
    os.makedirs(connectors_dir, exist_ok=True)  # 폴더가 없으면 생성

    # 최종 파일 경로
    file_path = os.path.join(connectors_dir, "s3_sink_config.json")

    config = {
        "name": "eventsim-s3-sink-connector",
        "config": {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "3",
            "topics": "eventsim_music_streaming",
            "topic.creation.enable": "true",
            "topic.creation.default.partitions": "4",
            "topic.creation.default.replication.factor": "1",
            "topic.creation.default.cleanup.policy": "delete",
            "s3.region": "ap-northeast-2",
            "s3.bucket.name": "de5-s4tify",
            "s3.part.size": "5242880",
            "flush.size": "1000",
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "locale": "ko_KR",
            "timezone": "Asia/Seoul",
            "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
            "schema.compatibility": "NONE",
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
            "timestamp.extractor": "RecordField",
            "timestamp.field": "ts",
            "partition.duration.ms": "86400000",
            "aws.access.key.id": aws_variables.get("aws_access_key_id"),
            "aws.secret.access.key": aws_variables.get("aws_secret_access_key"),
        },
    }

    with open(file_path, "w") as f:
        json.dump(config, f, indent=4)
