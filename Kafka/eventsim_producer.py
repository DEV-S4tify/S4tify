import os
import sys
import json
import time
import subprocess
from typing import Final

import avro.schema
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaAdminClient
from kafka.producer import KafkaProducer

# Kafka 패키지가 있는 경로 추가
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, ".."))

from Kafka.model.music_streaming import EventLog
from Kafka.utils.docker_utils import get_container_id, is_container_running
from Kafka.utils.schema_utils import register_schema, serialize_avro

SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Avro 스키마 로드
SCHEMA_PATH = os.path.join(BASE_DIR, "schemas", "music_streaming_schema.avsc")
with open(SCHEMA_PATH, "r", encoding='utf-8') as schema_file:
    schema_dict = json.load(schema_file)


def create_topic(bootstrap_servers, name, partitions, replica=1):
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        topic = NewTopic(
            name = name,
            num_partitions = partitions,
            replication_factor = replica)
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        print(e)
        pass
    finally:
        client.close()

def stream_docker_logs(container_name: str, producer: KafkaProducer, topic_name: str):
    """컨테이너 로그를 Kafka로 전송 (컨테이너 종료 감지)"""
    container_id = get_container_id(container_name)
    if not container_id:
        raise RuntimeError(f"{container_name} 컨테이너가 실행 중이 아닙니다.")

    # Docker logs 프로세스 시작
    process = subprocess.Popen(
        ["docker", "logs", "-f", container_id],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8'
    )

    try:
        while True: # sh script daemon으로 관리
            line = process.stdout.readline().strip()
            if not line:
                if is_container_running(container_name):
                    time.sleep(1)
                    continue  # 빈 줄이면 무시
                else:
                    print('Eventserver down')
                    break
                
            # JSON 로그만 필터링
            if line.startswith("{") and line.endswith("}"):
                try:
                    log_data = json.loads(line)  # JSON 파싱
                    event = EventLog(**log_data)  # Pydantic 검증 및 변환
                    value = event.model_dump_json().encode('utf-8')

                    producer.send(topic_name, key=str(event.ts), value=value)
                    producer.flush()
                except (json.JSONDecodeError, ValueError) as e:
                    print(f" JSON 변환 오류: {e}")  # 잘못된 JSON 무시
    except KeyboardInterrupt:
        pass
    finally:
        process.kill()  # 서브프로세스 종료
        producer.close()
        print("Kafka Producer 종료")


def main():
    topic_name: Final = 'eventsim_music_streaming'
    container_name: Final = 'eventsim_container'
    bootstrap_servers = ['localhost:9092']

    create_topic(bootstrap_servers, topic_name, 4)
    register_schema(SCHEMA_REGISTRY_URL, f'{topic_name}-value', schema_dict)

    schema = avro.schema.parse(open(SCHEMA_PATH).read())

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id = "eventsim_music_streaming_producer",
        key_serializer=lambda k: k.encode('utf-8') if k else None, 
        value_serializer=lambda v: v
    )

    stream_docker_logs(container_name, producer, topic_name)

if __name__ == "__main__":
    main()
