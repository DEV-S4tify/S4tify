from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import NewTopic
import subprocess
import json


def create_topic(bootstrap_servers, name, partitions, replica=1):
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        topic=NewTopic(
            name=name,
            num_partitions=partitions,
            replication_factor=replica)
        client.create_topics([topic])
        print(f"✅ Kafka topic '{name}' created successfully!")
    except TopicAlreadyExistsError:
        print(f"⚠️ Topic '{name}' already exists, skipping creation.")
    except Exception as e:
        print(f"❌ Failed to create topic: {e}")
    finally:
        client.close()
    

def main():
    topic_name = "fake_user"
    bootstrap_servers = ["localhost:9092"]

    ## topic 생성
    create_topic(bootstrap_servers, topic_name, partitions=4)

    ## producer 생성
    producer= KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )

    ## Docker 컨테이너 실행 및 컨테이너 ID 저장
    process = subprocess.Popen(
        ["docker", "run", "-d", "--rm", "--name", "eventsim_container", "eventsim",
        "-c", "examples/example-config.json",
        "--from", "15",
        "--nusers", "30000",
        "--growth-rate", "0.30"],
        stdout=subprocess.PIPE,
        text=True
    )

    ## 실행된 컨테이너 ID 가져오기
    container_id = process.stdout.read().strip()

    ## EventSim 로그 읽어서 Kafka 전송
    try:
        logs_process = subprocess.Popen(
            ["docker", "logs", "-f", container_id],
            stdout=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )

        for cnt,line in enumerate(logs_process.stdout):
            if cnt >= 10000:
                print("\n⏹️ Reached max event count. Stopping...")
                break

            try:         
                event = json.loads(line.strip())
                producer.send(topic_name, event)
                print(f"📩 Sent to Kafka: {event}")
            except json.JSONDecodeError:
                print(f"⚠️ Invalid JSON format: {line.strip()}")

    except KeyboardInterrupt:
        print("\n⏹️ Stopping event streaming manually...")

    ## Kafka Producer 종료
    print("🔄 Flushing and closing Kafka Producer...")
    producer.flush()
    producer.close()

    ## Docker 컨테이너 종료
    print(f"🛑 Stopping Docker container: {container_id}")
    subprocess.run(["docker", "stop", container_id])
    
    print("EventSim data streaming to Kafka completed.")

if __name__ == '__main__':
    main()