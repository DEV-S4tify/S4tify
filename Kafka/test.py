import os
import sys
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, ".."))

from Kafka.utils.docker_utils import register_sink_connector
from Kafka.utils.connect_utils import create_s3_sink_json

create_s3_sink_json()
register_sink_connector(f"{BASE_DIR}/connectors/s3_sink_config.json")


producer_script = os.path.join(BASE_DIR, "Kafka/" "eventsim_producer.py")
subprocess.run(["python", producer_script], check=True)