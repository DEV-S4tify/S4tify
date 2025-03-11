import os
import sys

from Kafka.utils.connect_utils import create_s3_sink_json
from Kafka.utils.docker_utils import register_sink_connector

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, ".."))


create_s3_sink_json()
register_sink_connector(f"{BASE_DIR}/connectors/s3_sink_config.json")
