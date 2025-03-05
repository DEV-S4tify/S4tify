import io
import json

import requests
from avro.io import BinaryEncoder, DatumWriter


# Avro 직렬화 함수
def serialize_avro(data, schema):
    """Avro 데이터를 직렬화하여 바이너리 포맷으로 변환"""
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()


# Avro 스키마 등록 (Schema Registry에 POST 요청)
def register_schema(SCHEMA_REGISTRY_URL: str, subject: str, schema_dict: dict):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload = {"schema": json.dumps(schema_dict)}

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200 or response.status_code == 201:
        print(f"스키마 등록 완료: {subject}")
        return response.json()
    else:
        print(f"스키마 등록 실패: {response.text}")
        return None
