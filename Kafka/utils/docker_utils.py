import json
import subprocess
import requests

def get_container_id(container_name: str) -> str:
    """지정된 컨테이너 이름에 해당하는 컨테이너 ID를 가져오는 함수"""
    try:
        result = subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={container_name}"],
            stdout=subprocess.PIPE, text=True, check=True
        )
        container_id = result.stdout.strip()
        return container_id if container_id else None
    except subprocess.CalledProcessError:
        return None
    
def is_container_running(container_name: str) -> bool:
    """컨테이너 실행 여부 확인"""
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
            stdout=subprocess.PIPE, text=True, check=True
        )
        return result.stdout.strip().lower() == "true"
    except subprocess.CalledProcessError:
        return False
    
def register_sink_connector(path:str):
    url = "http://localhost:8083/connectors"
    headers = {"Content-Type": "application/json"}
    with open(path, "r", encoding="utf-8") as file:
        payload = json.load(file)

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code in [200, 201]:  # 200: OK, 201: Created
        print(f"S3 Sink Connector 등록 완료: {response.json()}")
    else:
        print(f"S3 Sink Connector 등록 실패: {response.status_code}")
        print(response.text)