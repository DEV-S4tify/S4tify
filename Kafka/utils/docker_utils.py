import subprocess

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