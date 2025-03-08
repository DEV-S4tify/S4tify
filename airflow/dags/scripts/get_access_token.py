import base64
import json
import os

import requests

from airflow.models import Variable

SPOTIFY_CLIENT_ID = Variable.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = Variable.get("SPOTIFY_CLIENT_SECRET")
END_POINT = "https://accounts.spotify.com/api/token"


def get_token():

    encoded = base64.b64encode(
        f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode("utf-8")
    ).decode("ascii")

    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = {"grant_type": "client_credentials"}

    response = requests.post(END_POINT, data=payload, headers=headers)

    if response.status_code == 200:
        access_token = response.json()["access_token"]

        # airflow 변수 갱신
        Variable.set("SPOTIFY_ACCESS_TOKEN", access_token)

        print("success to change the access token!")

    else:
        print(f"Error {response.status_code}: {response.text}")
