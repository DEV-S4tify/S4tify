import requests


from scripts.get_access_token import get_token
from airflow.models import Variable

# Spotify API 설정
SPOTIFY_API_URL = "https://api.spotify.com/v1"
SPOTIFY_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)

# Spotify API에서 아티스트 ID 검색
def search_artist_id(artist_name):
    SPOTIFY_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)

    url = f"{SPOTIFY_API_URL}/search"
    headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}
    params = {"q": artist_name, "type": "artist", "limit": 1}
    response = requests.get(url, headers=headers, params=params)

    print("headers : ", headers)

    if response.status_code == 200:
        artists = response.json().get("artists", {}).get("items", [])
        artist_id = artists[0]["id"] if artists else None
        print(f"🔍 검색된 아티스트: {artist_name} -> ID: {artist_id}")
        return artist_id
    else:
        print(
            f"❌ 아티스트 검색 실패: {artist_name}, 상태 코드: {response.status_code}, 응답: {response.json()}"
        )
    return None


# Spotify API에서 아티스트 장르 가져오기
def get_artist_genre(artist_id):
    if not artist_id:
        return "Unknown"

    SPOTIFY_TOKEN = Variable.get("SPOTIFY_ACCESS_TOKEN", default_var=None)

    url = f"{SPOTIFY_API_URL}/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {SPOTIFY_TOKEN}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        genres = response.json().get("genres", [])
        genre_str = ", ".join(genres) if genres else "Unknown"
        print(f"🎵 장르 검색: ID {artist_id} -> {genre_str}")
        return genre_str
    else:
        print(
            f"❌ 장르 검색 실패: ID {artist_id}, 상태 코드: {response.status_code}, 응답: {response.json()}"
        )
    return "Unknown"