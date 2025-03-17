import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from scripts.load_spotify_data import *
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

TODAY = datetime.now().strftime("%Y-%m-%d")

FILE_PATH = f"data/spotify_crawling_data{TODAY}.csv"


# 데이터 프레임 생성 + 반환
def make_dataframe():

    columns = ["title", "artist", "artist_id"]

    return pd.DataFrame(columns=columns)


# 크롤링 데이터를 csv로 저장
def save_as_csv_file(df, logical_date):

    dir_path = "crawling_data"
    file_path = f"data/spotify_crawling_data_{TODAY}.csv"

    df.index.name = "rank"
    df.to_csv(file_path, encoding="utf-8", mode="w", header=True, index=True)
    load_s3_bucket(dir_path, f"spotify_crawling_data_{logical_date}.csv")


def data_crawling(logical_date):

    # retry 시, 크롤링은 성공한 상태라면 건너뜀
    if os.path.exists(FILE_PATH):
        print("file already exists")

    else:
        global_top50_df = make_dataframe()

        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Headless 모드 활성화
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        url = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"

        with webdriver.Chrome(service=Service(), options=chrome_options) as driver:

            print("크롤링 시작")

            driver.get(url)
            driver.implicitly_wait(500)

            try:
                # top50 리스트 가져오기
                scroll_element = driver.find_element(
                    By.XPATH,
                    '//*[@id="main"]/div/div[2]/div[5]/div/div[2]/div[2]/div/main/section/div[2]/div[3]/div/div[1]/div/div[2]/div[2]',
                )
                driver.execute_script(
                    """
                                arguments[0].scrollIntoView({behavior: 'smooth', block: 'end'});
                                """,
                    scroll_element,
                )
                # 페이지 로딩 대기
                driver.implicitly_wait(30)
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")

                time.sleep(2)
                song_lists = driver.find_elements(
                    By.XPATH, '//*[@id="main"]//div[@role="row"]'
                )

                print(len(song_lists))

                for i in range(1, len(song_lists)):

                    artist = []
                    artist_id = []

                    # 노래 정보가 있는 큰 div 가져오기
                    track_info = song_lists[i].find_element(
                        By.XPATH,
                        f'//*[@id="main"]/div/div[2]/div[5]/div/div[2]/div[2]/div/main/section/div[2]/div[3]/div/div[1]/div/div[2]/div[2]/div[{i}]/div',
                    )

                    # 진짜 노래 정보가 있는 작은 div 가져오기
                    song_info = track_info.find_element(
                        By.XPATH,
                        f'//*[@id="main"]/div/div[2]/div[5]/div/div[2]/div[2]/div/main/section/div[2]/div[3]/div/div[1]/div/div[2]/div[2]/div[{i}]/div/div[2]',
                    )
                    song_title = song_info.find_element(
                        By.XPATH,
                        f'//*[@id="main"]/div/div[2]/div[5]/div/div[2]/div[2]/div/main/section/div[2]/div[3]/div/div[1]/div/div[2]/div[2]/div[{i}]/div/div[2]/div/div',
                    )

                    global_top50_df.loc[i, "title"] = song_title.text

                    # 아티스트 정보 찾기
                    # 19금 노래의 경우 span[1]에 19금표시, [2]에 아티스트 정보가 있음  -> 클래스 이름은
                    # 같으니까 이걸로 가지고 오기
                    arti_info = song_info.find_element(
                        By.CSS_SELECTOR,
                        ".e-9640-text.encore-text-body-small.encore-internal-color-text-subdued.UudGCx16EmBkuFPllvss.standalone-ellipsis-one-line",
                    )
                    arti_list = arti_info.find_elements(By.TAG_NAME, "a")

                    for arti in arti_list:
                        print(arti.text)
                        artist.append(arti.text)
                        print(arti.get_attribute("href")[-22:])
                        artist_id.append(arti.get_attribute("href")[-22:])

                    global_top50_df.loc[i, "artist"] = artist
                    global_top50_df.loc[i, "artist_id"] = artist_id

            except Exception as e:
                print(f"error: {e}")

        save_as_csv_file(global_top50_df, logical_date)
