from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time, os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv("C://Users/Yeojun/.env")
from tqdm import tqdm

def get_weekly_top200_songs(countries, year=3):

    dates = []

    current_date = datetime.now() - timedelta(days=5)
    end_date = current_date - timedelta(days=(year * 365))
    while current_date >= end_date:
        date_string = current_date.strftime("%Y-%m-%d")
        dates.append(date_string)
        current_date -= timedelta(weeks=1)


    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    with webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options) as driver:
        driver.get("https://charts.spotify.com/charts/view/regional-global-weekly/latest")
        time.sleep(2)

        login_button = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/header/div/div[2]/a/span[1]')
        login_button.click()

        id_input = driver.find_element(By.XPATH, '//*[@id="login-username"]').send_keys(os.getenv("SPOTIFY_ID"))
        password_input = driver.find_element(By.XPATH, '//*[@id="login-password"]').send_keys(os.getenv("SPOTIFY_PASS"))
        login_button = driver.find_element(By.XPATH, '//*[@id="login-button"]')
        login_button.click()
        time.sleep(3)

        for country in tqdm(countries):
            for date in dates:
                driver.get(f"https://charts.spotify.com/charts/view/regional-{country}-weekly/{date}")

                time.sleep(2)

                csv_download_button = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div[3]/div/div/div[2]/span')
                csv_download_button.click()

                time.sleep(2)

countries = ['global','kr','us']
get_weekly_top200_songs(countries)