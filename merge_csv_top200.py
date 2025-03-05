
import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime, timedelta

def merge_csv(source, destination):
    # 통합 csv dataframe 생성
    header = ['rank','track_id','artist_names','track_name','source','peak_rank','previous_rank','weeks_on_chart','streams', 'country_code','date']
    combined_df = pd.DataFrame(columns=header)

    # 파일명을 기반으로 국가명과 일자 정보를 추출하여 데이터에 추가
    directory = source
    for filename in tqdm(os.listdir(directory)):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            _, country, _, year, month, day = filename.split('-')

            # 기존 데이터 로드
            df = pd.read_csv(file_path,encoding='utf-8')

            # uri에서 track_id만 추출하여 컬럼명 변경하여 저장       
            df['uri'] = df['uri'].str.split(':').str[-1]
            df.rename(columns={'uri': 'track_id'}, inplace=True)

            # 국가명과 일자 정보 추가 
            df['country_code'] = country
            df['date'] = datetime(int(year), int(month), int(day.split('.')[0]))
            
            # DataFrame을 통합
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            print(filename)
            
    # 수정된 데이터를 새로운 파일로 저장
    combined_df.to_csv(destination, index=False,encoding='utf-8-sig')

directory = 'C:/Users/Yeojun/s4tify/weekly_Top_Songs/'
new_filename = "weekly_top200_combined.csv"

source = 'C:/Users/Yeojun/Downloads/'
destination = os.path.join(directory, new_filename)

merge_csv(source, destination)
