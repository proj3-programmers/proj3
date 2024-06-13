import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# API 정보
service_key = '4W+SuygrZKovx1o9XAwMOCPQwob8CEfhXKzhBqlXwO+bGTScWmv+wceo2huz/E1QnlE/23RDy9lN4ClcV09Wxg=='  # 공공데이터포털에서 발급받은 인증키를 입력하세요.
base_url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'

# 지점 번호와 지점명 매핑
stations = {
    '서울': 108,
    '부산': 159,
    '대구': 143,
    '인천': 112,
    '광주': 156,
    '대전': 133,
    '울산': 152,
    '세종': 239,
    '경기': 119,  # 수원
    '강원': 101,  # 춘천
    '충북': 131,  # 청주
    '충남': 129,  # 서산
    '전북': 146,  # 전주
    '전남': 165,  # 목포
    '경북': 138,  # 포항
    '경남': 155,  # 창원
    '제주': 184   # 제주
}

# 사용할 항목들
selected_columns = [
    "stnId",
    "stnNm",
    "tm",
    "avgTa",
    "minTa",
    "maxTa",
    "sumRn",
    "maxInsWs",
    "sumSsHr",
    "ssDur",
    "sumFogDur"
]

# 데이터 수집 및 Redshift에 삽입하는 함수
def collect_and_insert_data(endpoint, start_date, end_date):
    data_list = []
    date_range = pd.date_range(start=start_date, end=end_date)

    for city, station_id in stations.items():
        for single_date in date_range:
            formatted_date = single_date.strftime('%Y%m%d')
            params = {
                'serviceKey': service_key,
                'numOfRows': '10',
                'pageNo': '1',
                'dataCd': 'ASOS',
                'dateCd': 'DAY',
                'startDt': formatted_date,
                'endDt': formatted_date,
                'stnIds': str(station_id),
                'dataType': 'JSON'
            }
            response = requests.get(endpoint, params=params)
            try:
                response_data = response.json()
                if response_data.get('response').get('header').get('resultCode') == '00':
                    items = response_data.get('response').get('body').get('items').get('item')
                    if items:
                        for item in items:
                            data_list.append({
                                'BASE_DT': item.get('tm'),
                                'AREA_NAME': city,
                                'AVG_TA': item.get('avgTa', 0.0) or 0.0,
                                'MIN_TA': item.get('minTa', 0.0) or 0.0,
                                'MAX_TA': item.get('maxTa', 0.0) or 0.0,
                                'SUM_RN': item.get('sumRn', 0.0) or 0.0,
                                'MAX_INS_WS': item.get('maxInsWs', 0.0) or 0.0,
                                'SUM_SS_HR': item.get('sumSsHr', 0.0) or 0.0,
                                'SS_DUR': item.get('ssDur', 0.0) or 0.0,
                                'SUM_FOG_DUR': item.get('sumFogDur', 0.0) or 0.0
                            })
            except ValueError:
                print(f"Error decoding JSON for date {formatted_date} and station {station_id}")
                print(f"Response text: {response.text}")

    if data_list:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_conn_id')
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO WH_WEATHER_INFO (
            BASE_DT, AREA_NAME, AVG_TA, MIN_TA, MAX_TA, SUM_RN, MAX_INS_WS, SUM_SS_HR, SS_DUR, SUM_FOG_DUR
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for data in data_list:
            cursor.execute(insert_query, (
                data['BASE_DT'], data['AREA_NAME'], data['AVG_TA'], data['MIN_TA'], data['MAX_TA'], data['SUM_RN'],
                data['MAX_INS_WS'], data['SUM_SS_HR'], data['SS_DUR'], data['SUM_FOG_DUR']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Data inserted into Redshift")
    else:
        print("No data collected")

# Airflow DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_daily_data_collection',
    default_args=default_args,
    description='Daily data collection from Weather API and insert into Redshift',
    schedule_interval='0 10 * * *',  # 매일 오전 10시에 실행
)

# Airflow Task용 함수
def collect_data_execution():
    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')  # 어제 날짜
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')    # 어제 날짜
    collect_and_insert_data(base_url, start_date, end_date)

collect_data_task = PythonOperator(
    task_id='collect_data_task',
    python_callable=collect_data_execution,
    dag=dag,
)

collect_data_task

