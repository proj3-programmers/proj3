import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import defaultdict
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# API key와 기본 URL 설정
api_key = 'b3b88dd403bb42d3ab002f3d0485ee22'
base_url = 'http://kopis.or.kr/openApi/restful/prfstsArea'

# API로부터 데이터를 가져오는 함수
def get_kopis_data(endpoint, params):
    params['service'] = api_key
    response = requests.get(endpoint, params=params)
    
    # 상태 코드 확인
    if response.status_code == 200:
        try:
            root = ET.fromstring(response.content)
            return root
        except ET.ParseError:
            print("XML Parse Error. Response content:")
            print(response.content)
            return None
    else:
        print(f"Request failed with status code {response.status_code}. Response content:")
        print(response.content)
        response.raise_for_status()

# XML 데이터를 리스트로 변환하는 함수 (태그가 동일한 여러 항목 처리)
def xml_to_list(element):
    data_list = []
    for child in element.findall('prfst'):
        data_dict = {}
        for subchild in child:
            data_dict[subchild.tag] = subchild.text
        data_list.append(data_dict)
    return data_list

# 시작 날짜와 종료 날짜 사이의 모든 날짜 목록 생성
def generate_dates(start_date, end_date):
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    delta = end - start
    return [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range(delta.days + 1)]

# 데이터 수집 및 Redshift에 삽입하는 함수
def collect_and_insert_data(endpoint, start_date, end_date):
    all_data = defaultdict(lambda: {'PRF_PRO_CNT': 0, 'PRF_DT_CNT': 0, 'NMRS': 0, 'AMOUNT': 0.0})
    dates = generate_dates(start_date, end_date)
    
    for date in dates:
        params = {
            'stdate': date,
            'eddate': date,
        }
        root = get_kopis_data(endpoint, params)
        if root is not None:
            data = xml_to_list(root)
            for item in data:
                key = (date, item.get('area', ''))
                if item.get('prfprocnt') != '0' or item.get('prfdtcnt') != '0' or item.get('nmrs') != '0' or item.get('amount') != '0.0':
                    all_data[key]['PRF_PRO_CNT'] += int(item.get('prfprocnt', 0))
                    all_data[key]['PRF_DT_CNT'] += int(item.get('prfdtcnt', 0))
                    all_data[key]['NMRS'] += int(item.get('nmrs', 0))
                    all_data[key]['AMOUNT'] += float(item.get('amount', 0.0))
    
    if all_data:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_conn_id')
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO bingryun.WH_PRFSTS_AREA_INFO (BASE_DT, AREA_NAME, PRF_PRO_CNT, PRF_DT_CNT, NMRS, AMOUNT)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        for key, values in all_data.items():
            date, area = key
            cursor.execute(insert_query, (date, area, values['PRF_PRO_CNT'], values['PRF_DT_CNT'], values['NMRS'], values['AMOUNT']))
        
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
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kopis_daily_data_collection',
    default_args=default_args,
    description='Daily data collection from KOPIS API and insert into Redshift',
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

