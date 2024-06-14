from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pandas import Timestamp
from datetime import datetime
from datetime import timedelta

import pandas as pd
import logging
import requests
import xmltodict

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract_transform():
    Key = Variable.get("show_data_api_key") #airflow variables에 저장되었던 키를 읽어와서 

    # current_date = datetime.now()
    # first_day_of_month = current_date.replace(day=1)

    # next_month = current_date.replace(day=28) + timedelta(days=4)
    # last_day_of_month = next_month - timedelta(days=next_month.day)
    
    # first_day = first_day_of_month.strftime('%Y%m%d')
    # last_day= last_day_of_month.strftime('%Y%m%d')
    
    cpage = 1
    all_results = []
    result = []
    
    while True:
        # url = f'http://www.kopis.or.kr/openApi/restful/pblprfr?service={Key}&stdate={first_day}&eddate={last_day}&cpage={cpage}&rows=1000&newsql=Y'
        url = f'http://www.kopis.or.kr/openApi/restful/pblprfr?service={Key}&stdate=20240612&eddate=20240630&cpage={cpage}&rows=1000&newsql=Y'

        result = xmltodict.parse(requests.get(url).content)

        if result['dbs'] is not None:
            all_results.append(result)
            cpage += 1
        else:
            break
        
    data = []
    for result in all_results:
        dbs = result['dbs']['db']
        for item in dbs:
            item = {key:value for key, value in item.items() if key != 'poster'}
            
            if 'prfpdfrom' in item:
                item['prfpdfrom'] = datetime.strptime(item['prfpdfrom'], '%Y.%m.%d').strftime('%Y-%m-%d')
            if 'prfpdto' in item:
                item['prfpdto'] = datetime.strptime(item['prfpdto'], '%Y.%m.%d').strftime('%Y-%m-%d')
            
            if 'area' in item:
                item['area'] = item['area'][:2]
            
            data.append(item)
    return data
@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    UNI_ID varchar(32) primary key,
    NAME varchar(255),
    START_DATE date,
    LAST_DATE date,
    PLACE varchar(128),
    AREA_NAME varchar(32),
    CATE varchar(32),
    OPENRUN boolean,
    TYPE varchar(16)
);""")

        for r in records:
            prfnm = r.get('prfnm', None)  # 'prfnm' 키가 없는 경우 None으로 설정
            prfpdfrom = r.get('prfpdfrom', None)  # 'prfpdfrom' 키가 없는 경우 None으로 설정
            prfpdto = r.get('prfpdto', None)  # 'prfpdto' 키가 없는 경우 None으로 설정
            fcltynm = r.get('fcltynm', None)  # 'fcltynm' 키가 없는 경우 None으로 설정
            area = r.get('area', None)  # 'area' 키가 없는 경우 None으로 설정
            genrenm = r.get('genrenm', None)  # 'genrenm' 키가 없는 경우 None으로 설정
            openrun = r.get('openrun', None)  # 'openrun' 키가 없는 경우 None으로 설정
            prfstate = r.get('prfstate', None)  # 'prfstate' 키가 없는 경우 None으로 설정
            
            if None in (prfnm, prfpdfrom, prfpdto, fcltynm, area, genrenm, openrun, prfstate):
                logging.warning("One or more required keys are missing in the record. Skipping this record.")
                continue
            
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (
                r['mt20id'], 
                prfnm, 
                prfpdfrom, 
                prfpdto, 
                fcltynm, 
                area, 
                genrenm, 
                openrun, 
                prfstate
            )
            print(sql % tuple(map(repr, values)))  # 디버깅을 위한 출력
            cur.execute(sql, values)
            # cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")

with DAG(
    dag_id = 'ShowData',
    start_date = datetime(2023,5,30),
    catchup=False,
    tags=['API'],
    schedule = '0 2 * * *'
) as dag:

    results = extract_transform()
    load("dooby99", "ShowData_v2", results)