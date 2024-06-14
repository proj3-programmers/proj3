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

    current_date = datetime.now()
    # first_day_of_month = current_date.replace(day=1)
    next_month = current_date.replace(day=28) + timedelta(days=4)
    last_day_of_month = next_month - timedelta(days=next_month.day)
    # first_day = first_day_of_month.strftime('%Y%m%d')
    last_day= last_day_of_month.strftime('%Y%m%d')
    
    url = f'http://kopis.or.kr/openApi/restful/boxStatsCate?service={Key}&stdate=20240101&eddate={last_day}'
    result = xmltodict.parse(requests.get(url).content)

    CATE = result['box-statsofs']['boxStatsof']

    return CATE
@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    CATE varchar(32) primary key,
    SHOW_CNT int,
    SCREENCNT int,
    RESERVATION_CNT int,
    CANCEL_CNT int,
    TOTAL_TICKET int,
    TOTAL_SALES bigint
);""")

        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r['catenm']}', {r['prfcnt']}, {r['prfdtcnt']}, {r['ntssnmrssm']}, {r['cancelnmrssm']}, {r['totnmrssm']}, {r['ntssamountsm']});"
            print(sql) 
            cur.execute(sql)

        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")

with DAG(
    dag_id = 'CateData',
    start_date = datetime(2024,1,1),
    catchup=False,
    tags=['API'],
    schedule_interval='@monthly'
) as dag:

    results = extract_transform()
    load("dooby99", "CateData_v2", results)
