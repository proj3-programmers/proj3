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
    end_date = current_date.replace(day=1)  # 이번 달 1일로 설정
    start_date = current_date - timedelta(days=6*30)  # 6개월 전 날짜
    dates = pd.date_range(start_date, end_date, freq='MS')

    # 모든 월에 대해 API 호출하여 데이터 수집
    all_data = []
    for date in dates:
        date_str = date.strftime('%Y%m%d')
        print(date_str)
        url = f'http://kopis.or.kr/openApi/restful/boxoffice?service={Key}&ststype=month&date={date_str}'
        result5 = xmltodict.parse(requests.get(url).content)
        
        Reservation = result5['boxofs']['boxof']
        Reservation = pd.DataFrame(Reservation)
        
        Reservation['start_date'] = Reservation['prfpd'].apply(lambda x: datetime.strptime(x.split('~')[0].strip(), '%Y.%m.%d').strftime('%Y-%m-%d'))
        Reservation['last_date'] = Reservation['prfpd'].apply(lambda x: datetime.strptime(x.split('~')[1].strip(), '%Y.%m.%d').strftime('%Y-%m-%d'))
        Reservation['year_month'] = date.strftime('%Y-%m-%d')
        
        if 'area' in Reservation.columns:
            Reservation['area'] = Reservation['area'].apply(lambda x: x[:2] if isinstance(x, str) else x)
        
        Reservation = Reservation.drop(columns=['prfpd', 'poster'])
        
        all_data.append(Reservation)


    # 모든 데이터를 하나의 DataFrame으로 합치기
    if all_data:
        final_data = pd.concat(all_data, ignore_index=True)
        reservation_json = final_data.to_json(orient='records')

    else:
        print("No data retrieved.")
        
    return reservation_json
@task
def load(schema, table, records):
    records = pd.read_json(records)
    logging.info("load started")
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    PLACE varchar(255),
    SEATCNT int,
    RANKING int,
    UNI_ID varchar(32) primary key,
    NAME varchar(255),
    CATE varchar(32),
    SCREENCNT int,
    AREA_NAME varchar(32),
    START_DATE date,
    LAST_DATE date,
    SAVE_DATE date
);""")

        for r in records.itertuples():
            name_escaped = r.prfnm.replace("'", "''")
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r.prfplcnm}', {r.seatcnt}, {r.rnum}, '{r.mt20id}', '{name_escaped}', '{r.cate}', {r.prfdtcnt}, '{r.area}', '{r.start_date}', '{r.last_date}', '{r.year_month}');"
            print(sql) 
            cur.execute(sql)


        cur.execute("COMMIT;")   # cur.execute("END;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise

    logging.info("load done")

with DAG(
    dag_id = 'ShowReservation_v2',
    start_date = datetime(2024,1,1),
    catchup=False,
    tags=['API'],
    schedule = '0 2 * * *'
) as dag:

    results = extract_transform()
    load("dooby99", "ShowReservation_v2", results)