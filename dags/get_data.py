import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import defaultdict
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from bs4 import BeautifulSoup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test',
    default_args=default_args,
    schedule_interval='0 9 * * *',
)

def collect_data_execution():
    try:
        redshift_hook = PostgresHook(postgres_conn_id='redshift_dev_db')
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        return

    # 테이블 생성 쿼리 실행
    create = """
    CREATE TABLE IF NOT EXISTS dooby99.TEST (
        UNI_ID VARCHAR(32) NOT NULL,
        NAME VARCHAR(64) NOT NULL,
        HALL_NUM VARCHAR(64) DEFAULT NULL,
        FAC_TYPE VARCHAR(64) DEFAULT NULL,
        AREA_NAME VARCHAR(32) DEFAULT NULL,
        GUGUN_NM VARCHAR(32) DEFAULT NULL,
        OPEN_DATE DATE DEFAULT NULL
    );
    """
    try:
        cursor.execute(create)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")
        cursor.close()
        conn.close()
        return

    # 데이터 가져오기
    url = "http://www.kopis.or.kr/openApi/restful/prfplc?service=d53ad643a7f94c20900030f2ecc40984&cpage=1&rows=100"
    try:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'xml')
        data = soup.find_all("db")
    except Exception as e:
        print(f"Error fetching or parsing data: {e}")
        cursor.close()
        conn.close()
        return
    
    for item in data:
        try:
            UNI_ID = item.find("mt10id").get_text().strip()
            NAME = item.find("fcltynm").get_text().strip()
            HALL_NUM = item.find("mt13cnt").get_text().strip()
            FAC_TYPE = item.find("fcltychartr").get_text().strip()
            AREA_NAME = item.find("sidonm").get_text().strip()
            GUGUN_NM = item.find("gugunnm").get_text().strip()
            OPEN_DATE_str = item.find("opende").get_text().strip()
            
            if OPEN_DATE_str:
                OPEN_DATE = datetime.strptime(OPEN_DATE_str, '%Y').date()
            else:
                OPEN_DATE = None
            
            # INSERT 쿼리 정의
            insert = """
            INSERT INTO dooby99.TEST (UNI_ID, NAME, HALL_NUM, FAC_TYPE, AREA_NAME, GUGUN_NM, OPEN_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            # Execute INSERT query
            cursor.execute(insert, (UNI_ID, NAME, HALL_NUM, FAC_TYPE, AREA_NAME, GUGUN_NM, OPEN_DATE))
            conn.commit()
            
        except AttributeError as e:
            print(f"Error extracting data: {e}")
            continue
        except ValueError as e:
            print(f"Error converting date: {e}")
            continue
        except Exception as e:
            print(f"Error inserting data: {e}")
            continue

    cursor.close()
    conn.close()


collect_data = PythonOperator(
    task_id='collect_data',
    python_callable=collect_data,
    dag=dag,
)

collect_data
