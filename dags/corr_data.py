from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from datetime import datetime

def fetch_and_merge_data():
    redshift_hook = PostgresHook(postgres_conn_id='redshift_conn_id') 
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    weather_query = "SELECT * FROM dooby99.WH_WEATHER_INFO"
    performance_query = "SELECT * FROM dooby99.WH_PRFSTS_AREA_INFO"
    
    cursor.execute(weather_query)
    weather_data = pd.DataFrame(cursor.fetchall(), columns=[desc[0].lower() for desc in cursor.description])
    
    cursor.execute(performance_query)
    performance_data = pd.DataFrame(cursor.fetchall(), columns=[desc[0].lower() for desc in cursor.description])
    
    print("Weather Data Columns:", weather_data.columns.tolist())
    print("Performance Data Columns:", performance_data.columns.tolist())
    
    area_mapping = {
        '경기': '수원',
        '강원': '춘천',
        '충북': '청주',
        '충남': '서산',
        '전북': '전주',
        '전남': '목포',
        '경북': '포항',
        '경남': '창원',
        '서울': '서울',
        '부산': '부산',
        '대구': '대구',
        '인천': '인천',
        '광주': '광주',
        '대전': '대전',
        '울산': '울산',
        '세종': '세종',
        '제주': '제주'
    }
    
    if 'area_name' in performance_data.columns:
        performance_data['area_name'] = performance_data['area_name'].map(area_mapping)
    else:
        raise KeyError("Performance data does not contain 'area_name' column.")
    
    data = pd.merge(weather_data, performance_data, on=['base_dt', 'area_name'])
    
    # 데이터 변환 및 예외 처리
    for col in ['avg_ta', 'min_ta', 'max_ta', 'sum_rn', 'max_ins_ws', 'sum_ss_hr', 'ss_dur', 'sum_fog_dur', 'nmrs', 'amount']:
        data[col] = pd.to_numeric(data[col], errors='coerce')
        if data[col].isnull().all():
            raise ValueError(f"All values in column '{col}' are null or could not be converted to numeric.")
        if (data[col] == 0).all():
            print(f"All values in column '{col}' are 0.")
    
    print("Merged Data Types after conversion:\n", data.dtypes)
    print("Merged Data Description:\n", data.describe())

    data.to_pickle('/tmp/merged_data.pkl')

    cursor.close()
    conn.close()

def analyze_and_store_correlation():
    redshift_hook = PostgresHook(postgres_conn_id='redshift_conn_id') 
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    data = pd.read_pickle('/tmp/merged_data.pkl')
    
    print("Merged Data Columns:", data.columns.tolist())
    print("Merged Data Types:\n", data.dtypes)
    print("Missing values in merged data:\n", data.isnull().sum())
    print("Merged Data Description:\n", data.describe())
    
    data = data.dropna()

    correlation_matrix = data.corr()
    
    print("Correlation Matrix Columns:", correlation_matrix.columns.tolist())
    print("Correlation Matrix:\n", correlation_matrix)
    
    if 'nmrs' in correlation_matrix.columns:
        ticket_sales_correlation = correlation_matrix['nmrs'][['avg_ta', 'min_ta', 'max_ta', 'sum_rn', 'max_ins_ws', 'sum_ss_hr', 'ss_dur', 'sum_fog_dur']]
    else:
        raise KeyError("'nmrs' column is not present in the correlation matrix.")
    
    correlation_results = ticket_sales_correlation.reset_index()
    correlation_results.columns = ['variable', 'correlation']
    correlation_results['correlation'] = correlation_results['correlation'].apply(lambda x: None if pd.isna(x) else x)

    insert_query = """
    INSERT INTO dooby99.correlation_results (variable, correlation) VALUES (%s, %s)
    """
    
    for _, row in correlation_results.iterrows():
        cursor.execute(insert_query, (row['variable'], row['correlation']))
    
    conn.commit()
    cursor.close()
    conn.close()

def insert_merged_data():
    redshift_hook = PostgresHook(postgres_conn_id='redshift_conn_id')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO dooby99.MT_WEATHER_AREA_LIST (
        BASE_DT, AREA_NAME, AVG_TA, MIN_TA, MAX_TA, SUM_RN, MAX_INS_WS, SUM_SS_HR, SS_DUR, SUM_FOG_DUR,
        PRF_PRO_CNT, PRF_DT_CNT, NMRS, AMOUNT
    )
    SELECT
        w.BASE_DT, w.AREA_NAME, w.AVG_TA, w.MIN_TA, w.MAX_TA, w.SUM_RN, w.MAX_INS_WS, w.SUM_SS_HR, w.SS_DUR, w.SUM_FOG_DUR,
        p.PRF_PRO_CNT, p.PRF_DT_CNT, p.NMRS, p.AMOUNT
    FROM
        dooby99.WH_WEATHER_INFO w
    JOIN
        dooby99.WH_PRFSTS_AREA_INFO p
    ON
        w.BASE_DT = p.BASE_DT AND w.AREA_NAME = p.AREA_NAME;
    """
    
    cursor.execute(insert_query)
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 12),
    'retries': 1,
}

dag = DAG(
    'weather_performance_analysis',
    default_args=default_args,
    description='Analyze correlation between weather and performance ticket sales',
    schedule_interval='@daily',
)

fetch_and_merge_task = PythonOperator(
    task_id='fetch_and_merge_data',
    python_callable=fetch_and_merge_data,
    dag=dag,
)

analyze_and_store_task = PythonOperator(
    task_id='analyze_and_store_correlation',
    python_callable=analyze_and_store_correlation,
    dag=dag,
)

insert_merged_data_task = PythonOperator(
    task_id='insert_merged_data',
    python_callable=insert_merged_data,
    dag=dag,
)

fetch_and_merge_task >> analyze_and_store_task >> insert_merged_data_task
