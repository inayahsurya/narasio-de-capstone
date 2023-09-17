from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.exceptions import HTTPError
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
     'Capstone',
     default_args=default_args,
     schedule_interval=timedelta(hours=1),
     catchup=False,  # Don't backfill or "catch up" on execution
 )

def extract():
    json = 'https://api.coindesk.com/v1/bpi/currentprice.json'

    try:
        response = requests.get(json)
        response.raise_for_status()
        # access JSOn content
        jsonResponse = response.json()
        print("Entire JSON response")
        print(jsonResponse)

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    return jsonResponse

def transform(jsonResponse):
    data = dict(jsonResponse)

    time_df = pd.DataFrame(data['time'], index=[0])
    time_df.columns = ['time_updated', 'time_updated_iso', 'time_update_duk']

    disclaimer_df = pd.DataFrame(data['disclaimer'], index=[0], columns=['disclaimer'])

    chart_name_df = pd.DataFrame(data['chartName'], index=[0], columns=['chartName'])
    chart_name_df.columns = ['chart_name']

    bpi = pd.DataFrame.from_dict(data['bpi'], orient='index')
    bpi_cols_name = ['bpi_usd_code', 'bpi_gdp_code', 'bpi_eur_code',
                    'bpi_usd_description', 'bpi_gdp_description', 'bpi_eur_description',
                    'bpi_usd_rate_float', 'bpi_gdp_rate_float', 'bpi_eur_rate_float']

    bpi_flat = {}
    iter = 0
    for col in bpi[['code', 'description', 'rate_float']]:
        for data in bpi[col]:
            bpi_flat[bpi_cols_name[iter]] = data
            iter += 1
    bpi_df = pd.DataFrame(bpi_flat, index=[0])

    data_df = pd.concat([time_df, disclaimer_df, chart_name_df, bpi_df], axis=1)
    data_df['bpi_idr_rate_float'] = data_df['bpi_usd_rate_float'] * 15379
    data_df['time_updated']=pd.to_datetime(data_df["time_updated"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    data_df['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return data_df

def load(df):

    # Define the username and password with special characters
    username = 'airflow'
    password = 'airflow'
    db_name = 'airflow'
    db_port = '5432'

    # Encode the password to handle special characters
    encoded_password = quote_plus(password)

    # Define the database connection URL with encoded username and password
    # db_url = f'postgresql://{username}:{encoded_password}@localhost:{db_port}/{db_name}'
    db_url = f'postgresql+psycopg2://{username}:{password}@9bc6184ca068:{db_port}/{db_name}'


    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    # Load the DataFrame into the database table
    df.to_sql('bpi', engine, if_exists='append', index=False)

def etl():
    jsonResponse = extract()
    df = transform(jsonResponse)
    load(df)

task1 = PythonOperator(
    task_id='task1',
    python_callable=etl,
    dag=dag,
)

def done():
    print("Done")


task2 = PythonOperator(
    task_id='task2',
    python_callable=done,
    dag=dag,
)

task1 >> task2