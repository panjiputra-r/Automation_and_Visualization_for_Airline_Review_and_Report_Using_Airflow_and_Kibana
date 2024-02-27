'''
==============================================================
Milestone 3

Name : Panji Putra Rianto
Batch : HCK-012

Objective : Program ini dibuat untuk melakukan otomatisasi dari meload data ke postgres, melakukan cleaning data hingga upload
ke elasticsearch untuk dilakukan visualisasi

Dataset : https://www.kaggle.com/datasets/juhibhojani/airline-reviews

===============================================================
'''


from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def load_csv_to_postgres():
    # meload data csv ke postgres
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/P2M3_panji_putra_data_raw.csv')
    #df.to_sql(nama_table, conn, index=False, if_exists='replace')
    df.to_sql('table_m3', conn, index=False, if_exists='replace')  # Menggunakan if_exists='replace' agar tabel digantikan jika sudah ada
    

def ambil_data():
    # fetch data
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query("select * from table_m3", conn) 
    df.to_csv('/opt/airflow/dags/P2M3_panji_putra_data_new.csv', sep=',', index=False)
    


def preprocessing(): 
    ''' fungsi untuk membersihkan data'''
    # mengambil data dari csv
    df = pd.read_csv("/opt/airflow/dags/P2M3_panji_putra_data_new.csv")

    # Melakukan split terhadap bulan dan tahun kolom
    df[['flight_month', 'flight_year']] = df['Date Flown'].str.split(' ', expand=True)

    # drop column yang tidak dibutuhkan 
    df.drop(columns=['Date Flown'], inplace=True)

    # mengubah kolom 'Unnamed: 0' menjadi 'id'
    df.rename(columns={'Unnamed: 0': 'flight_id'}, inplace=True)

    # mengubah format waktu ke datetime
    df['Review Date'] = df['Review Date'].str.replace(r'(\d+)(st|nd|rd|th)', r'\1', regex=True)
    df['Review Date'] = pd.to_datetime(df['Review Date'])

    # melakukan drop null
    df.dropna(inplace=True)
    # melakukan drop duplicate
    df.drop_duplicates(inplace=True)
    # mengubah kapital menjadi huruf kecil
    df.columns = df.columns.str.lower()

    # mengubah datatype dari object ke float
    df['overall_rating'] = df['overall_rating'].astype(float)

    # melakukan replace simbol pada data
    replace = str.maketrans({' ': '_'})
    df.columns = [col.translate(replace) for col in df.columns] 

    # menyimpan data bersih ke file csv baru
    df.to_csv('/opt/airflow/dags/P2M3_panji_putra_data_clean.csv', index=False)
    
def upload_to_elasticsearch():
    # mengupload data ke elasticsearch
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_panji_putra_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        
# memberi nama dan set waktu di waktu indonesia bagian barat        
default_args = {
    'owner': 'Panji', 
    'start_date': datetime(2024, 2, 22, 18, 00) - timedelta(hours=7)
}

with DAG(
    "P2M3_Panji_DAG_hck", #atur sesuai nama project kalian
    description='Milestone_3',
    schedule_interval='30 6 * * *', #atur schedule untuk menjalankan airflow pada 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    # Task : 1
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) #sesuai dengan nama fungsi yang dibuat
    
    #task: 2
    ''' Fungsi untuk mengambil data'''
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=ambil_data) #
    

    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)

    # Task: 4
    '''  Fungsi ini ditujukan untuk upload data ke elasticsearch.'''
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    #proses untuk menjalankan di airflow
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data



