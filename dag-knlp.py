import datetime
import logging
from datetime import timedelta
import requests 
import boto3 
import pandas as pd 
from bs4 import BeautifulSoup

from airflow import models
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Variables 

S3_BUCKET = models.Variable.get('s3_bucket')


# SQLs

CREATE_KOREAN_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS korean (
        data_id INT IDENTITY(1,1),
        edit_id VARCHAR (30),
        word_id VARCHAR (30),
        korean VARCHAR (500),
        PRIMARY KEY (data_id)
        );
    """

CREATE_KOREAN_JAPANESE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS korean_japanese (
        data_id INT IDENTITY(1,1),
        article_id VARCHAR (30),
        language VARCHAR(30),
        text VARCHAR (500),
        PRIMARY KEY (data_id)
        );
    """

CREATE_KOREAN_HANJYA_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS korean_hanjya (
        data_id INT IDENTITY(1,1),
        korean VARCHAR (100),
        hanjya VARCHAR (100),
        examples VARCHAR (100),
        PRIMARY KEY (data_id)
        );
    """

CREATE_KOREAN_JAPANESE_HANJYA_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS korean_japanese_hanjya (
        data_id INT IDENTITY(1,1),
        word_id VARCHAR(30),
        korean VARCHAR(500),
        japanese VARCHAR (500),
        hanjya VARCHAR (500),
        PRIMARY KEY (data_id)
        );
    """

MAKE_DIMENSION_TABLE_SQL = """
    INSERT INTO korean_japanese_hanjya (
        word_id,
        korean,
        japanese,
        hanjya 
    )
    SELECT DISTINCT 
        korean.word_id,
        korean.korean,
        text,
        hanjya
    FROM korean 
    LEFT JOIN korean_japanese ON korean.word_id = korean_japanese.article_id
    LEFT JOIN korean_hanjya ON korean.korean = korean_hanjya.korean; 
    """


# functions 

def read_hanjya(*args, **kawargs):
    '''
    this is a preprocessing function to process a raw text 
    to a clean, machine readable JSON file

    This is prepared for 'hanja.txt'
    '''

    s3 = boto3.resource('s3')
    f = open('hanja.txt', 'r')

    df = pd.DataFrame(columns=['korean', 'hanja', 'examples'])
    stop_line = 1500000  
    # stop_line is for development use (use small numbers 
    # (ex. 100) to stop earlier) instead waiting for hours 

    for index, line in enumerate(f): 
        split_line = line.split(':')

        try: df.loc[len(df)] = split_line 
        except: df.loc[len(df)] = [split_line[0], split_line[1], split_line[2:]]

        if index == stop_line: break

    hanja_JSON = df.to_json(orient='records', force_ascii=False, lines=True)
    filename = 'hanja2.json'
    s3.Bucket(S3_BUCKET).put_object(Key=filename, Body=hanja_JSON)

def read_title(*args, **kawargs):
    '''
    this is a preprocessing function to process a raw text 
    to a clean, machine readable JSON file

    This is prepared for kowiki-20190401-pages-articles-multistream-index.txt
    '''


    f = open('kowiki-20190401-pages-articles-multistream-index.txt', 'r')

    df = pd.DataFrame(columns=['edit_id', 'word_id', 'korean'])

    show = 10000
    stop_line = 1500000
    # stop_line is for development use (use small numbers 
    # (ex. 100) to stop earlier) instead waiting for hours 

    s3 = boto3.resource('s3')

    for index, line in enumerate(f): 
        split_line = line.split(':')

        try: df.loc[len(df)] = split_line 
        except: df.loc[len(df)] = [split_line[0], split_line[1], split_line[2:]]

        if index % show == 0 and index != 0:
            print(index, split_line)
            print(df.head())
            title_JSON = df.to_json( orient='records', force_ascii=False, lines=True)
            filename = 'titles2/titles{}.json'.format(str(int(index/show)).zfill(4))
            s3.Bucket('knlp-us').put_object(Key=filename, Body=title_JSON)
            df = pd.DataFrame(columns=['edit_id', 'word_id', 'korean'])

        if index == stop_line: 
            break

    title_JSON = df.to_json(orient='records', force_ascii=False, lines=True)
    filename = 'titles2/titles_final.json'
    s3.Bucket(S3_BUCKET).put_object(Key=filename, Body=title_JSON)

def read_langlink(*args, **kawargs):

    '''
    this is a preprocessing function to process a raw text 
    to a clean, machine readable JSON file

    This is prepared for kowiki-20190401-langlink(processed).txt
    '''


    f = open('kowiki-20190401-langlink(processed).txt', 'r')

    df = pd.DataFrame(columns=['article_id', 'language','text'])

    stop_line = 1500000
    # stop_line is for development use (use small numbers 
    # (ex. 100) to stop earlier) instead waiting for hours 

    for line in f: 
        split_line = line.split("),(")

        for index, tup in enumerate(split_line):
        
            split_word = tup.split(',')
            try: df.loc[len(df)] =  split_word
            except: df.loc[len(df)] = [split_word[0],split_word[1], split_word[2:]]
            
            if index == stop_line:
                break

    langlink_JSON = df.to_json(orient='records', force_ascii=False, lines=True)

    s3 = boto3.resource('s3')
    filename = 'langlink2.json'
    s3.Bucket(S3_BUCKET).put_object(Key=filename, Body=langlink_JSON)



def stage_data_to_redshift1(*args, **kwargs):
    '''
    this is a function to stage 'langlink2.json' onto Redshift
    '''

    filename = 'langlink2.json'
    aws_hook = AwsHook('aws-credentials')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook('redshift')
    s3_path = 's3://{}/{}'.format(S3_BUCKET,filename)
    sql = "COPY {} (article_id, language, text) FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' \
            JSON 'auto';".format(
        'korean_japanese', s3_path, credentials.access_key, credentials.secret_key
    )
    print('staged')
    redshift_hook.run(sql)

def stage_data_to_redshift2(*args, **kwargs):
    '''
    this is a functino to stage 'hanja2.json' onto Redshift
    '''

    filename = 'hanja2.json'
    aws_hook = AwsHook('aws-credentials')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook('redshift')
    s3_path = 's3://{}/{}'.format(S3_BUCKET,filename)
    sql = "COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' \
            JSON 'auto';".format(
        'korean_hanjya', s3_path, credentials.access_key, credentials.secret_key
    )
    print('staged')
    redshift_hook.run(sql)

def stage_data_to_redshift3(*args, **kwargs):
    '''
    this is a functino to stage files in 'titles2' folder onto Redshift
    '''

    filename = 'titles2'
    aws_hook = AwsHook('aws-credentials')
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook('redshift')
    s3_path = 's3://{}/{}'.format(S3_BUCKET,filename)
    sql = "COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' \
            JSON 'auto';".format(
        'korean', s3_path, credentials.access_key, credentials.secret_key
    )
    print('staged')
    redshift_hook.run(sql)

def check_data_count(*args, **kwargs):
    '''
    this is a functino to check data. 
    Check whether a table spacified by kwargs (['params']['table])
    has 1 or more rows
    '''

    table = kwargs['params']['table']
    redshift_hook = PostgresHook('redshift')
    records = redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data quality check failed. {table} returned no results")
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f"Data quality check failed. {table} contained 0 row")
    logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")

def check_data_length(*args, **kwargs):
    '''
    this is a functino to check data. 
    Check whether a field in a table spacified by kwargs 
    (['params']['table], and ['params]['field])
    has longer values than the threshold ['params']['max_length_th]
    If so, it is suspicios that some irregular data are inside
    '''

    table = kwargs['params']['table']
    field = kwargs['params']['field']
    max_length_th = kwargs['params']['max_length_th']
    redshift_hook = PostgresHook('redshift')
    records = redshift_hook.get_records(f"SELECT length({field}) FROM {table} ORDER BY length({field}) DESC LIMIT 1")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Data quality check failed. {field} returned no results")
    max_length = records[0][0]
    if max_length > max_length_th:
        raise ValueError(f"Data quality check failed. {field} contained too long data, something is wrong")
    logging.info(f"Data quality on table {table}{field} check passed with maxmum length {records[0][0]}")

# DAG 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2019, 8, 23),
    'retries':0,
    'retry_delay':timedelta(minutes=1),
    }

dag = DAG('korean-NLP', 
        catchup=False,
        schedule_interval='@monthly', 
        default_args=default_args,
        description='saving into Redshift'
        )

# Operators 

start_operator = DummyOperator(task_id='ETL_start', dag=dag)

preprocess_start = DummyOperator(task_id='Preprocess_start', dag=dag)

preprocess1 = PythonOperator(
    task_id='preprocess1',
    dag=dag,
    python_callable=read_hanjya
)

preprocess2 = PythonOperator(
    task_id='preprocess2',
    dag=dag,
    python_callable=read_langlink
)

preprocess3 = PythonOperator(
    task_id='preprocess3',
    dag=dag,
    python_callable=read_title
)

preprocess_end = DummyOperator(task_id='Preprocess_end', dag=dag)

create_table_start = DummyOperator(task_id='CreateTable_start', dag=dag)

create_korean_table = PostgresOperator(
    task_id='create_korean_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=CREATE_KOREAN_TABLE_SQL
)

create_korean_japanese_table = PostgresOperator(
    task_id='create_korean_japanese_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=CREATE_KOREAN_JAPANESE_TABLE_SQL
)

create_korean_hanjya_table = PostgresOperator(
    task_id='create_korean_hanjya_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=CREATE_KOREAN_HANJYA_TABLE_SQL
)

create_korean_japanese_hanjya_table = PostgresOperator(
    task_id='create_korean_japanese_hanjya_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=CREATE_KOREAN_JAPANESE_HANJYA_TABLE_SQL 
)

create_table_end = DummyOperator(task_id='CreateTable_end', dag=dag)

staging_start = DummyOperator(task_id='Staging_start', dag=dag)

stage_table1 = PythonOperator(
    task_id='stage_data_to_redshift1',
    dag=dag,
    python_callable=stage_data_to_redshift1
)

stage_table2 = PythonOperator(
    task_id='stage_data_to_redshift2',
    dag=dag,
    python_callable=stage_data_to_redshift2
)

stage_table3 = PythonOperator(
    task_id='stage_data_to_redshift3',
    dag=dag,
    python_callable=stage_data_to_redshift3
)

staging_end = DummyOperator(task_id='Staging_end', dag=dag)

make_dimension_table = PostgresOperator(
    task_id='make_dimension_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=MAKE_DIMENSION_TABLE_SQL
)

datacheck_start = DummyOperator(task_id='DataCheck_start', dag=dag)

datacheck1 = PythonOperator(
    task_id='check_data_count',
    dag=dag,
    python_callable=check_data_count,
    provide_context=True,
    params={'table':'korean'},
)

datacheck2 = PythonOperator(
    task_id='check_data_length',
    dag=dag,
    python_callable=check_data_length,
    provide_context=True,
    params={'table':'korean', 'field':'word_id', 'max_length_th':13},
)

datacheck_end = DummyOperator(task_id='DataCheck_end', dag=dag)

end_operator = DummyOperator(task_id='ETL_end',  dag=dag)

# sequences 
start_operator >> preprocess_start

preprocess_start >> preprocess1
preprocess_start >> preprocess2
preprocess_start >> preprocess3
preprocess1 >> preprocess_end
preprocess2 >> preprocess_end
preprocess3 >> preprocess_end

preprocess_end >> create_table_start 

create_table_start >> create_korean_table
create_table_start >> create_korean_japanese_table
create_table_start >> create_korean_hanjya_table
create_table_start >> create_korean_japanese_hanjya_table

create_korean_table >> create_table_end
create_korean_japanese_table >> create_table_end
create_korean_hanjya_table >> create_table_end
create_korean_japanese_hanjya_table >> create_table_end

create_table_end >> staging_start

staging_start >> stage_table1
staging_start >> stage_table2
staging_start >> stage_table3
stage_table1 >> staging_end
stage_table2 >> staging_end
stage_table3 >> staging_end

staging_end >> make_dimension_table

make_dimension_table >> datacheck_start

datacheck_start >> datacheck1
datacheck_start >> datacheck2
datacheck1 >> datacheck_end
datacheck2 >> datacheck_end

datacheck_end >> end_operator


