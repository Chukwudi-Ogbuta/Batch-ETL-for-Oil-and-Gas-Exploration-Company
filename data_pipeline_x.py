# Import Libraries
import json
import os
import pandas as pd
import mysql.connector
import couchdb
import itertools
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import pymongo
import psycopg2


# Define hosts
minio_host = "172.25.0.8"
postgres_host = "172.25.0.5"
mysql_host = "172.25.0.6"
mongo_host = "172.25.0.7"
couch_host = "172.25.0.4"


# Create default args for airflow
default_args = {"owner": "me",
                "retries": 1,
                "retry_delay": timedelta(minutes=1),
                "start_date": datetime(2024, 4,22)}

# Define DAG
dag = DAG("horizon_exploration_pipeline",
          description="A pipeline to extract drilling log data, seismic survey data, production data, & personnel info",
          default_args= default_args,
          schedule= timedelta(weeks=2)
          )


# Extraction task
# Write function to extract data from mysql database
def extract_func():
    conn = mysql.connector.connect(user="airflow", password="airflow", host=mysql_host, database="exploration",port="3306")
    cursor = conn.cursor()  # Create cursor object
    minio_client = Minio(f"{minio_host}:9000", access_key="myminio", secret_key="myminiosecret", secure=False)
    try:
        bucket_name = "extracted-data"
        cursor.execute("show tables;")  # Retrieve list of tables in database
        response = cursor.fetchall()
        table_list = list(itertools.chain(*response))  # Convert retrieved list of tables into list
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        for table_name in table_list:
                cursor.execute(f"select * from {table_name}")  # Retrieve data from a table
                columns = [col[0] for col in cursor.description]  # Retrieve column names
                df = cursor.fetchall()
                df = pd.DataFrame(df, columns=columns)  # Save data to dataframe
                excel_file = f"{table_name}.xlsx"
                df.to_excel(excel_file, index=False)  # Save data to excel file
                minio_client.fput_object(bucket_name, f"{table_name}.xlsx", excel_file)
                os.remove(excel_file)
        print("Extraction Successful")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()
        cursor.close()


extract_data = PythonOperator(task_id="extract_data",
                              python_callable=extract_func,
                              dag=dag)


# Transformation
def transform_func():
    minio_client = Minio(f"{minio_host}:9000", access_key="myminio", secret_key="myminiosecret", secure=False)
    try:
        if not minio_client.bucket_exists("transformed-data"):
                    minio_client.make_bucket("transformed-data")
        for filename in minio_client.list_objects("extracted-data"):
            basename = os.path.basename(filename.object_name)
            if basename.startswith("dl"):
                minio_client.fget_object("extracted-data", f"{basename}", f"{filename.object_name}")
                df = pd.read_excel(f"{basename}")  # Read drilling log data
                df.iloc[:, 4:13] = df.iloc[:, 4:13].round(2)  # Carry out some transformations
                df["wellbore_inclination"] = df["wellbore_trajectory"].str.split(",", expand=True)[0]
                df["wellbore_azimuth"] = df["wellbore_trajectory"].str.split(",", expand=True)[1]
                df.drop('wellbore_trajectory', axis=1, inplace=True)
                excel_file = f"{basename}"
                df.to_excel(excel_file, index=False)
                minio_client.fput_object("transformed-data", f"{basename}", excel_file)
                os.remove(excel_file)
            elif basename.startswith("ss"):
                minio_client.fget_object("extracted-data", f"{basename}", f"{filename.object_name}")
                df = pd.read_excel(f"{basename}")  # Read drilling log data
                df['latitude'] = df['location'].str.split(",", expand=True)[0]  # Carry out some transformations
                df['longitude'] = df['location'].str.split(",", expand=True)[1]
                df.drop('location', axis=1, inplace=True)
                df['seismic_reflection_data'] = df['seismic_reflection_data'].str.replace("Reflection Amplitude: ", "")
                df = df.rename({'seismic_reflection_data': 'seismic_reflection_amplitude'}, axis=1)
                excel_file = f"{basename}"
                df.to_excel(excel_file, index=False)
                minio_client.fput_object("transformed-data", f"{basename}", excel_file)
                os.remove(excel_file)
            elif basename.startswith("pd"):
                minio_client.fget_object('extracted-data', f"{basename}", f"{filename.object_name}")
                df = pd.read_excel(f'{basename}')  # Read excel file
                df['day'] = pd.to_datetime(df['production_date'], format="%Y-%m-%d").dt.day
                df['month'] = pd.to_datetime(df['production_date'], format="%Y-%m-%d").dt.month
                df['year'] = pd.to_datetime(df['production_date'], format="%Y-%m-%d").dt.year
                df['quarter'] = pd.to_datetime(df['production_date'], format="%Y-%m-%d").dt.quarter
                excel_file = f"{basename}"
                df.to_excel(excel_file, index=False)
                minio_client.fput_object("transformed-data", f"{basename}", excel_file)
                os.remove(excel_file)
    except Exception as e:
        print(f"Error: {e}")


transform_data = PythonOperator(task_id="transform_data",
                                python_callable = transform_func,
                                dag=dag)

def nosql_etl():
    minio_client = Minio(f"{minio_host}:9000", access_key="myminio", secret_key="myminiosecret", secure=False)
    client = pymongo.MongoClient(f"mongodb://{mongo_host}:27017")
    try:
        db_name = "personnel_data"
        db = client["horizon_employees"]  # Create destination database
        # Check if the collection already exists and delete, create either ways
        if db_name in db.list_collection_names():
            db.drop_collection(db_name)  # Create destination collection
        collection = db.create_collection(db_name)
        couch = couchdb.Server(f"http://admin:password@{couch_host}:5984")  # set up couch database object
        db = couch[db_name]
        all_docs = [db[doc_id] for doc_id in db]  # Read each row in NoSQL database
        with open ("personnel_data.json", "w") as file:  # Save file
            json.dump(all_docs, file)
        data = pd.read_json(f'personnel_data.json', orient="records") # Read json into dataframe
        minio_client.fput_object("extracted-data", "personnel_data.json", "personnel_data.json")
        data['new_salary'] = (data['salary'] + (data['salary'] * 0.10))
        data.drop('salary', axis=1, inplace=True)
        # Write DataFrame to JSON file
        json_file_name = "transformed_json.json"
        data.to_json(json_file_name, orient="records")
        minio_client.fput_object("transformed-data", "personnel_data.json", json_file_name)
        # Load data into mymongo database
        with open('transformed_json.json', 'r') as file:
            json_data = json.load(file)
        collection.insert_many(json_data)
        os.remove("personnel_data.json")
        os.remove("transformed_json.json")
    except Exception as e:
        print(f"Error: {e}")

nosql_task = PythonOperator(task_id="nosql_task",
                            python_callable=nosql_etl,
                            dag=dag)


# Load Structured datasets into Postgres data warehouse
def refresh_tables():
    conn = psycopg2.connect(user="airflow", password="airflow", host=postgres_host, database='exploration',port='5432')
    cursor = conn.cursor()
    minio_client = Minio(f"{minio_host}:9000", access_key="myminio", secret_key="myminiosecret", secure=False)
    try:
        for filename in minio_client.list_objects("transformed-data"):
            basename = os.path.basename(filename.object_name)
            table_name = os.path.splitext(basename)[0]
            if basename.endswith('xlsx') and basename.startswith('dl'):
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                cursor.execute(f"CREATE TABLE {table_name} ( \
                                                well_id VARCHAR(10), \
                                                depth INT, \
                                                lithology VARCHAR(20), \
                                                formation_thickness INT, \
                                                rop NUMERIC, \
                                                wob NUMERIC, \
                                                rotary_speed NUMERIC, \
                                                torque NUMERIC, \
                                                mud_flow_rate NUMERIC, \
                                                mud_weight NUMERIC, \
                                                hookload NUMERIC, \
                                                standpipe_pressure NUMERIC, \
                                                surface_pressure NUMERIC, \
                                                wellbore_trajectory VARCHAR(50));")
                print(f"Drilling log table {table_name} successfully created")
            elif basename.endswith('xlsx') and basename.startswith('ss'):
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                cursor.execute(f"CREATE TABLE {table_name} ( \
                                               survey_id VARCHAR(5), \
                                               location VARCHAR(50), \
                                               shot_point_id VARCHAR(10), \
                                               receiver_point_id VARCHAR(10), \
                                               seismic_reflection_data VARCHAR(50));")
                print(f"Seismic survey table {table_name} successfully created")
            elif basename.endswith('xlsx') and basename.startswith('pd'):
                # Drop table if exists
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                cursor.execute(f"CREATE TABLE {table_name} ( \
                                                well_id VARCHAR(10), \
                                                production_date DATE, \
                                                production_volume INT, \
                                                equipment_status VARCHAR(50), \
                                                environmental_status VARCHAR(50));")
                print(f"Production table pd{table_name} successfully created")
            conn.commit()
    except Exception as e:
        print(f"Error: {e}")



def load_func():
    minio_client = Minio(f"{minio_host}:9000", access_key="myminio", secret_key="myminiosecret", secure=False)
    conn = psycopg2.connect(user='airflow', password='airflow', port="5432", host=postgres_host, database="exploration")
    cursor = conn.cursor()
    try:
        for filename in minio_client.list_objects("transformed-data"):
            basename = os.path.basename(filename.object_name)
            tablename = os.path.splitext(basename)[0]
            if basename.endswith('xlsx'):
                minio_client.fget_object("transformed-data", basename, filename.object_name)
                df = pd.read_excel(basename)
                columns = ", ".join(df.columns)
                placeholders = ", ".join(["%s"] * len(df.columns))
                query = f"INSERT INTO {tablename}({columns}) VALUES ({placeholders});"
                for index,row in df.iterrows():
                    cursor.execute(query, tuple(row))
                os.remove(basename)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

refresh_destination = PythonOperator(task_id="refresh_destination",
                                     python_callable=refresh_tables,
                                     dag=dag)

load_data = PythonOperator(task_id="load_data",
                           python_callable=load_func,
                           dag=dag)

# Define dependencies
extract_data >> transform_data >> refresh_destination >> load_data >> nosql_task