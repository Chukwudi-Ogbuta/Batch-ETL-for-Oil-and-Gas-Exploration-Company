# Batch ETL for Oil and Gas Exploration Company

## Table of Content
1. [Project Overview](#project-overview)
2. [Data Sources](#data-sources)
3. [Tools](#tools)
4. [Data Generation](#data-generation)
5. [Data Migration to Source database](#data-migration-to-source-database)
6. [ETL Process](#etl-process)
7. [Deliverables](#deliverables)
8. [Limitations](#limitations)
9. [References](#references)

### Project Overview:
This project focuses on designing and implementing ETL processes to extract data from a database used by several oil well fields. The data collected includes drilling logs, seismic surveys, production data from a mysql database, and employee information from couch db. This data is extracted and transformed using python libraries before being loaded into centralized data repositories (Postgres and Mongo db database) for analysis.

![Batch ETL Pipeline Diagram](https://github.com/Chukwudi-Ogbuta/Batch-ETL-for-Oil-and-Gas-Exploration-Company/assets/117915257/59d52e36-25c6-40b2-a7c1-929f3d2bc2c6)


### Data Sources
I utilized python libraries such as random and faker to generate synthetic datasets in order to avoid the use of real information and ensure data privacy. All codes used for generating synthetic data are provided as part of this project.

- Drilling logs data: Detailed records documenting the depth, location, and geological characteristics encountered during the drilling process.
- Seismic Survey data: Information collected from seismic surveys to map subsurface structures and identify potential oil or gas reservoirs.
- Production data: Data tracking the output and performance of oil or gas wells, including production rates and operational metrics.
- Personnel data: details of employed workers

### Tools
- Python: Creating DAG, data generation, cleaning, transformation, and connection to database.
- Apache Airflow: used to orchestrate data pipeline.
- Couch DB: data source for personnel data.
- MySQL Database: data source for drilling logs, seismic surveys, and production data.
- PostgresSQL Database: Destination database that receives data from MySQL database.
- Mongo DB: Destination database that receives data from Couch DB.
- MinIO: Creation of buckets that sets a staging area for ingested and transformed datasets.
- Visual Studio Code: IDE for Python.
- Docker Desktop: Creation of containerized apps.

### Data Generation
In the data generation stage, I defined separate functions to create several csv data for drilling logs, seismic surveys, production data, and json personnel data using actual ranges that mimic real life data. These functions allow you to determine the size of the data, e.g., passing the number of sites, wells, and records per wells or the number of employees as an argument to the function.

### Data Migration to Source database
In the data migration, the csv files generated from the data generation phase was transferred to a database acting as the source database. This was completed using mysql.connection library in python. The personnel data which is JSON data format was sent to Couch DB using the couchdb library in python.

## ETL Process
Apache Airflow was utized in orchestrating the ETL pipeline. I executed the ETL in the following order, first by importing necessary libraries, defining default arguments, defining dag object, constructing tasks and passing functions to them, before finally setting up task dependencies.

```Python
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
```

The actual ETL phase included using the same MySQL.connection and couchdb library to query data from MySQL and Couch DB database into MinIO buckets, data from the buckets were read and transformations were carried out on these data before they were sent into another MinIO bucket set as a staging area. The final part of this ETL included creating destination tables or clearing them if they already exist and sending the data from the transformed buckets into the tables in MYSQL and Couch DB. The destination database is always cleared and a full reupload is done to account for any possible changes (Updates or new additions) that may have occurred in the source database.

The ETL process was broken up into the following tasks in Apache Airflow:
1. Extract data
2. Transform data
3. NoSQL ETL
4. Refresh tables
5. Load data

### Deliverables
- Extracted data sets: Raw data extracted from various sources, such as databases, CSV and JSON files.
- Transformed data sets: Data has been processed, cleaned, and transformed according to business rules requirements.
- Loading processes: Scripts, workflows, or pipelines used to load transformed data into databases.
- Codebase: Source code developed to implement the ETL processes and transformations.

### Limitations
- Working with a windows system I had to make use of containerized apps to run Apache Airflow.

### References
- IBM Data Engineering
