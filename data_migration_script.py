# Import Libraries to generate data set
import json
import os
import pandas as pd
import mysql.connector
import couchdb

# Define hosts
mysql_host = "172.25.0.6"
couch_host = "172.25.0.4"

# Create function to connect to mysql
def connect_to_mysql():
    conn = mysql.connector.connect(user="airflow", password="airflow", host=mysql_host, database="exploration", port="3306")
    cursor = conn.cursor()
    return conn, cursor

# Count number of files that we need to create tables for
def count_file(path):
    count = len(os.listdir(path))
    return count

# Create drilling log tables
def create_dl_tables(count, cursor):
    try:
        for number_index in range(1, count + 1):
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS dl{number_index:03}")
            cursor.execute(f"CREATE TABLE dl{number_index:03} ( \
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
                            wellbore_trajectory VARCHAR(50))")
            print(f"Drilling log table dl{number_index:03} successfully created")
        return "All drilling log tables created successfully"
    except mysql.connector.Error as error:
        return f"Error: {error}"

# Define function to create tables for seismic survey
def create_ss_tables(count, cursor):
    try:
        for number_index in range(1, count + 1):
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS ss{number_index}")
            cursor.execute(f"CREATE TABLE ss{number_index} ( \
                            survey_id VARCHAR(5), \
                            location VARCHAR(50), \
                            shot_point_id VARCHAR(10), \
                            receiver_point_id VARCHAR(10), \
                            seismic_reflection_data VARCHAR(50))")
            print(f"Seismic survey table S{number_index} successfully created")
        return "All seismic survey tables created successfully"
    except mysql.connector.Error as error:
        return f"Error: {error}"

# Define function to create production data tables
def create_pd_tables(count, cursor):
    try:
        for number_index in range(1, count + 1):
            # Drop table if exists
            cursor.execute(f"DROP TABLE IF EXISTS pd{number_index:03}")
            cursor.execute(f"CREATE TABLE pd{number_index:03} ( \
                            well_id VARCHAR(10), \
                            production_date DATE, \
                            production_volume INT, \
                            equipment_status VARCHAR(50), \
                            environmental_status VARCHAR(50))")
            print(f"Production table pd{number_index:03} successfully created")
        return "All production data tables created successfully"
    except mysql.connector.Error as error:
        return f"Error: {error}"

# Create function to create database for personnel_data in couch db
def create_couch_database():
    try:
        couch = couchdb.Server(f"http://admin:password@{couch_host}:5984")
        # Create database called personnel_data
        db_name = "personnel_data"
        if db_name in couch:
            couch.delete(db_name)
            db = couch.create(db_name)
        # Read personnel data and save to database
        path = os.path.join(os.getcwd(), "personnel_data")
        with open(os.path.join(path, 'personnel_data.json'), "r") as file:
            data = json.load(file)
        # Save json document to database
        for row in data:
            db.save(row)
        print("All personnel documents uploaded to couch database successfully")
    except Exception as e:
        print(f"An error occurred: {e}")

# Function to load the data into the tables created in the database
def mysql_loader(folder_path, cursor, conn):
    try:
        for i, file_name in enumerate(os.listdir(folder_path), start=1):
            if file_name.startswith("drill"):
                file_path = os.path.join(folder_path, file_name)
                df = pd.read_excel(file_path)
                print(f"Sending dl{i:03} to MySQL database")
                table_name = f"dl{i:03}"
                columns = ", ".join(df.columns)
                placeholders = ", ".join(['%s'] * len(df.columns))
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                for index, row in df.iterrows():
                    cursor.execute(insert_query, tuple(row))
                conn.commit()
            elif file_name.startswith("prod"):
                file_path = os.path.join(folder_path, file_name)
                df = pd.read_excel(file_path)
                print(f"Sending pd{i:03} to MySQL database")
                table_name = f"pd{i:03}"
                columns = ", ".join(df.columns)
                placeholders = ", ".join(['%s'] * len(df.columns))
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                for index, row in df.iterrows():
                    cursor.execute(insert_query, tuple(row))
                conn.commit()
            elif file_name.startswith("seis"):
                file_path = os.path.join(folder_path, file_name)
                df = pd.read_excel(file_path)
                print(f"Sending ss{i} to MySQL database")
                table_name = f"ss{i}"
                columns = ", ".join(df.columns)
                placeholders = ", ".join(['%s'] * len(df.columns))
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                for index, row in df.iterrows():
                    cursor.execute(insert_query, tuple(row))
                conn.commit()
    except Exception as e:
        raise RuntimeError(f"An error occurred: {e}")


# Main function
def main():
    try:
        conn, cursor = connect_to_mysql()
        count_dl = count_file(os.path.join(os.getcwd(), "drilling_logs"))
        count_ss = count_file(os.path.join(os.getcwd(), "seismic_survey"))
        count_pd = count_file(os.path.join(os.getcwd(), "production_data"))
        create_dl_tables(count_dl, cursor)
        create_ss_tables(count_ss, cursor)
        create_pd_tables(count_pd, cursor)
        mysql_loader(os.path.join(os.getcwd(), "drilling_logs"), cursor, conn)
        mysql_loader(os.path.join(os.getcwd(), "seismic_survey"), cursor, conn)
        mysql_loader(os.path.join(os.getcwd(), "production_data"), cursor, conn)
        create_couch_database()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()




