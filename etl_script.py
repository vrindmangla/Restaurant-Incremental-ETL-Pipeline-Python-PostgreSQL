import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# MAKE CONNECTION
def getConnection(): 
 try:  
  conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
 )
  print("Database connection established successfully!")  
  return conn
 
 except Exception as e:
   print(f"Error connecting to the database: {e}")
   return None

#READ CSV FILE
def read_csv(file_path):
    log_function("read_csv") 
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            df = pd.read_csv(file_path)
        return df

    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

# Snake Case
def lower_case_columns(df):
    log_function("lower_case_columns")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

# LOG TABLE
def log_function(script_name):
    query = """ INSERT INTO etl_log (script_name) VALUES (%s) """
    conn = getConnection()
    cursor = conn.cursor()
    cursor.execute(query, (script_name,))
    conn.commit()
    conn.close()
