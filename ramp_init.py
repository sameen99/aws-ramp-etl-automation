#!/usr/bin/python3
import os
import requests
import pandas as pd
from pandas import json_normalize
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import sys

# Load environment variables from .env file
load_dotenv(dotenv_path="/home/sameen/qb_scripts/.env_access_ramp")
load_dotenv(dotenv_path="/home/sameen/qb_scripts/.env")

# Function to execute SQL queries on Redshift
def execute_sql(sql_query, conn):
    try:
        cur = conn.cursor()
        cur.execute(sql_query)
        conn.commit()
        cur.close()
        print(f"[{datetime.now()}] SQL Query Executed Successfully:\n{sql_query}")
    except Exception as e:
        print(f"[{datetime.now()}] Error executing SQL query:\n{sql_query}\nError Details: {str(e)}")

# Function to fetch data from a specific URL
def fetch_data(url):
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {os.getenv('RAMP_API_TOKEN')}",
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        print(f"[{datetime.now()}] Successfully fetched data from: {url}")
        return response.json()
    else:
        print(f"[{datetime.now()}] Failed to fetch data from {url}. Status code: {response.status_code}\nError: {response.text}")
        return None

# Fetch all data from Ramp API
def fetch_all_data(endpoint):
    all_data = []
    response_data = fetch_data(endpoint)
    
    while response_data:
        all_data.extend(response_data['data'])
        next_page_url = response_data.get('page',{}).get('next')
        if next_page_url:
            response_data = fetch_data(next_page_url)
        else:
            break
    
    return all_data

# Main script execution
def main():
    # Fetch data from Ramp API
    if len(sys.argv) > 1:
        str_arg_db = sys.argv[1]
        if str_arg_db in ['dev', 'meddw']:
            print(f"Loading into {str_arg_db}.\n")
        else:
            sys.exit(f"Illegal db name {srt_arg_db}")
    else:
        print("No db argument given. Assuming meddw.\n")
        str_arg_db = "meddw"

    endpoint = "https://api.ramp.com/developer/v1/bills"
    all_data = fetch_all_data(endpoint)
    
    # Normalize data and create DataFrame
    df = pd.json_normalize(all_data, sep='_')
    
    # Select desired columns
    selected_columns = ['invoice_urls', 'deep_link_url', 'created_at', 'due_at', 'remote_id', 'status', 'issued_at',
                        'id', 'invoice_number', 'amount_amount', 'vendor_type', 'vendor_remote_id', 'vendor_remote_name',
                        'payment_payment_method', 'payment_payment_date', 'payment_amount_amount', 'payment_effective_date',
                        'payment']
    
    df_selected = df.loc[:, selected_columns]
    
    # Divide the amount_amount column by 100
    df_selected.loc[:, 'amount_amount'] = df_selected['amount_amount'] / 100
    df_selected.loc[:, 'payment_amount_amount'] = df_selected['payment_amount_amount'] / 100

    
    # Convert data types for Redshift
    data_types = {
        'invoice_urls': 'string',
        'deep_link_url': 'string',
        'created_at': 'string',
        'due_at': 'string',
        'remote_id': 'string',
        'status': 'string',
        'issued_at': 'string',
        'id': 'string',
        'invoice_number': 'string',
        'amount_amount': 'float64',
        'vendor_type': 'string',
        'vendor_remote_id': 'string',
        'vendor_remote_name': 'string',
        'payment_payment_method': 'string',
        'payment_payment_date': 'string',
        'payment_amount_amount': 'float64',
        'payment_effective_date': 'string',
        'payment': 'string'
    }
    df_selected = df_selected.astype(data_types)
    
    # Write DataFrame to Parquet and upload to S3
    s3_url = "s3://datalake-medusadistribution/datalake/to_redshift/ramp/ramp_bills.parquet"
    df_selected.to_parquet(s3_url)
    print(f"[{datetime.now()}] Successfully wrote DataFrame to Parquet format and uploaded to S3: {s3_url}")
    
    # Define SQL statements
    sql_statements = [
        """CREATE TABLE staging.ramp_bills (
            invoice_urls VARCHAR(4096),
            deep_link_url VARCHAR(625),
            created_at VARCHAR(255),
            due_at VARCHAR(255),
            remote_id VARCHAR(25),
            status VARCHAR(10),
            issued_at VARCHAR(255),
            id VARCHAR(255),
            invoice_number VARCHAR(25),
            amount_amount DOUBLE PRECISION,
            vendor_type VARCHAR(25),
            vendor_remote_id VARCHAR(25),
            vendor_remote_name VARCHAR(255),
            payment_payment_method VARCHAR(25),
            payment_payment_date VARCHAR(255),
            payment_amount_amount DOUBLE PRECISION,
            payment_effective_date VARCHAR(255),
            payment VARCHAR(20)
        );""",
        f"COPY staging.ramp_bills FROM '{s3_url}' IAM_ROLE '{os.getenv('REDSHIFT_IAM_ROLE')}' FORMAT AS PARQUET;",
        "TRUNCATE TABLE finance.ramp_bills;",
        """INSERT INTO finance.ramp_bills
           SELECT 
                 invoice_urls,
                 deep_link_url,
                 TO_TIMESTAMP(created_at, 'YYYY-MM-DD HH24:MI:SS'),
                 TO_TIMESTAMP(due_at, 'YYYY-MM-DD HH24:MI:SS'),
                 remote_id,
                 status,
                 TO_TIMESTAMP(issued_at, 'YYYY-MM-DD HH24:MI:SS'),
                 id,
                 invoice_number,
                 amount_amount,
                 vendor_type,
                 vendor_remote_id,
                 vendor_remote_name,
                 payment_payment_method,
                 TO_TIMESTAMP(payment_payment_date, 'YYYY-MM-DD HH24:MI:SS'),
                 payment_amount_amount,
                 TO_TIMESTAMP(payment_effective_date, 'YYYY-MM-DD HH24:MI:SS'),
                 payment
           FROM staging.ramp_bills;""",
        "DROP TABLE staging.ramp_bills;"
    ]
    
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD"),
            host=os.getenv("REDSHIFT_HOST"),
            port=os.getenv("REDSHIFT_PORT")
        )
        
        # Execute SQL statements
        for sql_statement in sql_statements:
            execute_sql(sql_statement, conn)
        
    except Exception as e:
        print(f"[{datetime.now()}] Error connecting to Redshift:\nError Details: {str(e)}")
    
    finally:
        if conn:
            conn.close()
    
    # Print DataFrame with selected columns
    print("DataFrame with Selected Columns from API Response:")
    print(df_selected)

if __name__ == "__main__":
    main()
