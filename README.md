# Ramp ETL Pipeline on AWS

This project implements an **ETL (Extract, Transform, Load)** pipeline that fetches data from the Ramp API, transforms the data into a structured format, and loads it into AWS S3 and Redshift. The goal is to automate the process of extracting data from Ramp, processing it, and storing it in a scalable cloud infrastructure for further analysis.

## Overview

The pipeline consists of three main steps:
1. **Extract**: Fetches raw data from the Ramp API.
2. **Transform**: Transforms the raw data into a structured format (DataFrame).
3. **Load**: Loads the transformed data into:
   - **AWS S3** (Data Lake): Stores raw and processed data in S3 for easy access.
   - **AWS Redshift**: Loads the data into Redshift for analytics and reporting.

## Features
- **API Integration**: Fetches data from Ramp through its API.
- **Data Transformation**: Transforms raw data into a structured format (DataFrame).
- **Data Lake**: Stores structured data in AWS S3 for scalability and easy access.
- **Redshift Data Warehouse**: Loads transformed data into AWS Redshift for querying and analysis.
- **Scalable**: The pipeline is designed to handle large datasets efficiently using AWS services.

## Python Libraries Used
- **boto3**: AWS SDK for Python, used for interacting with AWS S3 and Redshift.
- **requests**: For making HTTP requests to fetch data from the Ramp API.
- **pandas**: For transforming and structuring data into DataFrames.
- **psycopg2**: PostgreSQL adapter for Python to interact with Redshift and load data.
- **json**: For parsing the JSON data from the Ramp API.

## Requirements
Ensure that you have the following Python libraries installed:

```bash
pip install boto3 requests pandas psycopg2
