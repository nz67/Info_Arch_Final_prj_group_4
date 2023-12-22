#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import boto3
import csv
import io
import os
import json
import pymysql

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('FileProcessingStatus')  # DynamoDB table name
    required_files = ['WBSwithIO.csv', 'CostCenterData.csv', 'ProjectLists.csv']
    all_files_processed = True

    # Database connection
    rds_host = os.environ['DB_HOST']
    rds_user = os.environ['DB_USER']
    rds_password = os.environ['DB_PASSWORD']
    rds_db_name = os.environ['DB_NAME']
    conn = pymysql.connect(host=rds_host, user=rds_user, passwd=rds_password, db=rds_db_name)

    try:
        s3_client = boto3.client('s3')

        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            file_name = record['s3']['object']['key']

            # Read CSV file from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            file_content = response['Body'].read().decode('utf-8')
            csv_reader = csv.reader(io.StringIO(file_content))

            # Determine the table name based on the file name
            table_name = ""
            if 'WBSwithIO' in file_name:
                table_name = 'wbs_with_io'
            elif 'CostCenterData' in file_name:
                table_name = 'cost_center_data'
            elif 'ProjectLists' in file_name:
                table_name = 'project_lists'

            # Skip header and insert data into the database
            next(csv_reader)
            with conn.cursor() as cursor:
                for row in csv_reader:
                    query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in row])})"
                    cursor.execute(query, row)
                conn.commit()

            # Update the DynamoDB table
            table.update_item(
                Key={'FileName': file_name},
                UpdateExpression='SET #P = :val',
                ExpressionAttributeNames={'#P': 'Processed'},
                ExpressionAttributeValues={':val': True}
            )

        # Check if all files are processed
        response = table.scan()
        for item in response['Items']:
            if item['FileName'] in required_files and not item.get('Processed', False):
                all_files_processed = False
                break

        # perform data cleaning within this Lambda
        if all_files_processed:
            with conn.cursor() as cursor:
                cursor.execute("SET SQL_SAFE_UPDATES = 0;")
                cursor.execute("ALTER TABLE cost_center_data ADD COLUMN object_doc_no VARCHAR(255);")
                cursor.execute("UPDATE cost_center_data SET object_doc_no = CONCAT(Object, Document_Number);")
                cursor.execute("ALTER TABLE wbs_with_io ADD COLUMN partner_object_doc_no VARCHAR(255);")
                cursor.execute("UPDATE wbs_with_io SET partner_object_doc_no = CONCAT(Partner_Object, Document_Number);")
                cursor.execute("ALTER TABLE cost_center_data ADD COLUMN WBS_Element VARCHAR(60) AFTER Period;")
                cursor.execute("UPDATE cost_center_data SET WBS_Element = Partner_object;")
                cursor.execute("CREATE TABLE cc_deletes AS SELECT * FROM cost_center_data WHERE 1=0;")
                cursor.execute("""
                    INSERT INTO cc_deletes
                    SELECT ccd.*
                    FROM cost_center_data ccd
                    LEFT JOIN wbs_with_io wio ON ccd.object_doc_no = wio.partner_object_doc_no
                    WHERE wio.Object IS NULL;
                """)
                cursor.execute("""
                    CREATE TABLE wbs_cc AS
                    SELECT Full_Name, Personnel_Number, Total_Quantity, Val_COArea_Crcy, Period, WBS_Element, Object, Partner_Object, CO_Object_Name, Cost_Element_Descr, Document_Number, ParActivity, Fiscal_Year, Cost_Element, Functional_Area, Posting_Date
                    FROM wbs_with_io
                    UNION ALL
                    SELECT Full_Name, Personnel_Number, Total_Quantity, Val_COArea_Crcy, Period, WBS_Element, Partner_Object AS Object, Partner_Object, CO_Object_Name, Cost_Element_Name AS Cost_Element_Descr, Document_Number, Activity_Type AS ParActivity, Fiscal_Year, Cost_Element, Functional_Area, Posting_Date
                    FROM cc_deletes;
                """)
                cursor.execute("""
                    CREATE TABLE final_clean_data AS
                    SELECT *
                    FROM wbs_cc
                    WHERE Cost_Element_descr NOT IN ('Settle IO-MS(NP)', 'Settle IO-REM.COS (NP)', 'Settle IO-RD(NP)', 'Settle IO-REM COS')
                    AND Object NOT LIKE '9%'
                    AND Partner_Object NOT LIKE 'PA15%'
                    AND Cost_Element_descr <> 'Internal Labor';
                """)
                conn.commit()

            # Reset the DynamoDB table for the next run
            for file_name in required_files:
                table.update_item(
                    Key={'FileName': file_name},
                    UpdateExpression='SET #P = :val',
                    ExpressionAttributeNames={'#P': 'Processed'},
                    ExpressionAttributeValues={':val': False}
                )

    finally:
        # Ensuring the database connection is closed
        conn.close()

    return {
        'statusCode': 200,
        'body': 'File processing and data cleaning completed'
    }


# In[ ]:




