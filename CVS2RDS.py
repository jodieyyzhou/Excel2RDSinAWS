# A lambda function to interact with AWS RDS MySQL

import boto3
import logging
import os
import sys
import uuid
#import mysql.connector
import pymysql
import csv


REGION = 'your_region'
rds_host  = "database.xxxx.us-east.rds.amazonaws.com"
name = "username"
password = "dbpassword"
db_name = "dbname"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
	conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except Exception as e:
	logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
	logger.error(e)
	sys.exit()

logger.info("SUCCESS: Connection to RDS mysql instance succeeded")



def handler(event, context):
	s3 = boto3.client('s3')
	s3_resource = boto3.resource('s3')
	if event:
		s3_records = event['Records'][0]
		bucket_name = str(s3_records['s3']['bucket']['name'])
		file_name = str(s3_records['s3']['object']['key'] )
		download_path = '/tmp/{}'.format(file_name)
		s3_resource.meta.client.download_file(bucket_name,file_name,download_path)
		csv_data = csv.reader(open(download_path),delimiter=',')
		next(csv_data)
		with conn.cursor() as cur:
			for row in csv_data:
				logger.info(row)
				try:
					cur.execute('UPDATE test SET `column1` = %s WHERE `column2` = %s',row)
					#cur.execute('INSERT INTO test  (`column1`,`column2`,`column3`,`column4`) VALUES (%s, %s, %s, %s)',row)
				except Exception as e:
						logger.error(e)
						
			conn.commit()
		return 'File loaded into RDS:' +str(download_path)
		
