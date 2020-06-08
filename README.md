# Excel2RDSinAWS

The purpose of this function is mainly to streamline the process for Business Development Department to upload the Financial Model excel file into the SQL database.

# Bubble.io 
- Uploading excel file via File Uploader API into

# AWS S3 Bucket 
- Receive the excel file (file size 1.4MB) and trigger

# AWS Lambda 
## (1)Extract information and transform the dataFrame using pandas into csv file (file size 2.6KB) 
-reduce file size by 100% 
## (2)Connecting to AWS RDS and Update the Opportunities using update query
-full function completed within 0.9s
