# (1)Deploy layer of pandas library 
# (2)Add S3 bucket as trigger

import json
import pandas as pd
import boto3
import io
import uuid


def handler(event, context):
	s3 = boto3.client('s3')
	if event:
		s3_records = event['Records'][0]
		bucket_name = str(s3_records['s3']['bucket']['name'])
		file_name = str(s3_records['s3']['object']['key'])
		file_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
		file_content = file_obj['Body'].read()
		read_excel_data = io.BytesIO(file_content)
		df = pd.read_excel(read_excel_data,header = None)
		bf = pd.read_excel(read_excel_data,sheet_name = 1,header = None)
		df_columnC = df[[2]]
		df_columnD = df[[3]]
		df_columnE = df[[4]]
		array_C = df_columnC.loc[[33,52,79,142]]
		array_C = array_C.fillna(0)
		array_D = df_columnD.loc[[0,5,7,11,13,18,19,22,25,26,32,33,34,35,36,\
		37,54,55,57,58,60,66,71,73,76,77,78,79,80,83,85,92,93,100,101,104,107,\
		110,118,120,134,135,154,156,162,164]]
		array_D = array_D.fillna(0)
		array_E = df_columnE.loc[[5]]
		array_E = array_E.fillna(0)
		list_D = array_D.values.flatten().tolist()
		expected_mis = list_D[19]+list_D[26]
		expected_devfees = list_D[24]+list_D[25]
		list_D.pop(19)
		list_D.pop(23)
		list_D.pop(23)
		list_D.pop(23)
		list_D.append(expected_mis)
		list_D.append(expected_devfees)
		list_C = array_C.values.flatten().tolist()
		list_E = array_E.values.flatten().tolist()
		bf_OCF = bf.iloc[212]
		bf_dividend = bf.iloc[256]
		array_OCF = bf_OCF.loc[5:29]
		list_F = array_OCF.values.flatten().tolist()
		array_dividend = bf_dividend.loc[5:29]
		list_G = array_dividend.values.flatten().tolist()
		list_C.extend(list_D)
		list_C.extend(list_E)
		list_C.extend(list_F)
		list_C.extend(list_G)
		list_C.append(list_C.pop(4))
		lstStr = []
		lstVal = list_C
		zippedLst = zip(lstStr, lstVal)
		my_dic = dict(zippedLst)
		my_df = pd.DataFrame.from_dict(my_dic,orient='index')
		my_df = my_df.transpose()
		
		uploadpath = '/tmp/' + str(uuid.uuid4()) + '.csv'
		keyname = uploadpath.replace('/tmp/','')
		my_df.to_csv(uploadpath,index= False)
		
		s3_resource = boto3.resource('s3')
		s3_resource.Bucket('financial-model-csv').upload_file(uploadpath,keyname)
		
	#TODO implement
	return {
		'statusCode':200,
		'body':json.dumps('Hello from Lambda!')
	}
