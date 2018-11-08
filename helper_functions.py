import pandas as pd 
import hashlib
import os 
from utils import logger
from pathlib import Path
import json

MANIFEST = ""
MANIFEST_DOWNLOAD_DIR = ""

def file_as_bytes(file):
    with file:
        return file.read()

def check(dirname,total, df):
	'''
	check the md5 for each file downloaded. if md5 does not match, report error.
	'''
	error_code = 1
	count = 0
	for idname in os.listdir(dirname):
		# list all the ids 
		if idname.find("-") != -1:
			idpath = dirname +"/" + idname

			for filename in os.listdir(idpath):
				# check the miRNA file
				if filename.find("-") != -1:
					filepath = idpath + "/" + filename
					filehash = hashlib.md5(file_as_bytes(open(filepath, 'rb'))).hexdigest()
					if df.loc[df['filename'] == filename].md5.values[0] != filehash:
						logger.info("file id {} download fails, please downlaod again".format(idname))
					else:
						count +=1
	if count == total:
		logger.info("successful downloads")
		error_code = 0

	return error_code

def check_helper(manifest_file, dirname):
	logger.info(4*"="+"start checking"+4*"=")
	# the manifest file. modify file when use
	df = pd.read_csv(manifest_file, sep='\t')
	total = df.shape[0]
	error_code = check(dirname, total ,df)
	if(error_code == 0):
		logger.info(4*"="+"check finished"+4*"=")
	else:
		logger.error("Issue with md5 checksum")
	return error_code

def json_file_generator(input_json, output_csv):
	'''
	read the json file and parse the file id and case id info and save it 
	'''
	error_code = 1
	with open(input_json) as data_file:    
		data = json.load(data_file)

	data_arr = []
	case_ids = set()
	for each_record in data:
		# print (each_record)
		file_id = each_record['file_id']
		case_id =  each_record['cases'][0]['case_id']
		if case_id in case_ids:
			case_ids.add(case_id)

		else:
			
			data_arr.append([file_id,case_id])

	df = pd.DataFrame(data_arr, columns = ['file_id','case_id'])
	
	df.to_csv(output_csv,index=False)

	if(Path(output_csv).is_file()):
		error_code = 0
		logger.info("Successfully generated output files")
	return error_code

