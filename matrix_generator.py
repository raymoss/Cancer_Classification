# copyright: yueshi@usc.edu
import pandas as pd 
import hashlib
import os 
from utils import logger
from pathlib import Path
from collections import OrderedDict, defaultdict
import copy

def file_as_bytes(file):
    with file:
        return file.read()

def extractMatrix(dirname):
	'''
	return a dataframe of the miRNA matrix, each row is the miRNA counts for a file_id

	'''
	count = 0

	miRNA_data = []
	for idname in os.listdir(dirname):
		# list all the ids 
		if idname.find("-") != -1:
			idpath = dirname +"/" + idname

			# all the files in each id directory
			for filename in os.listdir(idpath):
				# check the miRNA file
				if filename.find("-") != -1:

					filepath = idpath + "/" + filename
					df = pd.read_csv(filepath,sep="\t")
					# columns = ["miRNA_ID", "read_count"]
					if count == 0:
						# get the miRNA_IDs 
						miRNA_IDs = df.miRNA_ID.values.tolist()

					id_miRNA_read_counts = [idname] + df.read_count.values.tolist()
					miRNA_data.append(id_miRNA_read_counts)


					count +=1
					# print (df)
	columns = ["file_id"] + miRNA_IDs
	df = pd.DataFrame(miRNA_data, columns=columns)
	return df

def extractLabel(inputfile):
	df = pd.read_csv(inputfile, sep="\t")	
	#
	# print (df[columns])
	df['label'] = df['cases.0.samples.0.sample_type']
	df['ethnicity'] = df['cases.0.demographic.ethnicity']
	df['race'] = df['cases.0.demographic.race']
	df['gender'] = df['cases.0.demographic.gender']
	df.loc[df['cases.0.samples.0.sample_type'].str.contains("Normal"), 'label'] = '0'
	# print(df.loc['cases.0.project.primary_site'])
	df.loc[df['cases.0.samples.0.sample_type'].str.contains("Tumor"), 'label'] = df['cases.0.project.disease_type']
	df['label'], _ = pd.factorize(df['label'])
	df['ethnicity'], _ = pd.factorize(df['ethnicity'])
	df['race'], _ = pd.factorize(df['race'])
	df['gender'], _ = pd.factorize(df['gender'])
	# tumor_count = df.loc[df.label == 1].shape[0]
	# normal_count = df.loc[df.label == '0'].shape[0]
	# logger.info("{} Normal samples, {} Tumor samples ".format(normal_count,tumor_count))
	columns = ['file_id','ethnicity','race','gender','label']
	return df[columns]

def preprocessing_threshold_removal(df):
	drop_columns_list = []
	drop_columns = 0
	dp = df.replace(-1, pd.np.nan).dropna(axis=0, how='any', subset=df.columns.tolist()[1:])
	for i in df.columns.tolist():
		# print("MOss" + i)
		if 'file_id' == i or 'label' == i:
			continue
		count = 0
		total_count = len(df[i])
		# print(i)
		# if df[i].min() >= 0 and df[i].max() < 10:
			
		a = df[i].value_counts().to_dict(OrderedDict)
		# if '0' in a.keys():
		flag = 0
		flag1 = 0
		# for key, value in a.items():
		# 	count += value
		# 	if flag == 1:
		# 		a[key] = pd.np.nan
		# 		continue
		# 	if count >= 0.75*total_count:
		# 		flag = 1
		for key, value in a.items():
			if(value > 0.7*total_count):
				df = df.drop(i, axis = 1)
				flag1 = 1
				break
			count += value
			if count >= 0.85*total_count:
				a[key] = pd.np.nan
		none_count = 0
		if(flag1 == 0):
			for j in df[i]:
				if a[j] is pd.np.nan:
					j = pd.np.nan
					none_count += 1
			if(none_count > 0.3*total_count):
				df = df.drop(i, axis = 1)
				drop_columns += 1
				print(i)
	print(drop_columns)
	# temp_data = copy.deepcopy(df)
	# temp_data.pop('file_id')
	# temp_df = temp_data.columns.tolist()
	print(len(df.columns.tolist()))
	# temp_df.remove('file_id')
	# print(temp_df)
	for i in df.columns.tolist()[1:]:
		mean_value = df[i].mean()
		std_value = df[i].values.std(ddof=1)
		if std_value < 0.5*mean_value:
			df.drop(i, axis = 1)
			print(i)
	print(len(df.columns.tolist()))
	df = df.dropna(axis=0, how='any', subset=df.columns.tolist()[1:])

	return df

class MatrixGenerator():
	def __init__(self, GDC_data, input_file, output_file):
		self.input_dir = GDC_data
		self.input_file = input_file
		self.output_file = output_file

	def generator(self):
		# extract data
		# error_code = 1
		# matrix_df = extractMatrix(self.input_dir)
		# label_df = extractLabel(self.input_file)

		# #merge the two based on the file_id
		# result = pd.merge(matrix_df, label_df, on='file_id', how="left")
		#print(result)
		df = pd.read_csv("matrix.csv")
		df = preprocessing_threshold_removal(df)
		# df = result
		# #save data
		df.to_csv(self.output_file, index=False)
		if(Path(self.output_file).is_file() and os.path.getsize(self.output_file)):
			error_code = 0

		return error_code

	#print (labeldf)

 




