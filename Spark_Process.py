import pandas as pd
import csv
from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from scipy.spatial import distance

original_matrix_file = pd.read_csv('../output_files/matrix.csv')
columns = original_matrix_file.columns.tolist()
required_values = original_matrix_file[columns[1:]].values
with open('output.csv','w') as f:
	writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
	for item in required_values:
		writer.writerow(item)

#Starting spark
sc = SparkContext("local", "Spark_Process")
data = sc.textFile("output.csv")
parsedData = data.map(lambda line: array([float(x) for x in line.split(',')]))
# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 20, maxIterations=1000, initializationMode="random")

#calculating distance of each datapoint from the centroid
index = 0
list_of_distances = []
for item in required_values:
	local_centroid = clusters.predict(item)
	distance_value = distance.euclidean(item, clusters.clusterCenters[local_centroid])
	list_of_distances.append((index, distance_value))
	index += 1
# sorted_distances = sorted(list_of_distances, key = lambda tup: tup[1], reverse = True)
# print(sorted_distances[0:5])
temp_df = pd.DataFrame(list_of_distances, columns = ['Index', 'Distance'])
temp_df.hist(column = 'Distance', bins = 100)



