from prediction_model import PredictionModel
from request_meta import RequestMeta
from helper_functions import *
from matrix_generator import MatrixGenerator

INPUT_FILE_DIR = "/home/ray/EE542/lab10/input_files/"
OUTPUT_FILE_DIR = "/home/ray/EE542/lab10/output_files/"
MANIFEST_FILE = INPUT_FILE_DIR + "gdc_manifest.2018-10-22.txt"
GDC_DATA_DIR = "/home/ray/EE542/lab10/data/"
INPUT_JSON_FILE = INPUT_FILE_DIR + "files.2018-10-22.json"
OUTPUT_CSV_FILE = OUTPUT_FILE_DIR + "file_case.csv"
OUTPUT_FILE_META_TSV = OUTPUT_FILE_DIR + "files_meta.tsv"
OUTPUT_CASE_META_TSV = OUTPUT_FILE_DIR + "case_meta.tsv"
OUTPUT_CSV_MATRIX_FILE = OUTPUT_FILE_DIR + "matrix.csv"


def main():
	# logger.info("Initiating the integrity check")
	# error_code = check_helper(MANIFEST_FILE, GDC_DATA_DIR)
	# if error_code != 0:
	# 	logger.error("Issue with verifying integrity")
	# 	return -1
	# logger.info("Initializing json file generator")
	# error_code = json_file_generator(INPUT_JSON_FILE, OUTPUT_CSV_FILE)
	# if error_code != 0:
	# 	logger.error("Issue with generating csv file in stage 2")
	# 	return -1
	# logger.info("Initializing meta data generator")
	# meta_object = RequestMeta(OUTPUT_CSV_FILE, OUTPUT_FILE_META_TSV, OUTPUT_CASE_META_TSV)
	# error_code = meta_object.meta_file_generate()
	# if error_code != 0:
	# 	logger.error("Issue with generating meta files")
	# 	return -1
	# logger.info("Successfully created meta data file")
	logger.info("Initializing matrix generator")
	matrix_generator_object = MatrixGenerator(GDC_DATA_DIR, OUTPUT_FILE_META_TSV, OUTPUT_CSV_MATRIX_FILE)
	error_code = matrix_generator_object.generator()
	if error_code != 0:
		logger.info("Issue with generating matrix csv")
		return -1
	logger.info("Successfully generated matrix csv")
	logger.info("Running prediction model")
	model_object = PredictionModel(OUTPUT_CSV_MATRIX_FILE)
	model_object.run_model()

if __name__ == "__main__":
	main()

