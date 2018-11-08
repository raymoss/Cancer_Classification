import requests
import json
import pandas as pd
import os
from pathlib import Path
from utils import logger

def retrieveFileMeta(file_ids, outputfile):
    '''

    Get the tsv metadata for the list of case_ids
    Args:
        file_ids: numpy array of file_ids
        outputfile: the output filename

    '''

    fd = open(outputfile,'w')
    cases_endpt = 'https://api.gdc.cancer.gov/files'

    # The 'fields' parameter is passed as a comma-separated string of single names.
    fields = [
        "file_id",
        "file_name",
        "cases.submitter_id",
        "cases.case_id",
        "data_category",
        "data_type",
        "cases.project.primary_site",
        "cases.project.disease_type",
        "cases.samples.tumor_descriptor",
        "cases.samples.tissue_type",
        "cases.samples.sample_type",
        "cases.samples.submitter_id",
        "cases.samples.sample_id",
        "cases.samples.portions.analytes.aliquots.aliquot_id",
        "cases.samples.portions.analytes.aliquots.submitter_id",
        "cases.demographic.ethnicity",
        "cases.demographic.gender",
        "cases.demographic.race"
        ]

    filters = {
        "op":"in",
        "content":{
            "field":"files.file_id",
            "value": file_ids.tolist()
        }
    }
    #print(filters)
    fields = ','.join(fields)

    params = {
        "filters" : filters,
        "fields": fields,
        "format": "TSV",
        "pretty": "true",
        "size": 11486
    }
    # print (params)
    #print (filters)
    #print (fields)
    
    
    response = requests.post(cases_endpt, headers = {"Content-Type": "application/json"},json = params)
    fd.write(response.content.decode("utf-8"))
    fd.close()

    # print(response.content)
def retrieveCaseMeta(file_ids,outputfile):
    '''

    Get the tsv metadata for the list of case_ids
    Args:
        file_ids: numpy array of file_ids
        outputfile: the output filename

    '''

    fd = open(outputfile,'w')
    cases_endpt = 'https://api.gdc.cancer.gov/cases'


    filters = {
        "op":"in",
        "content":{
            "field":"cases.case_id",
            "value": file_ids.tolist()
        }
    }

    # print (filters)
    #expand group is diagnosis and demoragphic
    params = {
        "filters" : filters,
        "expand" : "diagnoses,demographic,exposures",
        "format": "TSV",
        "pretty": "true",
        "size": 11486
    }
    # print (params)
    #print (filters)
    #print (fields)
    
    
    response = requests.post(cases_endpt, headers = {"Content-Type": "application/json"},json = params)
    # print (response.content.decode("utf-8"))
    fd.write(response.content.decode("utf-8"))
    fd.close()

def genCasePayload(file_ids,payloadfile):
    '''
    Used for the curl method to generate the file payload.
    '''

    fd = open(payloadfile,"w")
    filters = {
        "filters":{
            "op":"in",
            "content":{
                "field":"cases.case_id",
                "value": file_ids.tolist()
            }
        },
        "format":"TSV",
        "expand" : "diagnoses,demographic,exposures",
        "size": "1000",
        "pretty": "true"
    }
    json_str = json.dumps(filters)
    fd.write(json_str)
    fd.close()
    # return json_str

def genFilePayload(file_ids,payloadfile):
    '''
    Used for the curl method to generate the payload.
    '''


    fd = open(payloadfile,"w")
    filters = {
        "filters":{
            "op":"in",
            "content":{
                "field":"files.file_id",
                "value": file_ids.tolist()
            }
        },
        "format":"TSV",
        "fields":"file_id,file_name,cases.submitter_id,cases.case_id,data_category,data_type,cases.samples.tumor_descriptor,cases.samples.tissue_type,cases.samples.sample_type,cases.samples.submitter_id,cases.samples.sample_id,cases.samples.portions.analytes.aliquots.aliquot_id,cases.samples.portions.analytes.aliquots.submitter_id",
        "pretty":"true",
        "size": "1000"
    }
    json_str = json.dumps(filters)
    fd.write(json_str)
    fd.close()




def curlFileMeta(file_ids,payloadfile,outputfile):
    genFilePayload(file_ids,payloadfile)
    os.system("curl --request POST --header \"Content-Type: application/json\" --data @"+payloadfile+" 'https://api.gdc.cancer.gov/files' > "+outputfile)

def curlCaseMeta(case_ids,payloadfile,outputfile):
    genCasePayload(case_ids,payloadfile)
    os.system("curl --request POST --header \"Content-Type: application/json\" --data @"+payloadfile+" 'https://api.gdc.cancer.gov/cases' > "+outputfile)



class RequestMeta():
    def __init__(self, input_file, output_files_meta, output_case_meta):
        self.output_files_meta = output_files_meta
        self.output_case_meta = output_case_meta
        self.input_file = input_file

    

    def meta_file_generate(self):
        
        error_code = 1
        df = pd.read_csv(self.input_file)
        file_ids = df.file_id.values
        case_ids = df.case_id.values
        # print(case_ids)
        
        # python request method
        retrieveFileMeta(file_ids, self.output_files_meta)
        retrieveCaseMeta(case_ids, self.output_case_meta)

        if Path(self.output_files_meta).is_file() and os.path.getsize(self.output_files_meta) > 0 and \
           Path(self.output_case_meta).is_file() and os.path.getsize(self.output_case_meta):
           error_code = 0
           logger.info("Generated output files from request meta")

        return error_code


