# import json
# import os
import requests
# import base64
# from yaml import loader, load

# with open('profiles.yml') as f:
#     data = load(f, Loader=loader.SafeLoader)
    
#     host = data['databricks']['outputs']['dev']['host']
#     token = data['databricks']['outputs']['dev']['token']
#     cluster_id = data['engineering']['outputs']['dev']['cluster_id']

# dbrks_jobs_url = f"https://{host}/api/2.0/jobs/runs/submit"

def run_notebook(rest_url, token, cluster_id, notebook_path, notebook_params):
    # Create a new run
    jsonData = {
            'existing_cluster_id':cluster_id, 
            'notebook_task': {
                'notebook_path': notebook_path,
                'base_parameters': notebook_params
            }
        }
    print(jsonData)
    response = requests.post(rest_url, headers={'Authorization': 'Bearer %s' % token }, json=jsonData) # noqa E501
    print("response:",response)
    if response.status_code == 200:
        print(response.status_code)
        jsonResponse = response.json()
        print(jsonResponse['run_id'])

        return jsonResponse['run_id']
    else:
        raise Exception(response.text)


def notebook_parameters(file_name, table_name, schema_name, write_path):
    return {
        "fileName": file_name,
        "tableName": table_name,
        "schemaName": schema_name,
        "writePath": write_path,
    }
    # {
    #     "fileName": "airports.csv",
    #     "tableName": "airports",
    #     "schemaName": "Enriched",
    #     "writePath": "mnt/lake/enriched/seeds/airports",
    # }
