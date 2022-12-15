import json
import os
import requests
import base64
from yaml import loader, load
import register_seed_as_delta as register_seed_as_delta
import check_job_status as check_job_status

with open('profiles.yml') as f:
    data = load(f, Loader=loader.SafeLoader)
    
    host = data['databricks']['outputs']['dev']['host']
    token = data['databricks']['outputs']['dev']['token']
    cluster_id = data['engineering']['outputs']['dev']['cluster_id']

base_url = f'https://{host}/api/2.0/dbfs/put'

file_name = "airports.csv"

file_location = (
    "../seeds/"
    + file_name
)
print(file_location)

f = open(file_location, "rb")
files = {"content": (file_location, f)}
response = requests.post(
    base_url,
    files=files,
    headers={'Authorization': 'Bearer %s' % token },
    data={
        "path": f"/data/{file_name}",
        "overwrite": "true",
    },
)
if response.status_code == 200:
    print(response.status_code)
else:
    raise Exception(response.text)

notebook_parameters = register_seed_as_delta.notebook_parameters(file_name, "Airports", "Enriched", "")

print(notebook_parameters)

dbrks_jobs_url = f"https://{host}/api/2.1/jobs/runs/submit"

notebook_path = "/process_seed"

submit_notebook = register_seed_as_delta.run_notebook(dbrks_jobs_url, token, cluster_id, notebook_path, notebook_parameters)

dbrks_check_job_status_url = f"https://{host}/api/2.1/jobs/runs/get"

life_cycle_state = "PENDING"
# result_state= ""
while life_cycle_state != "TERMINATED":
    # life_cycle_state, result_state = check_job_status.check_job_status(dbrks_check_job_status_url, token, submit_notebook)
    life_cycle_state = check_job_status.check_job_status(dbrks_check_job_status_url, token, submit_notebook)

