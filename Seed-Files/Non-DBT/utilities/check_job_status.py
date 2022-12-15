import requests

def check_job_status(rest_url, token, run_id):
    # Check the status of a run
    run_identifier = int(run_id)
    response = requests.get(rest_url, headers={'Authorization': 'Bearer %s' % token }, json = {'run_id' : run_identifier})

    jsonResponse = response.json()

    life_cycle_state = jsonResponse['state']['life_cycle_state']
    # result_state = jsonResponse['state']['result_state']

    return life_cycle_state #, result_state


