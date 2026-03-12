import requests
import pandas as pd
import json

def fetch_inmobi_data(creds, date_range):
    
    url = "https://api.inmobi.com/v1.0/generatesession/generate"

    # Headers
    headers = {
        "userName": creds["userName"],  # Replace with your actual username
        "secretKey": creds["secretKey"]  # Replace with your actual secret key
    }

    # Make the GET request
    response = requests.get(url, headers=headers)


    session_Id = response.json()['respList'][0]['sessionId']
    account_Id = response.json()['respList'][0]['accountId']


    # Define API endpoint
    url = "https://api.inmobi.com/v3.0/reporting/publisher"

    # Define headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "accountId": account_Id,  # Replace with your actual Account ID
        "secretKey": creds["secretKey"],  # Replace with your actual Secret Key
        "sessionId": session_Id
    }

    # Define request payload
    payload = {
        "reportRequest": {
            "metrics": ["adImpressions","servedImpressions", "clicks", "earnings", "adRequests"],
            "timeFrame": date_range,
            # "timeFrame": "2025-04-07:2025-04-15",
            "groupBy": [ "date", 'integrationDirect','adUnitType','placement']
            # "filterBy": [
            #     {"filterName": "integrationDirect", "filterValue": "s_prebid", "comparator": "="}
            # ]
        }
    }

    # Make the POST request
    response = requests.post(url, headers=headers, data=json.dumps(payload))


    inmobi_data = pd.DataFrame(response.json()['respList'])
    inmobi_data.shape

    return inmobi_data