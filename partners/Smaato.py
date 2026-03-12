import requests
import pandas as pd

def fetch_Smaato_data(token, start_date, end_date):
    url = "https://api-reporting.verve.com/brand/supply"
    params = {
        "account_auth_token": token,
        "start_date": start_date,
        "end_date": end_date,
        "group_by": "date",
        "format": "json"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    result = response.json()

    df = pd.DataFrame(result["data"])

    return df
