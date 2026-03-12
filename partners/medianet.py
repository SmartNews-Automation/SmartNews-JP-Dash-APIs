import requests
import pandas as pd
import json



def token(user_mail, password):
    url = "https://neo.media.net/pub/api/v1/login/"

    payload = {
        "user_email": user_mail,
        "password": password,
        "merged_customer": True
    }

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

    data = response.json()
    auth_token = data["data"]["token"]

    return auth_token


def fetch_medianet_data(token, account_id, start_date, end_date):
    

    BASE = "https://neo.media.net/pub/api/v1"
    headers = {"token": token}


    customer_id = account_id 

    body = {
        "start_date": start_date,
        "end_date": end_date,
        "format": "json",
        "group_by": ["customer_id", "stats_date", "device_type","creative_id"],
        "pagination": {"enabled": 0},
        "filters": [
            {
                "col": "customer_id",
                "values": [customer_id],
                "include": True
            }
        ]
    }


    response = requests.post(f"{BASE}/reports", headers=headers, json=body)
    data = response.json()

    rows = data["data"].get("rows", [])


    df = pd.DataFrame(rows)

    return df



