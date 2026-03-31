import requests
import pandas as pd
def fetch_liftoff_data(APIKEY, start_date, end_date):

    API_KEY = APIKEY

    url = "https://report.api.vungle.com/ext/pub/reports/performance"

    params = {
    "start": start_date,
    "end": end_date,
    "dimensions": "date,placement",
    "aggregates": "views,revenue,ecpm"
    }

    headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Vungle-Version": "1",
    "Accept": "application/json"
}

    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    lo_jp_nimbus = pd.DataFrame(data)
    lo_jp_nimbus.columns = (lo_jp_nimbus.columns.str.strip().str.lower().str.replace(" ", "_"))
    return    lo_jp_nimbus