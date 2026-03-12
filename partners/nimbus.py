import requests
import pandas as pd

def fetch_nimbus_data(creds, start_date, end_date):

    Api_Key = creds['Api_Key']

    report_key = 'post_auction_traffic'
    response = requests.post(
        f"https://dashboard.adsbynimbus.com/v2/external/reports/{report_key}?account=smartnewsjapan",
        headers={
            "Authorization": f"Bearer {Api_Key}",
            "Content-Type": "application/json"
        },

        json={
            "start_date": start_date,
            "end_date": end_date,
            "interval": "day",
            "display": ["demand_source","auction_type","app_platform", "wins", "impressions", "revenue", "clicks"]
        }
    )

    data = response.json()

    nimbus_data = pd.DataFrame(data['data'])
    return nimbus_data
