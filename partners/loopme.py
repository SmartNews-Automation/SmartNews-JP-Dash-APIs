import requests
import pandas as pd

def fetch_loopme_data(api_auth_token):

    url = f'https://reports.loopme.com/api/v1/reports/apps?api_auth_token={api_auth_token}&date_range=days7&group_by=date,format'
    
    response = requests.get(url)
    print(response)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code: {response.status_code}")
    
    data = response.json()
    
    # Flatten nested JSON structure
    flat_data = []
    for item in data['series']:
        base = {
            'date': item['date'],
            'format': item['format']
        }
        base.update(item['totals'])
        flat_data.append(base)
    
    # Create DataFrame
    loopme_data = pd.DataFrame(flat_data)
    
    # Rename columns to cleaner names
    column_mapping = {
        "date": "date",
        "format": "format",
        "CTR, %": "CTR",
        "Clicks": "Clicks",
        "Earnings, $": "Earnings",
        "Display Rate, %": "Display_Rate",
        "Requests": "Requests",
        "Views": "Views",
        "eCPM, $": "eCPM",
        "Fill Rate, %": "Fill_Rate",
        "Video Starts": "Video_Starts",
        "Video Completes": "Video_Completes"
    }
    loopme_data.rename(columns=column_mapping, inplace=True)
    return loopme_data


