#---------------- Code to Push data to Big Query ----------------#
import os
import pandas as pd
import json
# from dotenv import load_dotenv
# load_dotenv()

from google.cloud import bigquery
from google.oauth2 import service_account

with open('creds.json', 'r') as f:
    creds = json.load(f)

# service_account_creds = json.loads(os.getenv('service_account_creds'))
service_account_creds = creds['service_account_creds']

project_id = "gocro.jp:causal-flame-796"  # Replace with your GCP project ID
dataset_id = "JP_PartnerUI_Data"  # Replace with your BigQuery dataset ID


def push_data_to_bq(data: pd.DataFrame, table: str, dataset=dataset_id, projectid=project_id, bq_cred = service_account_creds, mode="replace") -> str:
    try:
        print("\nPushing data to BigQuery....")
        credentials = service_account.Credentials.from_service_account_info(bq_cred)
        client = bigquery.Client(credentials=credentials, project=projectid)

        table_id = f"{projectid}.{dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE" if mode=="replace" else "WRITE_APPEND"
        )
        job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
        job.result()  # wait for completion
        print(f"✅ Data pushed successfully to {table_id}")

    except Exception as e:
        print(f"Error in 'push_data_to_bq()' : {e}")

    print(data)