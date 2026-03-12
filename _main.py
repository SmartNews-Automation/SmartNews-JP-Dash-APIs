import pandas as pd
import os
import json
import pandas_gbq
from datetime import datetime

import time
from partners.nimbus import fetch_nimbus_data
from partners.Smaato import fetch_Smaato_data
from partners.inmobi import fetch_inmobi_data
from partners.medianet import fetch_medianet_data,token
from partners.loopme import fetch_loopme_data



#----------------------------------  loading the creds.json file  -------------------------------------------#
with open('creds.json', 'r') as f:
    creds = json.load(f)

#----------------------------------  BigQuery Configuration  -------------------------------------------#
from bq import push_data_to_bq
print("✅BigQuery Module Imported")

#-----------------------------------------   Date Generation   -----------------------------------------#
from date_range import generate_last_x_days_start_date_end_date

date_range = generate_last_x_days_start_date_end_date()
start_date = date_range.split(':')[0]
end_date = date_range.split(':')[1]
print(f"✅Date Generated: Start Date - {start_date}, End Date - {end_date}")

#-----------------------------------------   Execution   -----------------------------------------#
failures_list = []

# #-----------------------------------------    Nimbus+   -----------------------------------------#
def nimbus_():
    try:
        nimbus_creds = creds['nimbus_creds']

        nimbus_data = fetch_nimbus_data(nimbus_creds, start_date, end_date)

        push_data_to_bq(data=nimbus_data, table="jp_nimbusplus_lastxdays")

        print("Nimbus Data Fetched and Pushed to BigQuery\n\n")

    except Exception as e:
        failures_list.append("Nimbus+")
        print(f"Error in Nimbus Data Fetching or Pushing: {e}")

# #-----------------------------------------   Smaato prebid  -----------------------------------------#

def Smaato():
    try:
        Smaato_creds = creds['Smaato_creds']

        Smaato_data_Prebid = fetch_Smaato_data(Smaato_creds['Prebid'],start_date,end_date)
        push_data_to_bq(data=Smaato_data_Prebid, table="jp_smaato_prebid_lastxdays")

        print("✅12. Smaato  prebid Data Fetched and Pushed to BigQuery\n\n")

    except Exception as e:
        failures_list.append("Smaato")
        print(f"Error in Smaato Data Fetching or Pushing: {e}")
    
# #-----------------------------------------   InMobi   -----------------------------------------#

def inmobi_():
    try:
        inmobi_creds = creds['inmobi_creds']

        inmobi_data = fetch_inmobi_data(inmobi_creds, date_range)

        push_data_to_bq(data=inmobi_data, table="jp_inmobi_last_Xdays")

        print("✅5. InMobi Data Fetched and Pushed to BigQuery\n\n")    

    except Exception as e:
        failures_list.append("InMobi")
        print(f"Error in InMobi Data Fetching or Pushing: {e}")

#-----------------------------------------   Media.Net - Prebid, TAM(Video), Nimbus  -----------------------------------------#

def medianet_():
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
        end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
        medianet_creds = creds['media_net_creds']
        user_name = medianet_creds['user_mail']
        password = medianet_creds['password']

        auth = token(user_name, password)

        medianet_data_prebid = fetch_medianet_data(auth, medianet_creds['prebid'], start, end)
        push_data_to_bq(data=medianet_data_prebid, table="jp_media_net_prebid")

        medianet_data_nimbus = fetch_medianet_data(auth, medianet_creds['nimbus'], start, end)
        push_data_to_bq(data=medianet_data_nimbus, table="jp_media_net_nimbus")

        medianet_data_obdirect = fetch_medianet_data(auth, medianet_creds['obdirect'], start, end)
        push_data_to_bq(data=medianet_data_obdirect, table="jp_media_net_obdirect")

        print("✅14. MediaNet  Data Fetched and Pushed to BigQuery\n\n")

    except Exception as e:
        failures_list.append("Media.Net")
        print(f"Error in MediaNet Data Fetching or Pushing: {e}")

#-----------------------------------------    LoopMe - Prebid   -----------------------------------------#

def loopme_():
    try:
        loopme_creds = creds['loopme_creds']

        loopme_data_nimbus = fetch_loopme_data(loopme_creds['nimbus'])

        push_data_to_bq(data=loopme_data_nimbus, table="jp_loopme_nimbus_lastxdays")

        print("✅13. LoopMe  Data Fetched and Pushed to BigQuery\n\n")

    except Exception as e:
        failures_list.append("LoopMe")
        print(f"Error in LoopMe Data Fetching or Pushing: {e}")



#-----------------------------------------   Summary   -----------------------------------------#

def summary(failures_list):
    if failures_list:
        i=1
        print(f"\n❌ {len(failures_list)} Failures: ")
        for partners in failures_list:
            print(f"{i}. {partners}")
            i+=1
    else:
        print("\n✅ All Executions Successful.")


def main():
    print("\n----- Execution Started -----\n")

    # List of partner functions to execute
    partners = [
        ("InMobi", inmobi_),
        ("Nimbus", nimbus_),
        ("Media.Net", medianet_),
        ("Smaato", Smaato),
        ("LoopMe", loopme_)
    ]


    for name, func in partners:
        print(f"Running: {name} ...")
        try:
            func()
            print(f" {name} completed successfully.\n")
        except Exception as e:
            print(f" {name} failed: {e}\n")
        time.sleep(0.5) 
    print("----- Execution Finished -----\n")


if __name__ == "__main__":
    main()

    if failures_list:
        i=1
        print(f"\n❌ {len(failures_list)} Failures: ")
        for partners in failures_list:
            print(f"{i}. {partners}")
            i+=1
    else:
        print("\n✅ All Executions Successful.")