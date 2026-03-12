#---------------- Date Generation - Last 7 Days ----------------#
from datetime import datetime, timedelta

def generate_last_x_days_start_date_end_date(end_day_diff=1, start_day_diff=6) -> datetime:
    ''' Function to return start date & end date of last 7 days date '''

    end_date  = datetime.now() - timedelta(days=end_day_diff)
    start_date = end_date - timedelta(days=start_day_diff)
    # start_date = start_date.strftime("%Y-%m-%d")
    # end_date = end_date.strftime("%Y-%m-%d")
    # return start_date, end_date

    return f"{start_date.strftime('%Y-%m-%d')}:{end_date.strftime('%Y-%m-%d')}"
