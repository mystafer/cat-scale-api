from datetime import datetime, timezone
import pytz as tz
import simplejson as json
import time

TZ_LOCAL = tz.timezone('America/New_York')

def current_milli_time():
    return round(time.time() * 1000)
    
def timestamp_to_keys(timestamp):
    """ convert a timestamp to partition and sort keys """
    dt = datetime.utcfromtimestamp(int(timestamp/1000))

    partition_key = dt.strftime("%Y.%m.%d")
    sort_key = dt.strftime("%H:%M:%S:") + str(timestamp%1000).zfill(3)

    return (partition_key, sort_key)

def date_to_query_key(dt: datetime):
    """ convert a datetime to partition query key """
    partition_key = dt.strftime("%Y.%m.%d")
    return { 'pk': partition_key }

def lambda_result_body(body: dict, status=200):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin" : "*",
        },
        "body": json.dumps(body),
    }

def parse_local_date_key(dt_s: str, start=True):
    """ parse a user entered string from local time zone as a datetime object """
    dt = datetime.strptime(dt_s, "%Y.%m.%d").replace(tzinfo=timezone.utc)
    if start == False:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    dt = dt.astimezone(TZ_LOCAL)
    return dt

def get_datetime_for_event_ts(event):
    timestamp = event['event_data']['timestamp']
    dt_utc = datetime.utcfromtimestamp(timestamp / 1000)
    dt_corrected = (dt_utc + TZ_LOCAL.utcoffset(dt_utc))
    return dt_corrected

# def get_utc_ts_for_event_ts(event):
#     timestamp = event['event_data']['timestamp']
#     dt_utc = datetime.utcfromtimestamp(timestamp / 1000)
#     return int(dt_utc.timestamp() * 1000)

def get_local_ts_for_event_ts(event):
    return int(get_datetime_for_event_ts(event).timestamp() * 1000)