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

# def correct_tz_offset(dt):
#     tz_offset = TZ_LOCAL.utcoffset(dt)
#     return dt + tz_offset

def parse_local_date_key(dt_s: str, start=True):
    """ parse a user entered string from local time zone as a datetime object """
    dt = datetime.strptime(dt_s, "%Y.%m.%d").replace(tzinfo=timezone.utc)
    if start == False:
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    dt = dt.astimezone(TZ_LOCAL)
    return dt

# def local_to_utc(dt: datetime):
#     dt_utc = datetime.utcfromtimestamp(dt.timestamp())
#     return dt_utc