from datetime import datetime
import simplejson as json
import time

def current_milli_time():
    return round(time.time() * 1000)
    
def timestamp_to_keys(timestamp):
    """ convert a timestamp to partition and sort keys """
    dt = datetime.utcfromtimestamp(int(timestamp/1000))

    partition_key = dt.strftime("%Y.%m.%d")
    sort_key = dt.strftime("%H:%M:%S:") + str(timestamp%1000).zfill(3)

    return (partition_key, sort_key)

def lambda_result_body(body: dict, status=200):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin" : "*",
        },
        "body": json.dumps(body),
    }
