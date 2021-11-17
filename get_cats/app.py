import boto3
from boto3.dynamodb.conditions import Key
import simplejson as json
from datetime import datetime, timedelta
from decimal import Decimal
import time
from pytz import timezone

MILLIS_24_HOURS = 24 * 60 * 60 * 1000  # 24 hours
NUM_PREVIOUS_WEIGHT_EVENTS = 3
TZ_LOCAL = timezone('America/New_York')

VISIT_COLLAPSE_MS = 2 * 60 * 1000 # 2 min

def current_milli_time():
    return round(time.time() * 1000)
    
def timestamp_to_keys(timestamp):
    """ convert a timestamp to partition and sort keys """
    dt = datetime.utcfromtimestamp(int(timestamp/1000))

    partition_key = dt.strftime("%Y.%m.%d")
    sort_key = dt.strftime("%H:%M:%S:") + str(timestamp%1000).zfill(3)

    return (partition_key, sort_key)

def query_events(keys, dynamodb=None):
    """ query DynamoDB log table for entries matching keys """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cat_scale_event')
    if 'sk' in keys:
        response = table.query(
            KeyConditionExpression=Key('sample_date').eq(keys['pk'])
                & Key('sample_time').between(*keys['sk'])
        )
    else:
        response = table.query(
            KeyConditionExpression=Key('sample_date').eq(keys['pk'])
        )

    return response['Items']

def query_for_cats(dynamodb=None):
    """ query DynamoDB log table for cats """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('cat_scale_settings')
    response = table.query(
        KeyConditionExpression=Key('setting_type').eq('cat-definition')
    )
    return response['Items']


def get_cats(dynamodb=None):
    """ fetch cat data from DynamoDB and add events from yesterday / today """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    # determine query keys for today and yesterday
    now = current_milli_time()
    today_keys = timestamp_to_keys(now)
    yesterday_keys = timestamp_to_keys(now - MILLIS_24_HOURS)
    day_before_keys = timestamp_to_keys(now - 2 * MILLIS_24_HOURS)

    # get all events for yesterday and today
    events = query_events({ 'pk': today_keys[0] }) + query_events({ 'pk': yesterday_keys[0] })  + query_events({ 'pk': day_before_keys[0] })

    # hack to ensure date_est renamed to date_local
    old_events = [e for e in events if 'date_local' not in e['event_data']]
    for event in old_events:
        event['event_data']['date_local'] = event['event_data'].pop('date_est')

    # loop over all cats
    cats = []
    for c in query_for_cats(dynamodb):

        # locate the last defined weight for cat
        defined_weight = sorted([w for w in c['defined_weights']], key=lambda x: x['timestamp'], reverse=True)[0]
        cat = {
            'id': c['setting_id'],
            'name': c['name'],
            'defined_weight': defined_weight['weight']
        }

        # locate events for cat sorted in revers chron order
        cat_events = sorted(
            [e for e in events if e['cat'] == cat['name']],
            key=lambda x: x['event_data']['timestamp'],
            reverse=True)

        now_local = datetime.now(TZ_LOCAL)
        today_local = now_local.strftime("%Y.%m.%d")        
        yesterday_local = (now_local - timedelta(days=1)).strftime("%Y.%m.%d")

        # store events split by day into cat object
        cat['today_events'] = [e for e in cat_events if e['event_data']['date_local'] == today_local]
        cat['yesterday_events'] = [e for e in cat_events if e['event_data']['date_local'] == yesterday_local]

        # compute yesterday and today's weight
        cat_weight_events = cat['yesterday_events'][:NUM_PREVIOUS_WEIGHT_EVENTS]
        if (len(cat_weight_events) > 0):
            cat['yesterday_weight'] = Decimal(sum(w['event_data']['weight'] for w in cat_weight_events) / len(cat_weight_events))
        else:
            cat['yesterday_weight'] = cat['defined_weight']

        cat_weight_events = cat['today_events'][:NUM_PREVIOUS_WEIGHT_EVENTS]
        if (len(cat_weight_events) > 0):
            cat['today_weight'] = Decimal(sum(w['event_data']['weight'] for w in cat_weight_events) / len(cat_weight_events))
        else:
            cat['today_weight'] = cat['yesterday_weight']

        # sort the cat events in chron order
        cat['today_events'].sort(key=lambda x: x['event_data']['timestamp'])    
        cat['yesterday_events'].sort(key=lambda x: x['event_data']['timestamp'])    

        # create visits list for today / yesterda
        cat['today_visits'] = collapse_events_to_visits(cat['today_events'])
        cat['yesterday_visits'] = collapse_events_to_visits(cat['yesterday_events'])

        cats.append(cat)

    return cats


def collapse_events_to_visits(events):
    """ events should be ordered already chronologically, this function will consolidate nearby events into visits """

    # if (len(events) == 0):
    #     return []

    # initialize visits list as empty and last timestamp to zero to force first event to create a visit
    visits = []
    visit = { 'end_timestamp': 0 }

    # loop over events and look for visits to collaps
    for event in events:

        event_ts = event['event_data']['timestamp']
        distance_ms = event_ts - visit['end_timestamp']

        # event is close, collapse it to current visit
        if distance_ms <= VISIT_COLLAPSE_MS:
            visit['events'].append(event)
            visit['end_timestamp'] = event_ts

        # event is far, start a new visit
        else:
            visit = {
                'start_timestamp': event_ts,
                'end_timestamp': event_ts,
                'events': [ event ]
            }
            visits.append(visit)

    # loop over visits and reduce size
    for visit in visits:
        last_event = visit['events'][-1]
        visit['elapsed_sec'] = (visit['end_timestamp'] - visit['start_timestamp']) / 1000 + last_event['event_data']['elapsed_sec']
        visit['events'] = [ e['event_data']['timestamp'] for e in visit['events'] ]

    return visits

def lambda_handler(event, context):

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin" : "*",
        },
        "body": json.dumps({
            "cats": get_cats(),
        }),
    }


if __name__ == '__main__':
    cats = get_cats()
    for cat in cats:
        print(cat['name'])
        print(len(cat['yesterday_events']))
        print(len(cat['yesterday_visits']))
        print(len(cat['today_events']))
        print(len(cat['today_visits']))
