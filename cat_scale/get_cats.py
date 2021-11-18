import boto3
from datetime import datetime, timedelta
from decimal import Decimal
from pytz import timezone

from manager.events import collapse_events_to_visits
from manager.query_ddb import query_events, query_for_cats
from utils import current_milli_time, lambda_result_body, timestamp_to_keys

MILLIS_24_HOURS = 24 * 60 * 60 * 1000  # 24 hours
NUM_PREVIOUS_WEIGHT_EVENTS = 3
TZ_LOCAL = timezone('America/New_York')

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
    events = query_events({ 'pk': today_keys[0] }, dynamodb)
    events += query_events({ 'pk': yesterday_keys[0] }, dynamodb)
    events += query_events({ 'pk': day_before_keys[0] }, dynamodb)

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

        # create visits list for today / yesterday
        cat['today_visits'] = collapse_events_to_visits(cat['today_events'])
        cat['yesterday_visits'] = collapse_events_to_visits(cat['yesterday_events'])

        cats.append(cat)

    return cats


def lambda_handler(event, context):
    return lambda_result_body({
        "cats": get_cats(),
    })


if __name__ == '__main__':
    cats = get_cats()
    for cat in cats:
        print(cat['name'])
        print(len(cat['yesterday_events']))
        print(len(cat['yesterday_visits']))
        print(len(cat['today_events']))
        print(len(cat['today_visits']))
