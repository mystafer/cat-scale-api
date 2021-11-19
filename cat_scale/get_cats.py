import boto3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from events import get_cat_events
from visits import collapse_events_to_visits
from query_ddb import query_for_cats
from utils import lambda_result_body, TZ_LOCAL

NUM_PREVIOUS_WEIGHT_EVENTS = 3

def get_cats(dynamodb=None):
    """ fetch cat data from DynamoDB and add events from yesterday / today """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    # determine today and yesterday, TZ relative
    today = datetime.now().astimezone(TZ_LOCAL).replace(hour=0, minute=0, second=0, microsecond=0)
    today_key = today.strftime("%Y.%m.%d")
    yesterday = today + timedelta(days=-1)
    yesterday_key = yesterday.strftime("%Y.%m.%d")

    # query dynamodb for cats
    cats = query_for_cats(dynamodb)

    # retrieve cat events
    get_cat_events(cats, yesterday_key, today_key, dynamodb)

    # calculate split ts
    offset = today.tzinfo.utcoffset(today)
    dt = today + offset
    split_ts = int(dt.astimezone(timezone.utc).timestamp() * 1000)

    # loop over cats and build results
    results = []
    for c in cats:

        # locate the last defined weight for cat
        defined_weight = sorted([w for w in c['defined_weights']], key=lambda x: x['timestamp'], reverse=True)[0]
        cat = {
            'id': c['setting_id'],
            'name': c['name'],
            'defined_weight': defined_weight['weight']
        }

        # collapse events to visits for cat
        visits = collapse_events_to_visits(c['events'], keep_events=True)

        # build list of original events and visits for cat
        cat["today_visits"] = []
        cat["today_events"] = []
        cat["yesterday_visits"] = []
        cat["yesterday_events"] = []

        for visit in visits:

            # determine which type of event this is for
            if visit['start_timestamp'] >= split_ts:
                type = 'today'
            else:
                type = 'yesterday'

            cat[f"{type}_visits"].append(visit)
            cat[f"{type}_events"] += visit['events']

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

        results.append(cat)

    return results


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
