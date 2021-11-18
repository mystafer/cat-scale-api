import boto3
from datetime import datetime, timedelta
from decimal import Decimal

from manager.events import collapse_events_to_visits
from manager.query_ddb import query_events, query_for_cats
from utils import lambda_result_body

DEFAULT_VISIT_COLLAPSE_MS = 1 * 60 * 60 * 1000 # 1 hour
MAX_DURATION = timedelta(days=31)

def get_cat_visits(start_date_s, end_date_s, collapse_ms=DEFAULT_VISIT_COLLAPSE_MS, dynamodb=None):
    """ fetch visits from DynamoDB for a date range """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    start_date = datetime.strptime(start_date_s, "%Y.%m.%d")
    end_date = datetime.strptime(end_date_s, "%Y.%m.%d")

    if (end_date < start_date):
        raise ValueError("End Date should be after Start Date")
    if (end_date - start_date > MAX_DURATION):
        raise ValueError("Date Range should be less than one month")

    # get all events in range
    events = []
    date = start_date
    while date <= end_date:
        events += query_events({ 'pk': datetime.strftime(date, "%Y.%m.%d") }, dynamodb)
        date += timedelta(days=1)

    # loop over all cats and split events
    cats = []
    for c in query_for_cats(dynamodb):

        # initialize cat object
        cat = {
            'id': c['setting_id'],
            'name': c['name']
        }

        # locate events for cat sorted in revers chron order
        cat_events = sorted(
            [e for e in events if e['cat'] == cat['name']],
            key=lambda x: x['event_data']['timestamp'])

        cat['visits'] = collapse_events_to_visits(cat_events, collapse_ms)

        cats.append(cat)

    return cats


def lambda_handler(event, context):

    start_date = event['queryStringParameters']['start_date']
    end_date = event['queryStringParameters']['end_date']

    collapse_ms = int(event['queryStringParameters']['collapse_ms']) \
        if 'collapse_ms' in event['queryStringParameters'] else DEFAULT_VISIT_COLLAPSE_MS

    try:
        return lambda_result_body({
            "cats": get_cat_visits(start_date, end_date, collapse_ms),
        })
    except ValueError as err:
        return lambda_result_body({
            "message": str(err),
        }, 400)


if __name__ == '__main__':
    try:
        cats = get_cat_visits('2021.11.15', '2021.11.15')
        
        for cat in cats:
            print(f"{cat['name']} -> {len(cat['visits'])}")

            for v in cat['visits']:
                 print(f"{v['start_timestamp']}: {v['total_collapsed']} -> {v['elapsed_sec']} sec")

    except ValueError as err:
        print(err)
