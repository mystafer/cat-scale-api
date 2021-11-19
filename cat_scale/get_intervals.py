import boto3
from datetime import datetime

from query_ddb import query_for_cats
from events import get_cat_events
from intervals import collapse_visits_to_intervals, DEFAULT_VISIT_INTERVAL_COLLAPSE_MS
from visits import collapse_events_to_visits
from utils import lambda_result_body, TZ_LOCAL

def get_cat_intervals(start_date, end_date, collapse_ms):
    dynamodb = boto3.resource('dynamodb')

    cats = query_for_cats(dynamodb)
    get_cat_events(cats, start_date, end_date, dynamodb)

    for cat in cats:
        cat['visits'] = collapse_events_to_visits(cat['events'])
        cat['intervals'] = collapse_visits_to_intervals(cat['visits'], collapse_ms)

    return cats


def lambda_handler(event, context):

    start_date = event['queryStringParameters']['start_date']
    end_date = event['queryStringParameters']['end_date']

    collapse_ms = int(event['queryStringParameters']['collapse_ms']) \
        if 'collapse_ms' in event['queryStringParameters'] else DEFAULT_VISIT_INTERVAL_COLLAPSE_MS

    try:
        return lambda_result_body({
            "cats": get_cat_intervals(start_date, end_date, collapse_ms),
        })
    except ValueError as err:
        return lambda_result_body({
            "message": str(err),
        }, 400)


if __name__ == '__main__':
    try:
        TWENTY_FOUR_HOURS = 24 * 60 * 60 * 1000 # 24 hours
        cats = get_cat_intervals('2021.11.18', '2021.11.19', TWENTY_FOUR_HOURS)
        
        for cat in cats:
            print(f"{cat['name']} -> {len(cat['visits'])}")

            sum_visits = 0
            for v in cat['intervals']:
                 print(f"{v['tick']}: {v['total_collapsed']} -> {v['elapsed_sec']} sec ... {datetime.utcfromtimestamp(int(v['tick'] / 1000)).astimezone(TZ_LOCAL)}")
                 sum_visits += v['total_collapsed']

            print(sum_visits)

    except ValueError as err:
        print(err)