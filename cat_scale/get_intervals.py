import boto3
from datetime import datetime

from query_ddb import query_for_cats
from events import get_cat_events
from intervals import collapse_visits_to_intervals, DEFAULT_VISIT_INTERVAL_COLLAPSE_MS
from visits import collapse_events_to_visits
from utils import lambda_result_body, parse_local_date_key, TZ_LOCAL

def get_cat_intervals(start_date_s, end_date_s, collapse_ms=DEFAULT_VISIT_INTERVAL_COLLAPSE_MS):
    dynamodb = boto3.resource('dynamodb')

    cats = query_for_cats(dynamodb)
    get_cat_events(cats, start_date_s, end_date_s, dynamodb)

    for cat in cats:
        cat['visits'] = collapse_events_to_visits(cat['events'])
        cat['intervals'] = collapse_visits_to_intervals(cat['visits'], collapse_ms)

        print(f"visits count: {len(cat['visits'])}")
        print(f"intervals count: {len(cat['intervals'])}")

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
        ONE_HOUR = 1 * 60 * 60 * 1000 # 1 hour
        cats = get_cat_intervals('2021.11.19', '2021.11.19', TWENTY_FOUR_HOURS)
        
        for cat in cats:
            print(f"{cat['name']} -> {len(cat['visits'])} -> {len(cat['intervals'])}")

            sum_visits = 0
            for v in cat['intervals']:
                dt = datetime.utcfromtimestamp(int(v['tick'] / 1000))
                print(f"{v['tick']}: {v['total_collapsed']} -> {v['elapsed_sec']} sec ... {dt}")
                sum_visits += v['total_collapsed']

            print(sum_visits)

    except ValueError as err:
        print(err)
