import boto3
from datetime import datetime

from query_ddb import query_for_cats
from events import get_cat_events
from utils import lambda_result_body, TZ_LOCAL
from visits import collapse_events_to_visits

def get_cat_visits(start_date, end_date):
    dynamodb = boto3.resource('dynamodb')

    cats = query_for_cats(dynamodb)
    get_cat_events(cats, start_date, end_date, dynamodb)

    for cat in cats:
        cat['visits'] = collapse_events_to_visits(cat['events'])

    return cats

def lambda_handler(event, context):

    start_date = event['queryStringParameters']['start_date']
    end_date = event['queryStringParameters']['end_date']

    try:
        return lambda_result_body({
            "cats": get_cat_visits(start_date, end_date),
        })
    except ValueError as err:
        return lambda_result_body({
            "message": str(err),
        }, 400)


if __name__ == '__main__':
    try:
        cats = get_cat_visits('2021.11.19', '2021.11.19')
        
        for cat in cats:
            print(f"{cat['name']} -> {len(cat['visits'])}")

            sum_visits = 0
            for v in cat['visits']:
                 print(f"{v['start_timestamp']}: {v['total_collapsed']} -> {v['elapsed_sec']} sec ... {datetime.utcfromtimestamp(int(v['start_timestamp'] / 1000))}")
                 sum_visits += v['total_collapsed']

            print(sum_visits)

    except ValueError as err:
        print(err)
